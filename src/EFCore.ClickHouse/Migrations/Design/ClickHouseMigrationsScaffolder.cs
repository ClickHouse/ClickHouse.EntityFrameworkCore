using ClickHouse.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Design;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

// MigrationsScaffolder and several scaffolding services are internal EF Core APIs.
#pragma warning disable EF1001

namespace ClickHouse.EntityFrameworkCore.Migrations.Design;

/// <summary>
/// A migrations scaffolder that splits one <c>dotnet ef migrations add</c> into a sequence of
/// single-operation step migrations (<c>…_001</c>, <c>…_002</c>, …), ordered by
/// <see cref="ClickHouseMigrationsSplitter"/> so every object exists before it is referenced.
/// <para>
/// ClickHouse has no transactions, so a single multi-operation migration that fails partway is
/// recorded as not-applied yet has half its DDL committed — re-running then re-executes applied
/// statements and fails. Emitting one migration (and therefore one history row) per operation
/// makes a partial failure resumable: already-applied steps stay recorded. The generated
/// <c>Down</c> methods are forward-only and throw <see cref="ClickHouseDownMigrationNotSupportedException"/>.
/// </para>
/// </summary>
public class ClickHouseMigrationsScaffolder : MigrationsScaffolder
{
    private readonly ClickHouseMigrationsSplitter _splitter;
    private readonly IMigrationsCodeGeneratorSelector _codeGeneratorSelector;
    private readonly ICurrentDbContext _currentContext;
    private readonly IMigrationsIdGenerator _idGenerator;
    private readonly IMigrationsAssembly _migrationsAssembly;
    private readonly IMigrationsModelDiffer _modelDiffer;
    private readonly IModel _model;

    private readonly List<StepMigrationData> _pendingStepMigrations = [];
    private string? _sharedSnapshotCode;

    public ClickHouseMigrationsScaffolder(
        MigrationsScaffolderDependencies dependencies,
        ClickHouseMigrationsSplitter splitter)
        : base(dependencies)
    {
        _splitter = splitter;
        _codeGeneratorSelector = dependencies.MigrationsCodeGeneratorSelector;
        _currentContext = dependencies.CurrentContext;
        _idGenerator = dependencies.MigrationsIdGenerator;
        _migrationsAssembly = dependencies.MigrationsAssembly;
        _modelDiffer = dependencies.MigrationsModelDiffer;
        // The current, finalized design model — this is what the base scaffolder diffs and the
        // code generators serialize. (Do not unwrap via GetRelationalModel().Model; the underlying
        // model no longer carries the relational-model annotation and GetRelationalModel() then NREs.)
        _model = dependencies.Model;
    }

    public override ScaffoldedMigration ScaffoldMigration(
        string migrationName,
        string? rootNamespace,
        string? subNamespace = null,
        string? language = null,
        bool dryRun = false)
    {
        _pendingStepMigrations.Clear();
        _sharedSnapshotCode = null;

        var operations = GetMigrationOperations();

        // A single operation (or none) needs no splitting — fall back to the standard scaffolder,
        // but still ensure the generated code imports the ClickHouse builder extensions in case
        // that single operation is a custom one (e.g. a lone materialized-view create).
        if (operations.Count <= 1)
        {
            var single = base.ScaffoldMigration(migrationName, rootNamespace, subNamespace, language, dryRun);
            return new ScaffoldedMigration(
                single.FileExtension,
                single.PreviousMigrationId,
                EnsureClickHouseUsings(single.MigrationCode),
                single.MigrationId,
                single.MetadataCode,
                single.MigrationSubNamespace,
                single.SnapshotCode,
                single.SnapshotName,
                single.SnapshotSubnamespace);
        }

        var codeGenerator = _codeGeneratorSelector.Select(language);

        var steps = _splitter.Split(operations, _model);

        // All steps share one timestamp and differ only by the 3-digit suffix, so their ids sort
        // in step order (e.g. 20250601_Add_001 < 20250601_Add_002).
        var baseId = _idGenerator.GenerateId(migrationName);
        var timestamp = baseId.Split('_')[0];

        var contextType = _currentContext.Context.GetType();
        var finalSubNamespace = subNamespace ?? "Migrations";
        var snapshotName = $"{contextType.Name}ModelSnapshot";

        // One shared snapshot of the final model is written for the whole set.
        _sharedSnapshotCode = codeGenerator.GenerateSnapshot(
            finalSubNamespace, contextType, snapshotName, _model);

        var previousMigrationId = _migrationsAssembly.Migrations.LastOrDefault().Key;
        ScaffoldedMigration? firstStep = null;

        for (var i = 0; i < steps.Count; i++)
        {
            var step = steps[i];
            var stepId = $"{timestamp}_{migrationName}_{step.StepSuffix}";
            var stepClassName = $"_{stepId}"; // leading underscore → valid C# identifier
            var stepComment = $"// Step {step.StepNumber} of {steps.Count}: {step.OperationDescription}";

            var upOperations = new List<MigrationOperation> { step.Operation };

            var migrationCode = GenerateStepMigrationCode(
                codeGenerator, stepId, stepClassName, upOperations, finalSubNamespace, contextType, stepComment);

            var metadataCode = codeGenerator.GenerateMetadata(
                finalSubNamespace, contextType, stepClassName, stepId, _model);

            if (i == 0)
            {
                firstStep = new ScaffoldedMigration(
                    codeGenerator.FileExtension,
                    previousMigrationId,
                    migrationCode,
                    stepId,
                    metadataCode,
                    finalSubNamespace,
                    _sharedSnapshotCode,
                    snapshotName,
                    finalSubNamespace);
            }
            else
            {
                _pendingStepMigrations.Add(new StepMigrationData(
                    stepId, stepClassName, migrationCode, metadataCode, stepComment));
            }

            previousMigrationId = stepId;
        }

        return firstStep!;
    }

    public override MigrationFiles Save(
        string projectDir,
        ScaffoldedMigration migration,
        string? outputDir,
        bool dryRun = false)
    {
        // Write the first step through the base implementation, then the remaining steps beside it.
        var files = base.Save(projectDir, migration, outputDir, dryRun);

        if (!dryRun && _pendingStepMigrations.Count > 0)
        {
            var migrationsDir = Path.GetDirectoryName(files.MigrationFile)
                ?? Path.Combine(projectDir, outputDir ?? "Migrations");

            foreach (var step in _pendingStepMigrations)
            {
                File.WriteAllText(
                    Path.Combine(migrationsDir, $"{step.MigrationId}.cs"),
                    step.MigrationCode);
                File.WriteAllText(
                    Path.Combine(migrationsDir, $"{step.MigrationId}.Designer.cs"),
                    step.MigrationMetadataCode);
            }
        }

        _pendingStepMigrations.Clear();
        _sharedSnapshotCode = null;
        return files;
    }

    private IReadOnlyList<MigrationOperation> GetMigrationOperations()
    {
        // The model snapshot for THIS context is the diff baseline. Use the assembly's own snapshot
        // rather than scanning types for a "*ModelSnapshot" (which would pick the wrong one in a
        // multi-DbContext project). Process upgrades an older snapshot to the current shape first.
        // SnapshotModelProcessor.Process finalizes the snapshot's cached mutable model in place,
        // and finalizing it a second time returns null. The single-operation fallback path calls
        // base.ScaffoldMigration, which re-processes the assembly's snapshot for its own diff —
        // so this pre-diff must run on a fresh snapshot instance and leave the cached one
        // untouched, or base silently diffs against a null source model and scaffolds the whole
        // schema as new.
        var snapshot = _migrationsAssembly.ModelSnapshot;
        var lastModel = snapshot is null
            ? null
            : Dependencies.SnapshotModelProcessor.Process(
                ((ModelSnapshot)Activator.CreateInstance(snapshot.GetType(), nonPublic: true)!).Model).GetRelationalModel();

        return _modelDiffer.GetDifferences(lastModel, _model.GetRelationalModel());
    }

    private static string GenerateStepMigrationCode(
        IMigrationsCodeGenerator codeGenerator,
        string migrationId,
        string migrationName,
        IReadOnlyList<MigrationOperation> upOperations,
        string @namespace,
        Type contextType,
        string stepComment)
    {
        // Generate with an empty Down, then replace it with a forward-only throwing Down.
        var standardCode = codeGenerator.GenerateMigration(@namespace, migrationName, upOperations, []);
        var downMethod = GenerateForwardOnlyDownMethod(migrationId);
        var modified = ReplaceDownMethod(standardCode, downMethod, stepComment);
        return EnsureClickHouseUsings(modified);
    }

    private static string GenerateForwardOnlyDownMethod(string migrationId)
        => $$"""
                protected override void Down(MigrationBuilder migrationBuilder)
                {
                    throw new ClickHouseDownMigrationNotSupportedException("{{migrationId}}");
                }
        """;

    private static string ReplaceDownMethod(string code, string newDownMethod, string stepComment)
    {
        const string downPattern = "protected override void Down(MigrationBuilder migrationBuilder)";
        var downIndex = code.IndexOf(downPattern, StringComparison.Ordinal);
        if (downIndex < 0)
            return code;

        var braceStart = code.IndexOf('{', downIndex);
        if (braceStart < 0)
            return code;

        var braceEnd = FindMatchingBrace(code, braceStart);
        if (braceEnd < 0)
            return code;

        var result = $"{code[..downIndex]}{newDownMethod.TrimStart()}{code[(braceEnd + 1)..]}";

        // Surface the step's position as a header comment at the top of the file.
        return stepComment + Environment.NewLine + result;
    }

    // Ensures the generated migration imports the namespaces it relies on: the builder extension
    // methods used to render custom operations, and (for split steps) the forward-only Down exception.
    private static string EnsureClickHouseUsings(string code)
    {
        code = EnsureUsing(code, "using ClickHouse.EntityFrameworkCore.Extensions;");
        code = EnsureUsing(code, "using ClickHouse.EntityFrameworkCore.Migrations;");
        return code;
    }

    private static string EnsureUsing(string code, string requiredUsing)
    {
        if (code.Contains(requiredUsing, StringComparison.Ordinal))
            return code;

        var usingIndex = code.IndexOf("using ", StringComparison.Ordinal);
        return usingIndex < 0
            ? code
            : code[..usingIndex] + requiredUsing + Environment.NewLine + code[usingIndex..];
    }

    private static int FindMatchingBrace(string code, int openBraceIndex)
    {
        var depth = 0;
        for (var i = openBraceIndex; i < code.Length; i++)
        {
            switch (code[i])
            {
                case '{':
                    depth++;
                    break;
                case '}':
                    if (--depth == 0)
                        return i;
                    break;
            }
        }

        return -1;
    }

    internal sealed record StepMigrationData(
        string MigrationId,
        string MigrationName,
        string MigrationCode,
        string MigrationMetadataCode,
        string StepComment);
}
