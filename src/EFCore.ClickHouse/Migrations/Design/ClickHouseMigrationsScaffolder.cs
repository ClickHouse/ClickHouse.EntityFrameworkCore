using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using ClickHouse.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Internal;
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

    // The relational model of the last snapshot — the "prior model" shown by the rename confirmation.
    private IRelationalModel? _sourceRelationalModel;

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

        // Match the base scaffolder's up-front guards (the multi-op path below bypasses base, so
        // these must be enforced here too). The single-op fallback re-checks harmlessly via base.
        if (string.Equals(migrationName, "migration", StringComparison.OrdinalIgnoreCase))
            throw new OperationException(DesignStrings.CircularBaseClassDependency);
        if (_migrationsAssembly.FindMigrationId(migrationName) is not null)
            throw new OperationException(DesignStrings.DuplicateMigrationName(migrationName));

        // Confirm any column/table renames interactively (in a terminal) before scaffolding, so a
        // rename EF inferred can be re-interpreted as a drop + add if it isn't really a rename.
        var operations = ConfirmRenames(GetMigrationOperations());

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
        var ns = ResolveNamespaces(rootNamespace, subNamespace, contextType);

        // One shared snapshot of the final model is written for the whole set.
        _sharedSnapshotCode = codeGenerator.GenerateSnapshot(
            ns.SnapshotNamespace, contextType, ns.SnapshotName, _model);

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
                codeGenerator, stepId, stepClassName, upOperations, ns.MigrationNamespace, contextType, stepComment);

            var metadataCode = codeGenerator.GenerateMetadata(
                ns.MigrationNamespace, contextType, stepClassName, stepId, _model);

            if (i == 0)
            {
                firstStep = new ScaffoldedMigration(
                    codeGenerator.FileExtension,
                    previousMigrationId,
                    migrationCode,
                    stepId,
                    metadataCode,
                    ns.MigrationSubNamespace,
                    _sharedSnapshotCode,
                    ns.SnapshotName,
                    ns.SnapshotSubNamespace);
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

    // ── Interactive rename confirmation ──────────────────────────────────────────────────────────
    // EF's differ infers a rename by pairing a dropped and an added column/table. That inference can
    // be wrong (you meant a drop + a brand-new column). Before scaffolding, show the before/after
    // model for each inferred rename and let the user confirm — in a real terminal. A "no" for a
    // column rename is re-expressed as an explicit DropColumn + AddColumn (data loss, with a warning).

    private IReadOnlyList<MigrationOperation> ConfirmRenames(IReadOnlyList<MigrationOperation> operations)
    {
        if (!operations.Any(o => o is RenameColumnOperation or RenameTableOperation))
            return operations;

        var result = new List<MigrationOperation>(operations.Count);
        foreach (var op in operations)
        {
            switch (op)
            {
                case RenameColumnOperation rc:
                    PrintColumnRename(rc);
                    if (ConfirmIsRename())
                        result.Add(rc);
                    else
                        result.AddRange(ConvertColumnRenameToDropAdd(rc));
                    break;

                case RenameTableOperation rt:
                    PrintTableRename(rt);
                    if (!ConfirmIsRename())
                        Dependencies.OperationReporter.WriteWarning(
                            $"Keeping the table rename {rt.Name} → {rt.NewName}. Converting a table rename to "
                            + "drop + create is not automatic — edit the migration by hand if that is what you meant.");
                    result.Add(rt);
                    break;

                default:
                    result.Add(op);
                    break;
            }
        }

        return result;
    }

    private void PrintColumnRename(RenameColumnOperation rc)
    {
        var before = ColumnNames(_sourceRelationalModel, rc.Table, rc.Schema);
        var after = ColumnNames(_model.GetRelationalModel(), rc.Table, rc.Schema);
        var priorMigration = _migrationsAssembly.Migrations.LastOrDefault().Key;

        var w = Console.Error;
        w.WriteLine();
        w.WriteLine($"  ┌─ Possible column rename in table '{QualifiedTable(rc.Table, rc.Schema)}'");
        if (priorMigration is not null)
            w.WriteLine($"  │  prior source: snapshot after migration '{priorMigration}'");
        w.WriteLine($"  │  before (prior model): {FormatColumns(before, rc.Name)}");
        w.WriteLine($"  │  after  (new model):   {FormatColumns(after, rc.NewName)}");
        w.WriteLine($"  │  EF inferred: RENAME COLUMN {Highlight(rc.Name)} → {Highlight(rc.NewName)}  (keeps existing data)");
        w.WriteLine($"  │  Answer n if '{rc.Name}' was really dropped and '{rc.NewName}' is a new column"
                    + $" — that drops '{rc.Name}' (data loss) and adds '{rc.NewName}'.");
        w.WriteLine("  └─");
    }

    private void PrintTableRename(RenameTableOperation rt)
    {
        var w = Console.Error;
        w.WriteLine();
        w.WriteLine($"  ┌─ Possible table rename");
        w.WriteLine($"  │  before (prior model): {Highlight(QualifiedTable(rt.Name, rt.Schema))}");
        w.WriteLine($"  │  after  (new model):   {Highlight(QualifiedTable(rt.NewName, rt.NewSchema))}");
        w.WriteLine($"  │  EF inferred: RENAME TABLE {rt.Name} → {rt.NewName}  (keeps existing data)");
        w.WriteLine("  └─");
    }

    // Reads a y/n answer from the console. A real terminal blocks for the user's answer (this is the
    // interactive prompt); piped input is honored (so "n" can be scripted). End-of-input — CI, IDE
    // tooling, or stdin redirected from /dev/null — returns null, in which case we keep EF's rename
    // inference rather than blocking. Empty (just Enter) defaults to yes.
    private bool ConfirmIsRename()
    {
        while (true)
        {
            Console.Error.Write("  Keep as a rename (preserves data)?  [Y/n] ");
            var line = Console.ReadLine();
            if (line is null)
            {
                Console.Error.WriteLine("(non-interactive input — keeping the rename)");
                return true;
            }

            var answer = line.Trim().ToLowerInvariant();
            if (answer is "" or "y" or "yes")
                return true;
            if (answer is "n" or "no")
                return false;
            Console.Error.WriteLine("  Please answer 'y' or 'n'.");
        }
    }

    // Re-expresses an inferred column rename as an explicit drop + add, rebuilding the AddColumn from
    // the target column so the new column keeps its type/nullability/default/comment.
    private IEnumerable<MigrationOperation> ConvertColumnRenameToDropAdd(RenameColumnOperation rc)
    {
        Dependencies.OperationReporter.WriteWarning(
            $"Treating {QualifiedTable(rc.Table, rc.Schema)}.{rc.NewName} as a new column: dropping '{rc.Name}' "
            + $"and adding '{rc.NewName}'. Existing data in '{rc.Name}' will be lost.");

        yield return new DropColumnOperation { Name = rc.Name, Table = rc.Table, Schema = rc.Schema };

        var column = _model.GetRelationalModel().Tables
            .FirstOrDefault(t => t.Name == rc.Table && t.Schema == rc.Schema)?.Columns
            .FirstOrDefault(c => c.Name == rc.NewName);

        var add = new AddColumnOperation
        {
            Name = rc.NewName,
            Table = rc.Table,
            Schema = rc.Schema,
            ClrType = column?.ProviderClrType ?? column?.PropertyMappings.First().Property.ClrType ?? typeof(string),
            ColumnType = column?.StoreType,
            IsNullable = column?.IsNullable ?? true,
            DefaultValueSql = column?.DefaultValueSql,
            ComputedColumnSql = column?.ComputedColumnSql,
            IsStored = column?.IsStored,
            Comment = column?.Comment,
            Collation = column?.Collation,
        };
        if (column is not null && column.TryGetDefaultValue(out var defaultValue))
            add.DefaultValue = defaultValue;

        yield return add;
    }

    private static IReadOnlyList<string> ColumnNames(IRelationalModel? model, string table, string? schema)
        => model?.Tables.FirstOrDefault(t => t.Name == table && t.Schema == schema)?.Columns
            .Select(c => c.Name).ToList() ?? [];

    private static string QualifiedTable(string table, string? schema)
        => string.IsNullOrEmpty(schema) ? table : $"{schema}.{table}";

    private static string FormatColumns(IReadOnlyList<string> columns, string highlight)
        => columns.Count == 0
            ? "(none)"
            : string.Join("  ", columns.Select(c => c == highlight ? Highlight(c) : c));

    // Bold the token in a real terminal; fall back to «guillemets» when output is redirected/captured.
    private static string Highlight(string text)
        => Console.IsOutputRedirected ? $"«{text}»" : $"[1;33m{text}[0m";

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

    // A scaffolded step migration id: "<14-digit timestamp>_<name>_<NNN>". The class it declares is
    // named "_<id>" (ScaffoldMigration), which distinguishes a real step from a user migration that
    // merely happens to be named like "Foo_001".
    private static readonly Regex StepIdRegex = new(@"^\d{14}_.+_\d{3,}$", RegexOptions.Compiled);

    /// <summary>
    /// Removes the last migration. When the last migration is a scaffolded <b>batch</b> of step
    /// migrations (from a single multi-operation <c>migrations add</c>), the whole batch is removed
    /// together and the snapshot is reverted to the migration preceding the batch — otherwise the
    /// base implementation would peel a single step and revert the snapshot to that step's Designer
    /// model, which (being the batch's final model) still contains the removed steps, silently losing
    /// their DDL. A non-step last migration falls through to the base behavior unchanged.
    /// </summary>
    public override MigrationFiles RemoveMigration(
        string projectDir, string? rootNamespace, bool force, string? language, bool dryRun)
    {
        var migrations = _migrationsAssembly.Migrations.ToList();
        if (migrations.Count == 0 || !IsStepMigration(migrations[^1].Key, migrations[^1].Value))
            return base.RemoveMigration(projectDir, rootNamespace, force, language, dryRun);

        // The batch is the maximal trailing run of step migrations sharing one "<timestamp>_<name>"
        // prefix (so two adds with the same name never merge — the timestamp differs).
        var prefix = StepPrefix(migrations[^1].Key);
        var batchStart = migrations.Count;
        while (batchStart > 0
            && IsStepMigration(migrations[batchStart - 1].Key, migrations[batchStart - 1].Value)
            && StepPrefix(migrations[batchStart - 1].Key) == prefix)
        {
            batchStart--;
        }
        var batch = migrations.GetRange(batchStart, migrations.Count - batchStart);

        var files = new MigrationFiles();
        var modelSnapshot = _migrationsAssembly.ModelSnapshot
            ?? throw new OperationException(DesignStrings.NoSnapshot);
        var codeGenerator = _codeGeneratorSelector.Select(language);
        var activeProvider = Dependencies.DatabaseProvider.Name;

        var lastStep = _migrationsAssembly.CreateMigration(migrations[^1].Value, activeProvider);

        // Sync guard (mirrors base): if the snapshot no longer matches the last compiled migration,
        // don't delete anything — just re-sync the snapshot to it. Each model instance is processed
        // exactly once (base is never re-entered here), so the in-place finalization is safe.
        var lastStepModel = Dependencies.SnapshotModelProcessor.Process(lastStep.TargetModel);
        var snapshotModel = Dependencies.SnapshotModelProcessor.Process(modelSnapshot.Model);
        if (_modelDiffer.HasDifferences(lastStepModel.GetRelationalModel(), snapshotModel.GetRelationalModel()))
        {
            Dependencies.OperationReporter.WriteVerbose(DesignStrings.ManuallyDeleted);
            return WriteSnapshot(files, modelSnapshot, codeGenerator, rootNamespace, projectDir, lastStepModel, dryRun);
        }

        // Applied steps cannot be removed: their DDL is committed and — because steps are
        // forward-only (Down throws) — cannot be reverted automatically.
        var applied = GetAppliedBatchIds(batch, force);
        if (applied.Count > 0)
        {
            if (!force)
                throw new OperationException(DesignStrings.RevertMigration(applied[^1]));

            throw new OperationException(
                "ClickHouse step migrations are forward-only, so applied migrations cannot be reverted "
                + $"automatically. Manually drop the objects created by [{string.Join(", ", applied)}] and "
                + "delete their rows from the migrations history table, then re-run 'dotnet ef migrations remove'.");
        }

        // Delete every step's migration + Designer file (newest first).
        var ext = codeGenerator.FileExtension;
        for (var i = batch.Count - 1; i >= 0; i--)
        {
            var id = batch[i].Key;

            var migrationFileName = id + ext;
            var migrationPath = TryGetProjectFile(projectDir, migrationFileName);
            if (migrationPath is not null)
            {
                Dependencies.OperationReporter.WriteInformation(DesignStrings.RemovingMigration(id));
                if (!dryRun)
                    File.Delete(migrationPath);
                files.MigrationFile ??= migrationPath;
            }
            else
            {
                Dependencies.OperationReporter.WriteWarning(
                    DesignStrings.NoMigrationFile(migrationFileName, batch[i].Value.ShortDisplayName()));
            }

            var designerFileName = id + ".Designer" + ext;
            var designerPath = TryGetProjectFile(projectDir, designerFileName);
            if (designerPath is not null)
            {
                if (!dryRun)
                    File.Delete(designerPath);
                files.MetadataFile ??= designerPath;
            }
            else
            {
                Dependencies.OperationReporter.WriteVerbose(DesignStrings.NoMigrationMetadataFile(designerFileName));
            }
        }

        // Snapshot: delete it when the batch was the entire history, else revert it to the migration
        // preceding the batch (whose Designer model is that point's correct final model).
        if (batchStart == 0)
        {
            var snapshotFileName = modelSnapshot.GetType().Name + ext;
            var snapshotPath = TryGetProjectFile(projectDir, snapshotFileName);
            if (snapshotPath is not null)
            {
                Dependencies.OperationReporter.WriteInformation(DesignStrings.RemovingSnapshot);
                if (!dryRun)
                    File.Delete(snapshotPath);
                files.SnapshotFile = snapshotPath;
            }
            else
            {
                Dependencies.OperationReporter.WriteWarning(
                    DesignStrings.NoSnapshotFile(snapshotFileName, modelSnapshot.GetType().ShortDisplayName()));
            }
            return files;
        }

        var previous = _migrationsAssembly.CreateMigration(migrations[batchStart - 1].Value, activeProvider);
        var revertedModel = Dependencies.SnapshotModelProcessor.Process(previous.TargetModel, resetVersion: true);
        return WriteSnapshot(files, modelSnapshot, codeGenerator, rootNamespace, projectDir, revertedModel, dryRun);
    }

    private static bool IsStepMigration(string id, TypeInfo typeInfo)
        => StepIdRegex.IsMatch(id) && typeInfo.Name == "_" + id;

    private static string StepPrefix(string id) => id[..id.LastIndexOf('_')];

    private List<string> GetAppliedBatchIds(List<KeyValuePair<string, TypeInfo>> batch, bool force)
    {
        HashSet<string> appliedSet;
        try
        {
            appliedSet = Dependencies.HistoryRepository.GetAppliedMigrations()
                .Select(e => e.MigrationId)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
        }
        catch (Exception ex) when (force)
        {
            Dependencies.OperationReporter.WriteVerbose(ex.ToString());
            Dependencies.OperationReporter.WriteWarning(
                DesignStrings.ForceRemoveMigration(batch[^1].Key, ex.Message));
            return [];
        }

        return batch.Select(b => b.Key).Where(appliedSet.Contains).ToList();
    }

    private MigrationFiles WriteSnapshot(
        MigrationFiles files, ModelSnapshot modelSnapshot, IMigrationsCodeGenerator codeGenerator,
        string? rootNamespace, string projectDir, IModel model, bool dryRun)
    {
        var name = modelSnapshot.GetType().Name;
        var fileName = name + codeGenerator.FileExtension;
        var snapshotNamespace = modelSnapshot.GetType().Namespace!;
        var code = codeGenerator.GenerateSnapshot(snapshotNamespace, _currentContext.Context.GetType(), name, model);

        var path = TryGetProjectFile(projectDir, fileName)
            ?? Path.Combine(GetDirectory(projectDir, null, GetSubNamespace(rootNamespace, snapshotNamespace)), fileName);

        Dependencies.OperationReporter.WriteInformation(DesignStrings.RevertingSnapshot);
        if (!dryRun)
            File.WriteAllText(path, code, Encoding.UTF8);
        return files;
    }

    // Reproduces the base scaffolder's namespace/snapshot-name resolution for the multi-op step
    // path (which bypasses base): combines root + sub namespace, reuses the previous migration's /
    // existing snapshot's namespace when the sub-namespace was defaulted, handles the multi-context
    // "foreign migrations" redirect, and reuses an existing ModelSnapshot type's name.
    private (string MigrationNamespace, string SnapshotNamespace, string SnapshotName,
        string MigrationSubNamespace, string SnapshotSubNamespace)
        ResolveNamespaces(string? rootNamespace, string? subNamespace, Type contextType)
    {
        var rootIsNull = rootNamespace is null;
        var subDefaulted = false;
        if (string.IsNullOrEmpty(subNamespace) && !rootIsNull)
        {
            subDefaulted = true;
            subNamespace = "Migrations";
        }

        var lastTypeInfo = _migrationsAssembly.Migrations.LastOrDefault().Value;

        var migrationNamespace =
            !string.IsNullOrEmpty(rootNamespace) && !string.IsNullOrEmpty(subNamespace)
                ? rootNamespace + "." + subNamespace
                : (!string.IsNullOrEmpty(rootNamespace) ? rootNamespace : subNamespace)!;
        if (subDefaulted)
            migrationNamespace = GetNamespace(lastTypeInfo?.AsType(), migrationNamespace);

        var contextName = contextType.Name;
        var backtick = contextName.IndexOf('`');
        if (backtick != -1)
            contextName = contextName[..backtick];

        // Redirect this context's migrations into a per-context sub-namespace when another context
        // already owns the target namespace, matching base behavior for multi-DbContext projects.
        if (ContainsForeignMigrations(migrationNamespace, contextType))
        {
            if (subDefaulted)
            {
                var builder = new StringBuilder();
                if (!string.IsNullOrEmpty(rootNamespace))
                    builder.Append(rootNamespace).Append('.');
                builder.Append("Migrations.");
                if (contextName.EndsWith("Context", StringComparison.Ordinal))
                    builder.Append(contextName, 0, contextName.Length - "Context".Length);
                else
                    builder.Append(contextName).Append("Migrations");
                migrationNamespace = builder.ToString();
            }
            else
            {
                Dependencies.OperationReporter.WriteWarning(DesignStrings.ForeignMigrations(migrationNamespace));
            }
        }

        var modelSnapshot = _migrationsAssembly.ModelSnapshot;
        var snapshotNamespace = rootIsNull
            ? migrationNamespace
            : GetNamespace(modelSnapshot?.GetType(), migrationNamespace);

        var snapshotName = contextName + "ModelSnapshot";
        if (modelSnapshot is not null)
        {
            var existing = modelSnapshot.GetType().Name;
            if (existing != snapshotName)
            {
                Dependencies.OperationReporter.WriteVerbose(DesignStrings.ReusingSnapshotName(existing));
                snapshotName = existing;
            }
        }

        return (migrationNamespace, snapshotNamespace, snapshotName,
            GetSubNamespace(rootNamespace, migrationNamespace),
            GetSubNamespace(rootNamespace, snapshotNamespace));
    }

    // Mirrors the base scaffolder's private ContainsForeignMigrations: true when another DbContext
    // already owns migrations in the target namespace (so this context's should be redirected).
    private bool ContainsForeignMigrations(string migrationsNamespace, Type contextType)
        => _migrationsAssembly.Assembly.DefinedTypes.Any(t =>
            !t.IsAbstract
            && !t.IsGenericTypeDefinition
            && t.Namespace == migrationsNamespace
            && t.IsSubclassOf(typeof(Migration))
            && t.GetCustomAttribute<DbContextAttribute>() is { } attr
            && attr.ContextType != contextType);

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

        // Keep the prior model around for the rename confirmation's before/after display.
        _sourceRelationalModel = lastModel;

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
