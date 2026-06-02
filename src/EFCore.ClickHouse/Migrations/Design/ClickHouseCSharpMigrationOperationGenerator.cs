using ClickHouse.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations.Design;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

// CSharpMigrationOperationGenerator and its dependencies are internal EF Core APIs.
#pragma warning disable EF1001

namespace ClickHouse.EntityFrameworkCore.Migrations.Design;

/// <summary>
/// Teaches the C# migration scaffolder how to render this provider's custom migration operations
/// (materialized view and database create/drop). The base generator only knows the built-in
/// operation types and throws for anything else, so each custom operation is emitted as a call to
/// the matching <c>ClickHouseMigrationBuilderExtensions</c> method. The base loop has already
/// written the <c>migrationBuilder</c> receiver, so each method below appends only the
/// <c>.MethodName(...)</c> continuation (callers must import
/// <c>ClickHouse.EntityFrameworkCore.Extensions</c>; the scaffolder ensures that using is present).
/// </summary>
public class ClickHouseCSharpMigrationOperationGenerator : CSharpMigrationOperationGenerator
{
    public ClickHouseCSharpMigrationOperationGenerator(CSharpMigrationOperationGeneratorDependencies dependencies)
        : base(dependencies)
    {
    }

    // The base dispatches via Generate((dynamic)operation, builder) from its own call site, which
    // cannot bind to overloads declared on this derived type. The custom operations therefore reach
    // the base's catch-all Generate(MigrationOperation, ...) (which throws), so we override that
    // catch-all and dispatch to the typed methods below ourselves.
    protected override void Generate(MigrationOperation operation, IndentedStringBuilder builder)
    {
        switch (operation)
        {
            case ClickHouseCreateMaterializedViewOperation createMv:
                Generate(createMv, builder);
                break;
            case ClickHouseDropMaterializedViewOperation dropMv:
                Generate(dropMv, builder);
                break;
            case ClickHouseCreateDatabaseOperation createDb:
                Generate(createDb, builder);
                break;
            case ClickHouseDropDatabaseOperation dropDb:
                Generate(dropDb, builder);
                break;
            case ClickHouseCreateDictionaryOperation createDict:
                Generate(createDict, builder);
                break;
            case ClickHouseDropDictionaryOperation dropDict:
                Generate(dropDict, builder);
                break;
            default:
                base.Generate(operation, builder);
                break;
        }
    }

    protected virtual void Generate(ClickHouseCreateMaterializedViewOperation operation, IndentedStringBuilder builder)
    {
        var code = Dependencies.CSharpHelper;
        builder.AppendLine(".CreateClickHouseMaterializedView(");
        using (builder.Indent())
        {
            var args = new List<string>
            {
                $"viewName: {code.Literal(operation.ViewName)}",
                $"targetTable: {code.Literal(operation.TargetTable)}",
                $"selectQuery: {code.Literal(operation.SelectQuery)}",
            };
            if (operation.Database != null)
                args.Add($"database: {code.Literal(operation.Database)}");
            if (operation.TargetDatabase != null)
                args.Add($"targetDatabase: {code.Literal(operation.TargetDatabase)}");
            if (operation.Cluster != null)
                args.Add($"cluster: {code.Literal(operation.Cluster)}");
            if (operation.IfNotExists)
                args.Add($"ifNotExists: {code.Literal(true)}");
            if (operation.Populate)
                args.Add($"populate: {code.Literal(true)}");

            AppendArgs(builder, args);
        }
    }

    protected virtual void Generate(ClickHouseDropMaterializedViewOperation operation, IndentedStringBuilder builder)
    {
        var code = Dependencies.CSharpHelper;
        builder.AppendLine(".DropClickHouseMaterializedView(");
        using (builder.Indent())
        {
            var args = new List<string> { $"viewName: {code.Literal(operation.ViewName)}" };
            if (operation.Database != null)
                args.Add($"database: {code.Literal(operation.Database)}");
            if (operation.Cluster != null)
                args.Add($"cluster: {code.Literal(operation.Cluster)}");
            if (operation.IfExists)
                args.Add($"ifExists: {code.Literal(true)}");

            AppendArgs(builder, args);
        }
    }

    protected virtual void Generate(ClickHouseCreateDictionaryOperation operation, IndentedStringBuilder builder)
    {
        var code = Dependencies.CSharpHelper;
        builder.AppendLine(".CreateClickHouseDictionary(");
        using (builder.Indent())
        {
            var columns = string.Join(", ", operation.Columns.Select(c => c.Default is null
                ? $"new ClickHouseDictionaryColumn({code.Literal(c.Name)}, {code.Literal(c.Type)})"
                : $"new ClickHouseDictionaryColumn({code.Literal(c.Name)}, {code.Literal(c.Type)}, {code.Literal(c.Default)})"));
            var keys = string.Join(", ", operation.KeyColumns.Select(k => code.Literal(k)));

            var args = new List<string>
            {
                $"name: {code.Literal(operation.DictionaryName)}",
                $"columns: new[] {{ {columns} }}",
                $"keyColumns: new[] {{ {keys} }}",
                $"sourceTable: {code.Literal(operation.SourceTable)}",
                $"layout: {code.Literal(operation.Layout)}",
            };
            if (operation.SourceDatabase != null)
                args.Add($"sourceDatabase: {code.Literal(operation.SourceDatabase)}");
            if (operation.SourceNamedCollection != null)
                args.Add($"sourceNamedCollection: {code.Literal(operation.SourceNamedCollection)}");
            if (operation.SourceHost != null)
                args.Add($"sourceHost: {code.Literal(operation.SourceHost)}");
            if (operation.SourcePort is { } sourcePort)
                args.Add($"sourcePort: {code.Literal(sourcePort)}");
            if (operation.SourceUser != null)
                args.Add($"sourceUser: {code.Literal(operation.SourceUser)}");
            if (operation.SourcePassword != null)
                args.Add($"sourcePassword: {code.Literal(operation.SourcePassword)}");
            if (operation.LayoutParams != null)
                args.Add($"layoutParams: {code.Literal(operation.LayoutParams)}");
            if (operation.LifetimeMin is { } min)
                args.Add($"lifetimeMin: {code.Literal(min)}");
            if (operation.LifetimeMax is { } max)
                args.Add($"lifetimeMax: {code.Literal(max)}");
            if (operation.Database != null)
                args.Add($"database: {code.Literal(operation.Database)}");
            if (operation.Cluster != null)
                args.Add($"cluster: {code.Literal(operation.Cluster)}");
            if (operation.IfNotExists)
                args.Add($"ifNotExists: {code.Literal(true)}");
            if (operation.OrReplace)
                args.Add($"orReplace: {code.Literal(true)}");

            AppendArgs(builder, args);
        }
    }

    protected virtual void Generate(ClickHouseDropDictionaryOperation operation, IndentedStringBuilder builder)
    {
        var code = Dependencies.CSharpHelper;
        builder.AppendLine(".DropClickHouseDictionary(");
        using (builder.Indent())
        {
            var args = new List<string> { $"name: {code.Literal(operation.DictionaryName)}" };
            if (operation.Database != null)
                args.Add($"database: {code.Literal(operation.Database)}");
            if (operation.Cluster != null)
                args.Add($"cluster: {code.Literal(operation.Cluster)}");
            if (operation.IfExists)
                args.Add($"ifExists: {code.Literal(true)}");

            AppendArgs(builder, args);
        }
    }

    protected virtual void Generate(ClickHouseCreateDatabaseOperation operation, IndentedStringBuilder builder)
    {
        builder.AppendLine(".CreateClickHouseDatabase(");
        using (builder.Indent())
            AppendArgs(builder, [$"name: {Dependencies.CSharpHelper.Literal(operation.Name)}"]);
    }

    protected virtual void Generate(ClickHouseDropDatabaseOperation operation, IndentedStringBuilder builder)
    {
        builder.AppendLine(".DropClickHouseDatabase(");
        using (builder.Indent())
            AppendArgs(builder, [$"name: {Dependencies.CSharpHelper.Literal(operation.Name)}"]);
    }

    // Emits one argument per line, comma-separated, closing with ")". The base loop appends the
    // trailing ";".
    private static void AppendArgs(IndentedStringBuilder builder, IReadOnlyList<string> args)
    {
        for (var i = 0; i < args.Count; i++)
        {
            builder.Append(args[i]);
            if (i < args.Count - 1)
                builder.AppendLine(",");
        }

        builder.Append(")");
    }
}
