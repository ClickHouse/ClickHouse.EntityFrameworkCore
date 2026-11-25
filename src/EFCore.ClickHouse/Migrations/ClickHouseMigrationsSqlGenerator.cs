using ClickHouse.EntityFrameworkCore.Infrastructure.Internal;
using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using ClickHouse.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Migrations;

public class ClickHouseMigrationsSqlGenerator : MigrationsSqlGenerator
{
    private readonly string? _clusterName;

    public ClickHouseMigrationsSqlGenerator(
        MigrationsSqlGeneratorDependencies dependencies,
        IDbContextOptions options)
        : base(dependencies)
    {
        _clusterName = options.FindExtension<ClickHouseOptionsExtension>()?.ClusterName;
    }

    // ClickHouse does not support transactions — suppress on all statements.
    protected override void EndStatement(MigrationCommandListBuilder builder, bool suppressTransaction = true)
        => base.EndStatement(builder, suppressTransaction: true);

    // Custom operation dispatch

    protected override void Generate(MigrationOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        switch (operation)
        {
            case ClickHouseCreateDatabaseOperation createDb:
                Generate(createDb, builder);
                return;
            case ClickHouseDropDatabaseOperation dropDb:
                Generate(dropDb, builder);
                return;
            default:
                base.Generate(operation, model, builder);
                return;
        }
    }

    protected virtual void Generate(ClickHouseCreateDatabaseOperation operation, MigrationCommandListBuilder builder)
    {
        builder
            .Append("CREATE DATABASE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name));
        AppendOnCluster(builder);
        EndStatement(builder, suppressTransaction: true);
    }

    protected virtual void Generate(ClickHouseDropDatabaseOperation operation, MigrationCommandListBuilder builder)
    {
        builder
            .Append("DROP DATABASE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name));
        AppendOnCluster(builder);
        EndStatement(builder, suppressTransaction: true);
    }

    // ON CLUSTER: ClickHouse puts the clause after the table or database name, before the column list
    // and before the ALTER commands.

    private void AppendOnCluster(MigrationCommandListBuilder builder)
    {
        if (_clusterName is null)
            return;

        builder
            .Append(" ON CLUSTER ")
            .Append(Dependencies.TypeMappingSource.GetMapping(typeof(string)).GenerateSqlLiteral(_clusterName));
    }

    private void AppendAlterTable(MigrationCommandListBuilder builder, string table, string? schema)
    {
        builder
            .Append("ALTER TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(table, schema));
        AppendOnCluster(builder);
    }

    // CREATE TABLE with ENGINE clause

    protected override void Generate(
        CreateTableOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        builder
            .Append("CREATE TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name, operation.Schema));
        AppendOnCluster(builder);
        builder.AppendLine(" (");

        using (builder.Indent())
        {
            CreateTableColumns(operation, model, builder);
            CreateTableConstraints(operation, model, builder);
            builder.AppendLine();
        }

        builder.Append(")");

        GenerateEngineClause(operation, builder);

        builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
        EndStatement(builder);
    }

    protected override void Generate(
        DropTableOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        builder
            .Append("DROP TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name, operation.Schema));
        AppendOnCluster(builder);

        if (terminate)
        {
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder);
        }
    }

    // Column definition: ClickHouse nullable wrapping, codec, TTL, comment

    protected override void ColumnDefinition(
        string? schema,
        string table,
        string name,
        ColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        if (!string.IsNullOrEmpty(operation.ComputedColumnSql))
        {
            ComputedColumnDefinition(schema, table, name, operation, model, builder);
            return;
        }

        var columnType = operation.ColumnType ?? GetColumnType(schema, table, name, operation, model)!;

        // Wrap nullable scalar types in Nullable(T).
        // Skip: arrays (CLR T[] or List<T> → Array(T)), Map, Json, Tuple, Variant —
        // ClickHouse does not support Nullable(Array(...)) etc.
        if (operation.IsNullable && !IsNonNullableContainerType(operation.ClrType, columnType))
        {
            columnType = $"Nullable({columnType})";
        }

        builder
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(name))
            .Append(" ")
            .Append(columnType);

        // DEFAULT
        var defaultValue = operation.DefaultValueSql;
        if (string.IsNullOrWhiteSpace(defaultValue) && operation.DefaultValue is not null)
        {
            var typeMapping = (!string.IsNullOrEmpty(operation.ColumnType)
                ? Dependencies.TypeMappingSource.FindMapping(operation.DefaultValue.GetType(), operation.ColumnType)
                : null) ?? Dependencies.TypeMappingSource.FindMapping(operation.DefaultValue.GetType())!;
            defaultValue = typeMapping.GenerateSqlLiteral(operation.DefaultValue);
        }

        if (!string.IsNullOrWhiteSpace(defaultValue))
            builder.Append(" DEFAULT ").Append(defaultValue);

        // ClickHouse column definition order: DEFAULT → COMMENT → CODEC → TTL

        // COMMENT
        var comment = operation.FindAnnotation(ClickHouseAnnotationNames.ColumnComment);
        if (comment?.Value is string commentStr && !string.IsNullOrWhiteSpace(commentStr))
        {
            var escaped = commentStr.Replace("'", "\\'");
            builder.Append($" COMMENT '{escaped}'");
        }

        // CODEC
        var codec = operation.FindAnnotation(ClickHouseAnnotationNames.ColumnCodec);
        if (codec?.Value is string codecStr && !string.IsNullOrWhiteSpace(codecStr))
            builder.Append($" CODEC({codecStr})");

        // TTL
        var columnTtl = operation.FindAnnotation(ClickHouseAnnotationNames.ColumnTtl);
        if (columnTtl?.Value is string ttlStr && !string.IsNullOrWhiteSpace(ttlStr))
            builder.Append($" TTL {ttlStr}");
    }

    protected override void ComputedColumnDefinition(
        string? schema,
        string table,
        string name,
        ColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        var keyword = operation.IsStored == true ? " MATERIALIZED " : " ALIAS ";
        var columnType = operation.ColumnType ?? GetColumnType(schema, table, name, operation, model)!;

        if (operation.IsNullable && !IsNonNullableContainerType(operation.ClrType, columnType))
        {
            columnType = $"Nullable({columnType})";
        }

        builder
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(name))
            .Append(" ")
            .Append(columnType)
            .Append(keyword)
            .Append(operation.ComputedColumnSql!);
    }

    // Suppress primary key, foreign key, unique constraints — ClickHouse doesn't support them as SQL constraints

    protected override void CreateTableConstraints(
        CreateTableOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        CreateTableCheckConstraints(operation, model, builder);
    }

    // ALTER TABLE operations

    protected override void Generate(
        AddColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        AppendAlterTable(builder, operation.Table, operation.Schema);
        builder.Append(" ADD COLUMN ");

        ColumnDefinition(operation, model, builder);
        EndStatement(builder);
    }

    protected override void Generate(
        DropColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        AppendAlterTable(builder, operation.Table, operation.Schema);
        builder
            .Append(" DROP COLUMN ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name));

        EndStatement(builder);
    }

    protected override void Generate(
        AlterColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        AppendAlterTable(builder, operation.Table, operation.Schema);
        builder.Append(" MODIFY COLUMN ");

        ColumnDefinition(operation.Schema, operation.Table, operation.Name, operation, model, builder);
        EndStatement(builder);

        // Emit REMOVE statements for column annotations that were present on the old column but not the new one.
        // ClickHouse requires explicit REMOVE CODEC / REMOVE TTL / REMOVE COMMENT — a bare MODIFY COLUMN
        // does not clear these attributes.
        EmitColumnAnnotationRemovals(operation, builder);
    }

    private void EmitColumnAnnotationRemovals(AlterColumnOperation operation, MigrationCommandListBuilder builder)
    {
        ReadOnlySpan<string> removableAnnotations =
        [
            ClickHouseAnnotationNames.ColumnCodec,
            ClickHouseAnnotationNames.ColumnTtl,
            ClickHouseAnnotationNames.ColumnComment,
        ];

        foreach (var annotationName in removableAnnotations)
        {
            var oldValue = (string?)operation.OldColumn.FindAnnotation(annotationName)?.Value;
            var newValue = (string?)operation.FindAnnotation(annotationName)?.Value;

            if (string.IsNullOrWhiteSpace(oldValue) || !string.IsNullOrWhiteSpace(newValue))
                continue;

            var keyword = annotationName switch
            {
                ClickHouseAnnotationNames.ColumnCodec => "REMOVE CODEC",
                ClickHouseAnnotationNames.ColumnTtl => "REMOVE TTL",
                ClickHouseAnnotationNames.ColumnComment => "REMOVE COMMENT",
                _ => null
            };

            if (keyword is null)
                continue;

            AppendAlterTable(builder, operation.Table, operation.Schema);
            builder
                .Append(" MODIFY COLUMN ")
                .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name))
                .Append(" ")
                .Append(keyword);
            EndStatement(builder);
        }
    }

    protected override void Generate(
        RenameColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        AppendAlterTable(builder, operation.Table, operation.Schema);
        builder
            .Append(" RENAME COLUMN ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name))
            .Append(" TO ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.NewName));

        EndStatement(builder);
    }

    protected override void Generate(
        RenameTableOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        builder
            .Append("RENAME TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name, operation.Schema))
            .Append(" TO ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.NewName!, operation.NewSchema));
        AppendOnCluster(builder);

        EndStatement(builder);
    }

    // ALTER TABLE — reject ClickHouse metadata changes (engine, ORDER BY, etc. are immutable)

    protected override void Generate(
        AlterTableOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        // Collect all ClickHouse annotations from old and new
        var oldAnnotations = operation.OldTable.GetAnnotations()
            .Where(a => a.Name.StartsWith(ClickHouseAnnotationNames.Prefix, StringComparison.Ordinal))
            .ToDictionary(a => a.Name, a => a.Value);
        var newAnnotations = operation.GetAnnotations()
            .Where(a => a.Name.StartsWith(ClickHouseAnnotationNames.Prefix, StringComparison.Ordinal))
            .ToDictionary(a => a.Name, a => a.Value);

        // Find any annotation that was added, removed, or changed
        var allKeys = oldAnnotations.Keys.Union(newAnnotations.Keys);
        foreach (var key in allKeys)
        {
            oldAnnotations.TryGetValue(key, out var oldVal);
            newAnnotations.TryGetValue(key, out var newVal);

            if (!AnnotationValuesEqual(oldVal, newVal))
            {
                var shortName = key[ClickHouseAnnotationNames.Prefix.Length..];
                throw new NotSupportedException(
                    $"ClickHouse does not support changing table metadata '{shortName}' via ALTER TABLE. " +
                    "Recreate the table instead.");
            }
        }

        // Delegate to base for non-ClickHouse annotation changes (e.g., comments)
        base.Generate(operation, model, builder);
    }

    private static bool AnnotationValuesEqual(object? a, object? b)
    {
        if (a is null && b is null) return true;
        if (a is null || b is null) return false;
        if (a is string[] arrA && b is string[] arrB) return arrA.SequenceEqual(arrB);
        return a.Equals(b);
    }

    // Data-skipping indices

    protected override void Generate(
        CreateIndexOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        if (operation.IsUnique)
            throw new NotSupportedException("ClickHouse does not support unique indexes.");

        var indexType = (string?)operation.FindAnnotation(ClickHouseAnnotationNames.SkippingIndexType)?.Value;
        if (indexType is null)
        {
            // Standard index — ClickHouse doesn't support CREATE INDEX syntax
            // Skip silently rather than error, since EF may generate these for PK-like indices
            return;
        }

        var indexParams = (string?)operation.FindAnnotation(ClickHouseAnnotationNames.SkippingIndexParams)?.Value;
        var granularity = (int?)operation.FindAnnotation(ClickHouseAnnotationNames.SkippingIndexGranularity)?.Value ?? 1;

        AppendAlterTable(builder, operation.Table, operation.Schema);
        builder
            .Append(" ADD INDEX ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name))
            .Append(" (")
            .Append(string.Join(", ", operation.Columns.Select(c =>
                Dependencies.SqlGenerationHelper.DelimitIdentifier(c))))
            .Append(") TYPE ")
            .Append(indexType);

        if (!string.IsNullOrWhiteSpace(indexParams))
            builder.Append($"({indexParams})");

        builder.Append($" GRANULARITY {granularity}");
        EndStatement(builder);
    }

    protected override void Generate(
        DropIndexOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        // Only emit DROP INDEX for skipping indexes (same symmetry as CreateIndexOperation).
        // Standard EF indexes are not created in ClickHouse, so dropping them is a no-op.
        var indexType = (string?)operation.FindAnnotation(ClickHouseAnnotationNames.SkippingIndexType)?.Value;
        if (indexType is null)
            return;

        AppendAlterTable(builder, operation.Table!, operation.Schema);
        builder
            .Append(" DROP INDEX ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name));

        EndStatement(builder);
    }

    // Unsupported operations

    protected override void Generate(AddForeignKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true)
        => throw new NotSupportedException("ClickHouse does not support foreign key constraints.");

    protected override void Generate(DropForeignKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true)
        => throw new NotSupportedException("ClickHouse does not support foreign key constraints.");

    protected override void Generate(AddUniqueConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException("ClickHouse does not support unique constraints.");

    protected override void Generate(DropUniqueConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException("ClickHouse does not support unique constraints.");

    protected override void Generate(AddPrimaryKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true)
    {
        // No-op: ClickHouse primary key is structural (ORDER BY), not a constraint
    }

    protected override void Generate(DropPrimaryKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true)
    {
        // No-op: ClickHouse primary key is structural (ORDER BY), not a constraint
    }

    protected override void Generate(CreateSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException("ClickHouse does not support sequences.");

    protected override void Generate(AlterSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException("ClickHouse does not support sequences.");

    protected override void Generate(DropSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException("ClickHouse does not support sequences.");

    protected override void Generate(RenameSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException("ClickHouse does not support sequences.");

    protected override void Generate(EnsureSchemaOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException("ClickHouse does not support schemas. Use databases instead.");

    // ENGINE clause generation

    private void GenerateEngineClause(CreateTableOperation operation, MigrationCommandListBuilder builder)
    {
        var engine = (string?)operation.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value
            ?? ClickHouseAnnotationNames.MergeTree;

        var replicated = operation.FindAnnotation(ClickHouseAnnotationNames.Replicated)?.Value is true;
        if (replicated && !ClickHouseEngines.IsMergeTreeFamily(engine))
        {
            throw new NotSupportedException(
                $"The '{engine}' engine has no Replicated variant. Only MergeTree family engines can be replicated.");
        }

        builder.AppendLine();

        // ENGINE = EngineName or ENGINE = EngineName(args)
        // Simple engines (Log, TinyLog, StripeLog, Memory) use bare names without parentheses.
        if (IsSimpleEngine(engine))
        {
            builder.Append($"ENGINE = {engine}");
        }
        else
        {
            var args = new List<string>();
            if (replicated)
            {
                // Replicated<Engine>(zoo_path, replica_name, <engine args>). ClickHouse needs both or neither
                // of the first two arguments. With neither, it uses default_replica_path and default_replica_name.
                AddReplicationArg(operation, ClickHouseAnnotationNames.ReplicatedZooKeeperPath, args);
                AddReplicationArg(operation, ClickHouseAnnotationNames.ReplicatedReplicaName, args);
            }

            AddEngineArgs(operation, engine, args);

            var engineName = replicated ? ClickHouseEngines.ReplicatedPrefix + engine : engine;
            builder.Append($"ENGINE = {engineName}({string.Join(", ", args)})");
        }

        // ORDER BY
        var orderBy = (string[]?)operation.FindAnnotation(ClickHouseAnnotationNames.OrderBy)?.Value;
        if (orderBy is { Length: > 0 })
        {
            builder.AppendLine();
            builder.Append("ORDER BY (");
            builder.Append(string.Join(", ", orderBy.Select(QuoteColumnOrExpression)));
            builder.Append(")");
        }
        else if (ClickHouseEngines.IsMergeTreeFamily(engine))
        {
            builder.AppendLine();
            builder.Append("ORDER BY tuple()");
        }

        // PARTITION BY
        var partitionBy = (string[]?)operation.FindAnnotation(ClickHouseAnnotationNames.PartitionBy)?.Value;
        if (partitionBy is { Length: > 0 })
        {
            builder.AppendLine();
            builder.Append("PARTITION BY ");
            if (partitionBy.Length == 1)
                builder.Append(QuoteColumnOrExpression(partitionBy[0]));
            else
            {
                builder.Append("(");
                builder.Append(string.Join(", ", partitionBy.Select(QuoteColumnOrExpression)));
                builder.Append(")");
            }
        }

        // PRIMARY KEY
        var primaryKey = (string[]?)operation.FindAnnotation(ClickHouseAnnotationNames.PrimaryKey)?.Value;
        if (primaryKey is { Length: > 0 })
        {
            builder.AppendLine();
            builder.Append("PRIMARY KEY (");
            builder.Append(string.Join(", ", primaryKey.Select(QuoteColumnOrExpression)));
            builder.Append(")");
        }

        // SAMPLE BY
        var sampleBy = (string[]?)operation.FindAnnotation(ClickHouseAnnotationNames.SampleBy)?.Value;
        if (sampleBy is { Length: > 0 })
        {
            builder.AppendLine();
            builder.Append("SAMPLE BY ");
            if (sampleBy.Length == 1)
                builder.Append(QuoteColumnOrExpression(sampleBy[0]));
            else
            {
                builder.Append("(");
                builder.Append(string.Join(", ", sampleBy.Select(QuoteColumnOrExpression)));
                builder.Append(")");
            }
        }

        // TTL
        var ttl = (string?)operation.FindAnnotation(ClickHouseAnnotationNames.Ttl)?.Value;
        if (!string.IsNullOrWhiteSpace(ttl))
        {
            builder.AppendLine();
            builder.Append($"TTL {ttl}");
        }

        // SETTINGS
        GenerateSettingsClause(operation, builder);
    }

    private void AddReplicationArg(CreateTableOperation operation, string annotationName, List<string> args)
    {
        if (operation.FindAnnotation(annotationName)?.Value is string value)
            args.Add(Dependencies.TypeMappingSource.GetMapping(typeof(string)).GenerateSqlLiteral(value));
    }

    private void AddEngineArgs(CreateTableOperation operation, string engine, List<string> args)
    {
        switch (engine)
        {
            case ClickHouseAnnotationNames.ReplacingMergeTree:
                var version = (string?)operation.FindAnnotation(ClickHouseAnnotationNames.ReplacingMergeTreeVersion)?.Value;
                var isDeleted = (string?)operation.FindAnnotation(ClickHouseAnnotationNames.ReplacingMergeTreeIsDeleted)?.Value;
                if (version is not null)
                    args.Add(QuoteColumnOrExpression(version));
                if (isDeleted is not null)
                    args.Add(QuoteColumnOrExpression(isDeleted));
                break;

            case ClickHouseAnnotationNames.SummingMergeTree:
                var columns = (string[]?)operation.FindAnnotation(ClickHouseAnnotationNames.SummingMergeTreeColumns)?.Value;
                if (columns is { Length: > 0 })
                {
                    var quotedColumns = columns.Select(QuoteColumnOrExpression);
                    // SummingMergeTree accepts a single optional parameter: the column to sum, or a
                    // tuple of columns. Multiple columns must be wrapped in a tuple — emitting them as
                    // a comma-separated argument list produces invalid DDL (NUMBER_OF_ARGUMENTS_DOESNT_MATCH).
                    args.Add(columns.Length == 1
                        ? quotedColumns.First()
                        : $"({string.Join(", ", quotedColumns)})");
                }
                break;

            case ClickHouseAnnotationNames.CollapsingMergeTree:
                var sign = (string?)operation.FindAnnotation(ClickHouseAnnotationNames.CollapsingMergeTreeSign)?.Value;
                if (sign is not null)
                    args.Add(QuoteColumnOrExpression(sign));
                break;

            case ClickHouseAnnotationNames.VersionedCollapsingMergeTree:
                var vcSign = (string?)operation.FindAnnotation(ClickHouseAnnotationNames.VersionedCollapsingMergeTreeSign)?.Value;
                var vcVersion = (string?)operation.FindAnnotation(ClickHouseAnnotationNames.VersionedCollapsingMergeTreeVersion)?.Value;
                if (vcSign is not null) args.Add(QuoteColumnOrExpression(vcSign));
                if (vcVersion is not null) args.Add(QuoteColumnOrExpression(vcVersion));
                break;

            case ClickHouseAnnotationNames.GraphiteMergeTree:
                var config = (string?)operation.FindAnnotation(ClickHouseAnnotationNames.GraphiteMergeTreeConfigSection)?.Value;
                if (config is not null)
                    args.Add($"'{config}'");
                break;
        }
    }

    private void GenerateSettingsClause(CreateTableOperation operation, MigrationCommandListBuilder builder)
    {
        var settings = new Dictionary<string, string>();
        foreach (var annotation in operation.GetAnnotations())
        {
            if (annotation.Name.StartsWith(ClickHouseAnnotationNames.SettingPrefix, StringComparison.Ordinal)
                && annotation.Value is string value)
            {
                var key = annotation.Name[ClickHouseAnnotationNames.SettingPrefix.Length..];
                settings[key] = value;
            }
        }

        if (settings.Count == 0)
            return;

        builder.AppendLine();
        builder.Append("SETTINGS ");
        builder.Append(string.Join(", ", settings.Select(kv => $"{kv.Key} = {kv.Value}")));
    }

    private string QuoteColumnOrExpression(string columnOrExpr)
    {
        // Simple identifier (letters, digits, underscore) → backtick quote.
        // Anything else (operators, parentheses, spaces) → SQL expression, emit verbatim.
        if (IsSimpleIdentifier(columnOrExpr))
            return Dependencies.SqlGenerationHelper.DelimitIdentifier(columnOrExpr);

        return columnOrExpr;
    }

    private static bool IsSimpleIdentifier(string s)
    {
        if (s.Length == 0)
            return false;

        if (s[0] != '_' && !char.IsLetter(s[0]))
            return false;

        for (var i = 1; i < s.Length; i++)
        {
            if (s[i] != '_' && !char.IsLetterOrDigit(s[i]))
                return false;
        }

        return true;
    }

    /// <summary>
    /// Returns true for CLR/store types that ClickHouse does not allow inside Nullable().
    /// Array, Map, Tuple, Variant, Dynamic, Json — and already-Nullable columns.
    /// </summary>
    private static bool IsNonNullableContainerType(Type? clrType, string columnType)
    {
        // CLR array (T[])
        if (clrType?.IsArray == true)
            return true;

        // List<T> → maps to Array(T) at the store level
        if (clrType is { IsGenericType: true } && clrType.GetGenericTypeDefinition() == typeof(List<>))
            return true;

        // Store-type check covers Array, Map, Tuple, Variant, Dynamic, Json, and already-wrapped Nullable.
        // LowCardinality is included because ClickHouse rejects Nullable(LowCardinality(...)) — the user
        // must explicitly write LowCardinality(Nullable(...)) when nullable semantics are needed.
        return columnType.StartsWith("Nullable(", StringComparison.OrdinalIgnoreCase)
            || columnType.StartsWith("LowCardinality(", StringComparison.OrdinalIgnoreCase)
            || columnType.StartsWith("Array(", StringComparison.OrdinalIgnoreCase)
            || columnType.StartsWith("Map(", StringComparison.OrdinalIgnoreCase)
            || columnType.StartsWith("Tuple(", StringComparison.OrdinalIgnoreCase)
            || columnType.StartsWith("Variant(", StringComparison.OrdinalIgnoreCase)
            || columnType.StartsWith("Json", StringComparison.OrdinalIgnoreCase)
            || columnType.Equals("Dynamic", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsSimpleEngine(string engine)
        => engine is ClickHouseAnnotationNames.TinyLog
            or ClickHouseAnnotationNames.StripeLog
            or ClickHouseAnnotationNames.Log
            or ClickHouseAnnotationNames.Memory;
}
