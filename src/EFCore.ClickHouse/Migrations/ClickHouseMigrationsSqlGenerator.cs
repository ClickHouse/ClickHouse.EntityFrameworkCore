using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using ClickHouse.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace ClickHouse.EntityFrameworkCore.Migrations;

public class ClickHouseMigrationsSqlGenerator : MigrationsSqlGenerator
{
    public ClickHouseMigrationsSqlGenerator(MigrationsSqlGeneratorDependencies dependencies)
        : base(dependencies)
    {
    }

    // ClickHouse does not support transactions — suppress on all statements.
    protected new void EndStatement(MigrationCommandListBuilder builder, bool suppressTransaction = true)
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
        EndStatement(builder, suppressTransaction: true);
    }

    protected virtual void Generate(ClickHouseDropDatabaseOperation operation, MigrationCommandListBuilder builder)
    {
        builder
            .Append("DROP DATABASE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name));
        EndStatement(builder, suppressTransaction: true);
    }

    // CREATE TABLE with ENGINE clause

    protected override void Generate(
        CreateTableOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        base.Generate(operation, model, builder, terminate: false);

        GenerateEngineClause(operation, builder);

        builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
        EndStatement(builder);
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

        // Wrap nullable non-array types in Nullable(T)
        // Skip Json type — ClickHouse Json returns {} for NULL, not SQL NULL
        if (operation.IsNullable && operation.ClrType?.IsArray != true
            && !columnType.StartsWith("Nullable(", StringComparison.OrdinalIgnoreCase)
            && !columnType.StartsWith("Json", StringComparison.OrdinalIgnoreCase))
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

        if (operation.IsNullable && operation.ClrType?.IsArray != true
            && !columnType.StartsWith("Nullable(", StringComparison.OrdinalIgnoreCase))
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
        builder
            .Append("ALTER TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table, operation.Schema))
            .Append(" ADD COLUMN ");

        ColumnDefinition(operation, model, builder);
        EndStatement(builder);
    }

    protected override void Generate(
        DropColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        builder
            .Append("ALTER TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table, operation.Schema))
            .Append(" DROP COLUMN ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name));

        EndStatement(builder);
    }

    protected override void Generate(
        AlterColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        builder
            .Append("ALTER TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table, operation.Schema))
            .Append(" MODIFY COLUMN ");

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

            builder
                .Append("ALTER TABLE ")
                .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table, operation.Schema))
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
        builder
            .Append("ALTER TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table, operation.Schema))
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

        builder
            .Append("ALTER TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table, operation.Schema))
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

        builder
            .Append("ALTER TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table!, operation.Schema))
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

        builder.AppendLine();

        // ENGINE = EngineName or ENGINE = EngineName(args)
        // Simple engines (Log, TinyLog, StripeLog, Memory) use bare names without parentheses.
        if (IsSimpleEngine(engine))
        {
            builder.Append($"ENGINE = {engine}");
        }
        else
        {
            builder.Append($"ENGINE = {engine}(");
            GenerateEngineArgs(operation, engine, builder);
            builder.Append(")");
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
        else if (IsMergeTreeFamily(engine))
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

    private void GenerateEngineArgs(CreateTableOperation operation, string engine, MigrationCommandListBuilder builder)
    {
        switch (engine)
        {
            case ClickHouseAnnotationNames.ReplacingMergeTree:
                var version = (string?)operation.FindAnnotation(ClickHouseAnnotationNames.ReplacingMergeTreeVersion)?.Value;
                var isDeleted = (string?)operation.FindAnnotation(ClickHouseAnnotationNames.ReplacingMergeTreeIsDeleted)?.Value;
                var args = new List<string>();
                if (version is not null)
                    args.Add(QuoteColumnOrExpression(version));
                if (isDeleted is not null)
                    args.Add(QuoteColumnOrExpression(isDeleted));
                builder.Append(string.Join(", ", args));
                break;

            case ClickHouseAnnotationNames.SummingMergeTree:
                var columns = (string[]?)operation.FindAnnotation(ClickHouseAnnotationNames.SummingMergeTreeColumns)?.Value;
                if (columns is { Length: > 0 })
                    builder.Append(string.Join(", ", columns.Select(QuoteColumnOrExpression)));
                break;

            case ClickHouseAnnotationNames.CollapsingMergeTree:
                var sign = (string?)operation.FindAnnotation(ClickHouseAnnotationNames.CollapsingMergeTreeSign)?.Value;
                if (sign is not null)
                    builder.Append(QuoteColumnOrExpression(sign));
                break;

            case ClickHouseAnnotationNames.VersionedCollapsingMergeTree:
                var vcSign = (string?)operation.FindAnnotation(ClickHouseAnnotationNames.VersionedCollapsingMergeTreeSign)?.Value;
                var vcVersion = (string?)operation.FindAnnotation(ClickHouseAnnotationNames.VersionedCollapsingMergeTreeVersion)?.Value;
                var vcArgs = new List<string>();
                if (vcSign is not null) vcArgs.Add(QuoteColumnOrExpression(vcSign));
                if (vcVersion is not null) vcArgs.Add(QuoteColumnOrExpression(vcVersion));
                builder.Append(string.Join(", ", vcArgs));
                break;

            case ClickHouseAnnotationNames.GraphiteMergeTree:
                var config = (string?)operation.FindAnnotation(ClickHouseAnnotationNames.GraphiteMergeTreeConfigSection)?.Value;
                if (config is not null)
                    builder.Append($"'{config}'");
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

    private static bool IsMergeTreeFamily(string engine)
        => engine is ClickHouseAnnotationNames.MergeTree
            or ClickHouseAnnotationNames.ReplacingMergeTree
            or ClickHouseAnnotationNames.SummingMergeTree
            or ClickHouseAnnotationNames.AggregatingMergeTree
            or ClickHouseAnnotationNames.CollapsingMergeTree
            or ClickHouseAnnotationNames.VersionedCollapsingMergeTree
            or ClickHouseAnnotationNames.GraphiteMergeTree;

    private static bool IsSimpleEngine(string engine)
        => engine is ClickHouseAnnotationNames.TinyLog
            or ClickHouseAnnotationNames.StripeLog
            or ClickHouseAnnotationNames.Log
            or ClickHouseAnnotationNames.Memory;
}
