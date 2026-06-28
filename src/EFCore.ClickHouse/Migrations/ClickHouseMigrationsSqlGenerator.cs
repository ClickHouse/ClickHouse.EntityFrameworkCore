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
    protected override void EndStatement(MigrationCommandListBuilder builder, bool suppressTransaction = true)
        => base.EndStatement(builder, suppressTransaction: true);

    // Appends the statement terminator and finalizes the command. The base EndStatement only ends
    // the command; the terminator must be appended by the caller (the built-in operations do this
    // too). Every custom override below routes through this so each emitted statement is terminated
    // — without it, the concatenated `migrations script` output runs statements together.
    private void TerminateStatement(MigrationCommandListBuilder builder)
    {
        builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
        EndStatement(builder);
    }

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
            case ClickHouseCreateMaterializedViewOperation createMv:
                Generate(createMv, builder);
                return;
            case ClickHouseDropMaterializedViewOperation dropMv:
                Generate(dropMv, builder);
                return;
            case ClickHouseCreateDictionaryOperation createDict:
                Generate(createDict, builder);
                return;
            case ClickHouseDropDictionaryOperation dropDict:
                Generate(dropDict, builder);
                return;
            case ClickHouseAddProjectionOperation addProjection:
                Generate(addProjection, builder);
                return;
            case ClickHouseDropProjectionOperation dropProjection:
                Generate(dropProjection, builder);
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
        TerminateStatement(builder);
    }

    protected virtual void Generate(ClickHouseDropDatabaseOperation operation, MigrationCommandListBuilder builder)
    {
        builder
            .Append("DROP DATABASE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name));
        TerminateStatement(builder);
    }

    protected virtual void Generate(ClickHouseCreateMaterializedViewOperation operation, MigrationCommandListBuilder builder)
    {
        // ClickHouse forbids combining an explicit target table with POPULATE
        // ("you can't declare both 'TO [db].[table]' and 'POPULATE'"). This provider's views always
        // write to a target table, so POPULATE is unsupported; backfill the target with a follow-up
        // INSERT … SELECT instead.
        if (operation.Populate)
            throw new NotSupportedException(
                $"Materialized view '{operation.ViewName}' sets Populate, but ClickHouse does not allow "
                + "POPULATE together with a 'TO' target table. Remove Populate and backfill the target "
                + "table with an INSERT … SELECT after the view is created.");

        builder.Append("CREATE MATERIALIZED VIEW ");

        if (operation.IfNotExists)
            builder.Append("IF NOT EXISTS ");

        AppendQualifiedName(builder, operation.Database, operation.ViewName);

        if (!string.IsNullOrWhiteSpace(operation.Cluster))
            builder.Append($" ON CLUSTER '{operation.Cluster}'");

        builder.Append(" TO ");
        AppendQualifiedName(builder, operation.TargetDatabase, operation.TargetTable);

        builder.AppendLine();
        builder.Append("AS ").Append(operation.SelectQuery);

        TerminateStatement(builder);
    }

    protected virtual void Generate(ClickHouseDropMaterializedViewOperation operation, MigrationCommandListBuilder builder)
    {
        builder.Append("DROP VIEW ");

        if (operation.IfExists)
            builder.Append("IF EXISTS ");

        AppendQualifiedName(builder, operation.Database, operation.ViewName);

        if (!string.IsNullOrWhiteSpace(operation.Cluster))
            builder.Append($" ON CLUSTER '{operation.Cluster}'");

        TerminateStatement(builder);
    }

    protected virtual void Generate(ClickHouseCreateDictionaryOperation operation, MigrationCommandListBuilder builder)
    {
        var helper = Dependencies.SqlGenerationHelper;

        builder.Append(operation.OrReplace ? "CREATE OR REPLACE DICTIONARY " : "CREATE DICTIONARY ");

        if (operation.IfNotExists && !operation.OrReplace)
            builder.Append("IF NOT EXISTS ");

        AppendQualifiedName(builder, operation.Database, operation.DictionaryName);

        if (!string.IsNullOrWhiteSpace(operation.Cluster))
            builder.Append($" ON CLUSTER '{operation.Cluster}'");

        // Column list
        builder.AppendLine();
        builder.AppendLine("(");
        using (builder.Indent())
        {
            for (var i = 0; i < operation.Columns.Count; i++)
            {
                var col = operation.Columns[i];
                builder.Append(helper.DelimitIdentifier(col.Name)).Append(" ").Append(col.Type);
                if (!string.IsNullOrWhiteSpace(col.Default))
                    builder.Append(" DEFAULT ").Append(col.Default);
                if (i < operation.Columns.Count - 1)
                    builder.Append(",");
                builder.AppendLine();
            }
        }
        builder.AppendLine(")");

        // PRIMARY KEY
        builder.Append("PRIMARY KEY ");
        builder.AppendLine(string.Join(", ", operation.KeyColumns.Select(helper.DelimitIdentifier)));

        // SOURCE — a ClickHouse table. Connection settings are optional; when omitted the dictionary
        // loads as the 'default' user with an empty password (only works where 'default' is passwordless).
        builder.Append("SOURCE(CLICKHOUSE(");
        // A named collection (defined in server config) supplies host/port/user/password; the inline
        // settings below override its fields.
        if (!string.IsNullOrWhiteSpace(operation.SourceNamedCollection))
            builder.Append("NAME ").Append(SqlStringLiteral(operation.SourceNamedCollection)).Append(" ");
        if (!string.IsNullOrWhiteSpace(operation.SourceHost))
            builder.Append("HOST ").Append(SqlStringLiteral(operation.SourceHost)).Append(" ");
        if (operation.SourcePort is { } port)
            builder.Append("PORT ").Append(port.ToString()).Append(" ");
        if (!string.IsNullOrWhiteSpace(operation.SourceUser))
            builder.Append("USER ").Append(SqlStringLiteral(operation.SourceUser)).Append(" ");
        // Intentionally uses != null (not IsNullOrWhiteSpace): an explicitly-configured empty password
        // (PASSWORD '') is distinct from "no password configured".
        if (operation.SourcePassword is not null)
            builder.Append("PASSWORD ").Append(SqlStringLiteral(operation.SourcePassword)).Append(" ");
        builder.Append("TABLE ").Append(SqlStringLiteral(operation.SourceTable));
        if (!string.IsNullOrWhiteSpace(operation.SourceDatabase))
            builder.Append(" DB ").Append(SqlStringLiteral(operation.SourceDatabase));
        builder.AppendLine("))");

        // LAYOUT
        builder.Append("LAYOUT(").Append(operation.Layout).Append("(");
        if (!string.IsNullOrWhiteSpace(operation.LayoutParams))
            builder.Append(operation.LayoutParams);
        builder.AppendLine("))");

        // LIFETIME
        if (operation.LifetimeMin is { } min && operation.LifetimeMax is { } max)
            builder.Append($"LIFETIME(MIN {min} MAX {max})");
        else if (operation.LifetimeMax is { } maxOnly)
            builder.Append($"LIFETIME({maxOnly})");

        TerminateStatement(builder);
    }

    protected virtual void Generate(ClickHouseDropDictionaryOperation operation, MigrationCommandListBuilder builder)
    {
        builder.Append("DROP DICTIONARY ");

        if (operation.IfExists)
            builder.Append("IF EXISTS ");

        AppendQualifiedName(builder, operation.Database, operation.DictionaryName);

        if (!string.IsNullOrWhiteSpace(operation.Cluster))
            builder.Append($" ON CLUSTER '{operation.Cluster}'");

        TerminateStatement(builder);
    }

    // Projections — ALTER TABLE … ADD / MATERIALIZE / DROP PROJECTION (MergeTree family only).
    // ADD with Materialize emits two terminated statements from one operation (the ADD then a
    // MATERIALIZE so existing parts are covered), mirroring the AlterColumn REMOVE emission.

    protected virtual void Generate(ClickHouseAddProjectionOperation operation, MigrationCommandListBuilder builder)
    {
        AppendAlterTablePrefix(builder, operation.Table, operation.Schema, operation.Cluster);
        builder.Append(" ADD PROJECTION ");
        if (operation.IfNotExists)
            builder.Append("IF NOT EXISTS ");
        builder
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.ProjectionName))
            .Append(" (")
            .Append(operation.SelectQuery)
            .Append(")");
        TerminateStatement(builder);

        if (operation.Materialize)
        {
            AppendAlterTablePrefix(builder, operation.Table, operation.Schema, operation.Cluster);
            builder
                .Append(" MATERIALIZE PROJECTION ")
                .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.ProjectionName));
            TerminateStatement(builder);
        }
    }

    protected virtual void Generate(ClickHouseDropProjectionOperation operation, MigrationCommandListBuilder builder)
    {
        AppendAlterTablePrefix(builder, operation.Table, operation.Schema, operation.Cluster);
        builder.Append(" DROP PROJECTION ");
        if (operation.IfExists)
            builder.Append("IF EXISTS ");
        builder.Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.ProjectionName));
        TerminateStatement(builder);
    }

    // "ALTER TABLE [db.]t [ON CLUSTER 'c']" — shared prefix for the projection ALTER statements.
    // Uses AppendQualifiedName (not DelimitIdentifier(name, schema)) because ClickHouse's SQL helper
    // drops the schema argument — the database qualifier must be emitted explicitly, as the MV/dict
    // generators do.
    private void AppendAlterTablePrefix(MigrationCommandListBuilder builder, string table, string? schema, string? cluster)
    {
        builder.Append("ALTER TABLE ");
        AppendQualifiedName(builder, schema, table);
        if (!string.IsNullOrWhiteSpace(cluster))
            builder.Append($" ON CLUSTER '{cluster}'");
    }

    // A single-quoted ClickHouse string literal (used for dictionary SOURCE parameters).
    private static string SqlStringLiteral(string value)
        => "'" + value.Replace("\\", "\\\\").Replace("'", "\\'") + "'";

    private void AppendQualifiedName(MigrationCommandListBuilder builder, string? database, string name)
    {
        if (!string.IsNullOrWhiteSpace(database))
        {
            builder.Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(database));
            builder.Append(".");
        }

        builder.Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(name));
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

        TerminateStatement(builder);
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
        builder
            .Append("ALTER TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table, operation.Schema))
            .Append(" ADD COLUMN ");

        ColumnDefinition(operation, model, builder);
        TerminateStatement(builder);
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

        TerminateStatement(builder);
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
        TerminateStatement(builder);

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
            TerminateStatement(builder);
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

        TerminateStatement(builder);
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

        TerminateStatement(builder);
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
            // Projection annotations live on the entity type, so adding/removing a projection surfaces
            // here as a table-annotation delta. They are realized as separate ADD/DROP PROJECTION
            // operations by the ClickHouse differ, so they are NOT an unsupported table-metadata change.
            if (key.StartsWith(ClickHouseAnnotationNames.ProjectionPrefix, StringComparison.Ordinal))
                continue;

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
        TerminateStatement(builder);
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

        TerminateStatement(builder);
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
