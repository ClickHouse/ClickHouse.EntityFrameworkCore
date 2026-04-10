using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using ClickHouse.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Xunit;

namespace EFCore.ClickHouse.Tests;

public class MigrationSqlGeneratorTests
{
    [Fact]
    public void CreateTable_MergeTree_with_OrderBy()
    {
        var sql = GenerateCreateTable(op =>
        {
            op.AddAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseAnnotationNames.MergeTree);
            op.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id", "Name" });
            op.Columns.Add(new AddColumnOperation { Name = "Id", ColumnType = "Int64", ClrType = typeof(long) });
            op.Columns.Add(new AddColumnOperation { Name = "Name", ColumnType = "String", ClrType = typeof(string) });
        });

        Assert.Contains("ENGINE = MergeTree()", sql);
        Assert.Contains("ORDER BY (`Id`, `Name`)", sql);
    }

    [Fact]
    public void CreateTable_ReplacingMergeTree_with_version()
    {
        var sql = GenerateCreateTable(op =>
        {
            op.AddAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseAnnotationNames.ReplacingMergeTree);
            op.AddAnnotation(ClickHouseAnnotationNames.ReplacingMergeTreeVersion, "Version");
            op.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });
            op.Columns.Add(new AddColumnOperation { Name = "Id", ColumnType = "Int64", ClrType = typeof(long) });
            op.Columns.Add(new AddColumnOperation { Name = "Version", ColumnType = "UInt64", ClrType = typeof(ulong) });
        });

        Assert.Contains("ENGINE = ReplacingMergeTree(`Version`)", sql);
    }

    [Fact]
    public void CreateTable_CollapsingMergeTree_with_sign()
    {
        var sql = GenerateCreateTable(op =>
        {
            op.AddAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseAnnotationNames.CollapsingMergeTree);
            op.AddAnnotation(ClickHouseAnnotationNames.CollapsingMergeTreeSign, "Sign");
            op.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });
            op.Columns.Add(new AddColumnOperation { Name = "Id", ColumnType = "Int64", ClrType = typeof(long) });
            op.Columns.Add(new AddColumnOperation { Name = "Sign", ColumnType = "Int8", ClrType = typeof(sbyte) });
        });

        Assert.Contains("ENGINE = CollapsingMergeTree(`Sign`)", sql);
    }

    [Fact]
    public void CreateTable_StripeLog_no_OrderBy()
    {
        var sql = GenerateCreateTable(op =>
        {
            op.AddAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseAnnotationNames.StripeLog);
            op.Columns.Add(new AddColumnOperation { Name = "Id", ColumnType = "Int64", ClrType = typeof(long) });
        });

        Assert.Contains("ENGINE = StripeLog", sql);
        Assert.DoesNotContain("StripeLog()", sql);
        Assert.DoesNotContain("ORDER BY", sql);
    }

    [Fact]
    public void CreateTable_Memory_no_parentheses()
    {
        var sql = GenerateCreateTable(op =>
        {
            op.AddAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseAnnotationNames.Memory);
            op.Columns.Add(new AddColumnOperation { Name = "Id", ColumnType = "Int64", ClrType = typeof(long) });
        });

        Assert.Contains("ENGINE = Memory", sql);
        Assert.DoesNotContain("Memory()", sql);
        Assert.DoesNotContain("ORDER BY", sql);
    }

    [Fact]
    public void CreateTable_nullable_column_wraps_in_Nullable()
    {
        var sql = GenerateCreateTable(op =>
        {
            op.AddAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseAnnotationNames.MergeTree);
            op.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });
            op.Columns.Add(new AddColumnOperation { Name = "Id", ColumnType = "Int64", ClrType = typeof(long) });
            op.Columns.Add(new AddColumnOperation
            {
                Name = "Value", ColumnType = "String", ClrType = typeof(string), IsNullable = true
            });
        });

        Assert.Contains("`Value` Nullable(String)", sql);
        Assert.DoesNotContain("NOT NULL", sql);
    }

    [Fact]
    public void CreateTable_column_with_codec()
    {
        var sql = GenerateCreateTable(op =>
        {
            op.AddAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseAnnotationNames.MergeTree);
            op.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });
            op.Columns.Add(new AddColumnOperation { Name = "Id", ColumnType = "Int64", ClrType = typeof(long) });
            var col = new AddColumnOperation { Name = "Temp", ColumnType = "Int16", ClrType = typeof(short) };
            col.AddAnnotation(ClickHouseAnnotationNames.ColumnCodec, "Delta, ZSTD");
            op.Columns.Add(col);
        });

        Assert.Contains("`Temp` Int16 CODEC(Delta, ZSTD)", sql);
    }

    [Fact]
    public void CreateTable_column_with_ttl()
    {
        var sql = GenerateCreateTable(op =>
        {
            op.AddAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseAnnotationNames.MergeTree);
            op.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });
            op.Columns.Add(new AddColumnOperation { Name = "Id", ColumnType = "Int64", ClrType = typeof(long) });
            var col = new AddColumnOperation { Name = "Created", ColumnType = "DateTime", ClrType = typeof(DateTime) };
            col.AddAnnotation(ClickHouseAnnotationNames.ColumnTtl, "Created + INTERVAL 1 MONTH");
            op.Columns.Add(col);
        });

        Assert.Contains("TTL Created + INTERVAL 1 MONTH", sql);
    }

    [Fact]
    public void CreateTable_column_with_comment()
    {
        var sql = GenerateCreateTable(op =>
        {
            op.AddAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseAnnotationNames.MergeTree);
            op.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });
            op.Columns.Add(new AddColumnOperation { Name = "Id", ColumnType = "Int64", ClrType = typeof(long) });
            var col = new AddColumnOperation { Name = "Name", ColumnType = "String", ClrType = typeof(string) };
            col.AddAnnotation(ClickHouseAnnotationNames.ColumnComment, "User name");
            op.Columns.Add(col);
        });

        Assert.Contains("COMMENT 'User name'", sql);
    }

    [Fact]
    public void CreateTable_with_partitionBy_expression()
    {
        var sql = GenerateCreateTable(op =>
        {
            op.AddAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseAnnotationNames.MergeTree);
            op.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });
            op.AddAnnotation(ClickHouseAnnotationNames.PartitionBy, new[] { "toYYYYMM(CreatedAt)" });
            op.Columns.Add(new AddColumnOperation { Name = "Id", ColumnType = "Int64", ClrType = typeof(long) });
        });

        Assert.Contains("PARTITION BY toYYYYMM(CreatedAt)", sql);
    }

    [Fact]
    public void CreateTable_with_settings()
    {
        var sql = GenerateCreateTable(op =>
        {
            op.AddAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseAnnotationNames.MergeTree);
            op.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });
            op.AddAnnotation(ClickHouseAnnotationNames.SettingPrefix + "index_granularity", "4096");
            op.Columns.Add(new AddColumnOperation { Name = "Id", ColumnType = "Int64", ClrType = typeof(long) });
        });

        Assert.Contains("SETTINGS index_granularity = 4096", sql);
    }

    [Fact]
    public void CreateTable_with_table_TTL()
    {
        var sql = GenerateCreateTable(op =>
        {
            op.AddAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseAnnotationNames.MergeTree);
            op.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });
            op.AddAnnotation(ClickHouseAnnotationNames.Ttl, "Created + INTERVAL 30 DAY");
            op.Columns.Add(new AddColumnOperation { Name = "Id", ColumnType = "Int64", ClrType = typeof(long) });
        });

        Assert.Contains("TTL Created + INTERVAL 30 DAY", sql);
    }

    [Fact]
    public void CreateTable_MergeTree_no_explicit_OrderBy_falls_back_to_tuple()
    {
        var sql = GenerateCreateTable(op =>
        {
            op.AddAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseAnnotationNames.MergeTree);
            op.Columns.Add(new AddColumnOperation { Name = "Id", ColumnType = "Int64", ClrType = typeof(long) });
        });

        Assert.Contains("ORDER BY tuple()", sql);
    }

    [Fact]
    public void AddForeignKey_throws_NotSupportedException()
    {
        Assert.Throws<NotSupportedException>(() =>
        {
            Generate(new AddForeignKeyOperation
            {
                Table = "t",
                Name = "FK_Test",
                Columns = ["Id"],
                PrincipalTable = "other",
                PrincipalColumns = ["Id"]
            });
        });
    }

    [Fact]
    public void CreateSequence_throws_NotSupportedException()
    {
        Assert.Throws<NotSupportedException>(() =>
        {
            Generate(new CreateSequenceOperation { Name = "seq" });
        });
    }

    [Fact]
    public void EnsureSchema_throws_NotSupportedException()
    {
        Assert.Throws<NotSupportedException>(() =>
        {
            Generate(new EnsureSchemaOperation { Name = "dbo" });
        });
    }

    [Fact]
    public void RenameTable_generates_RENAME_TABLE()
    {
        var sql = Generate(new RenameTableOperation { Name = "old_table", NewName = "new_table" });
        Assert.Contains("RENAME TABLE `old_table` TO `new_table`", sql);
    }

    [Fact]
    public void RenameColumn_generates_ALTER_TABLE_RENAME_COLUMN()
    {
        var sql = Generate(new RenameColumnOperation { Table = "t", Name = "old_col", NewName = "new_col" });
        Assert.Contains("ALTER TABLE `t` RENAME COLUMN `old_col` TO `new_col`", sql);
    }

    [Fact]
    public void DropIndex_skipping_generates_ALTER_TABLE_DROP_INDEX()
    {
        var op = new DropIndexOperation { Table = "t", Name = "idx_name" };
        op.AddAnnotation(ClickHouseAnnotationNames.SkippingIndexType, "minmax");
        var sql = Generate(op);
        Assert.Contains("ALTER TABLE `t` DROP INDEX `idx_name`", sql);
    }

    // Finding 3: standard index create/drop symmetry

    [Fact]
    public void CreateIndex_standard_is_noop()
    {
        var op = new CreateIndexOperation
        {
            Name = "IX_Test", Table = "t", Columns = ["Col1"]
        };
        var sql = Generate(op);
        Assert.DoesNotContain("INDEX", sql);
    }

    [Fact]
    public void DropIndex_standard_is_noop()
    {
        var op = new DropIndexOperation { Table = "t", Name = "IX_Test" };
        // No skipping index annotation — should be no-op, symmetric with create
        var sql = Generate(op);
        Assert.DoesNotContain("INDEX", sql);
    }

    // Finding 1: AlterTableOperation rejects ClickHouse metadata changes

    [Fact]
    public void AlterTable_engine_change_throws()
    {
        var op = new AlterTableOperation { Name = "t" };
        op.AddAnnotation(ClickHouseAnnotationNames.Engine, "ReplacingMergeTree");
        op.OldTable.AddAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree");

        Assert.Throws<NotSupportedException>(() => Generate(op));
    }

    [Fact]
    public void AlterTable_orderBy_change_throws()
    {
        var op = new AlterTableOperation { Name = "t" };
        op.AddAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree");
        op.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id", "Name" });
        op.OldTable.AddAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree");
        op.OldTable.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });

        Assert.Throws<NotSupportedException>(() => Generate(op));
    }

    [Fact]
    public void AlterTable_partitionBy_change_throws()
    {
        var op = new AlterTableOperation { Name = "t" };
        op.AddAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree");
        op.AddAnnotation(ClickHouseAnnotationNames.PartitionBy, new[] { "toYYYYMM(ts)" });
        op.OldTable.AddAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree");

        Assert.Throws<NotSupportedException>(() => Generate(op));
    }

    [Fact]
    public void AlterTable_ttl_change_throws()
    {
        var op = new AlterTableOperation { Name = "t" };
        op.AddAnnotation(ClickHouseAnnotationNames.Ttl, "ts + INTERVAL 30 DAY");
        op.OldTable.AddAnnotation(ClickHouseAnnotationNames.Ttl, "ts + INTERVAL 7 DAY");

        Assert.Throws<NotSupportedException>(() => Generate(op));
    }

    [Fact]
    public void AlterTable_primaryKey_change_throws()
    {
        var op = new AlterTableOperation { Name = "t" };
        op.AddAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree");
        op.AddAnnotation(ClickHouseAnnotationNames.PrimaryKey, new[] { "Id", "Name" });
        op.OldTable.AddAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree");
        op.OldTable.AddAnnotation(ClickHouseAnnotationNames.PrimaryKey, new[] { "Id" });

        Assert.Throws<NotSupportedException>(() => Generate(op));
    }

    [Fact]
    public void AlterTable_sampleBy_change_throws()
    {
        var op = new AlterTableOperation { Name = "t" };
        op.AddAnnotation(ClickHouseAnnotationNames.SampleBy, new[] { "Id" });
        op.OldTable.AddAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree");

        Assert.Throws<NotSupportedException>(() => Generate(op));
    }

    [Fact]
    public void AlterTable_settings_change_throws()
    {
        var op = new AlterTableOperation { Name = "t" };
        op.AddAnnotation(ClickHouseAnnotationNames.SettingPrefix + "index_granularity", "4096");
        op.OldTable.AddAnnotation(ClickHouseAnnotationNames.SettingPrefix + "index_granularity", "8192");

        Assert.Throws<NotSupportedException>(() => Generate(op));
    }

    [Fact]
    public void AlterTable_engine_specific_arg_change_throws()
    {
        var op = new AlterTableOperation { Name = "t" };
        op.AddAnnotation(ClickHouseAnnotationNames.Engine, "ReplacingMergeTree");
        op.AddAnnotation(ClickHouseAnnotationNames.ReplacingMergeTreeVersion, "Ver2");
        op.OldTable.AddAnnotation(ClickHouseAnnotationNames.Engine, "ReplacingMergeTree");
        op.OldTable.AddAnnotation(ClickHouseAnnotationNames.ReplacingMergeTreeVersion, "Ver1");

        Assert.Throws<NotSupportedException>(() => Generate(op));
    }

    [Fact]
    public void AlterTable_no_clickhouse_changes_delegates_to_base()
    {
        // Non-ClickHouse metadata change (e.g., comment) should not throw
        var op = new AlterTableOperation { Name = "t", Comment = "new comment" };
        op.OldTable.Comment = "old comment";
        var sql = Generate(op);
        // Should not throw — base handles standard annotation changes
        Assert.NotNull(sql);
    }

    // Finding 2: idempotent scripts throw

    [Fact]
    public void GetBeginIfNotExistsScript_throws_NotSupportedException()
    {
        var repo = CreateHistoryRepository();
        Assert.Throws<NotSupportedException>(() => repo.GetBeginIfNotExistsScript("20260101000000_Init"));
    }

    [Fact]
    public void GetBeginIfExistsScript_throws_NotSupportedException()
    {
        var repo = CreateHistoryRepository();
        Assert.Throws<NotSupportedException>(() => repo.GetBeginIfExistsScript("20260101000000_Init"));
    }

    [Fact]
    public void GetEndIfScript_throws_NotSupportedException()
    {
        var repo = CreateHistoryRepository();
        Assert.Throws<NotSupportedException>(() => repo.GetEndIfScript());
    }

    [Fact]
    public void GetCreateIfNotExistsScript_contains_IF_NOT_EXISTS()
    {
        var repo = CreateHistoryRepository();
        var script = repo.GetCreateIfNotExistsScript();
        Assert.Contains("IF NOT EXISTS", script);
        Assert.Contains("CREATE TABLE", script);
    }

    // Corner cases from review section D

    [Fact]
    public void Column_comment_with_single_quote_is_escaped()
    {
        var sql = GenerateCreateTable(op =>
        {
            op.AddAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseAnnotationNames.MergeTree);
            op.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });
            op.Columns.Add(new AddColumnOperation { Name = "Id", ColumnType = "Int64", ClrType = typeof(long) });
            var col = new AddColumnOperation { Name = "Name", ColumnType = "String", ClrType = typeof(string) };
            col.AddAnnotation(ClickHouseAnnotationNames.ColumnComment, "it's a name");
            op.Columns.Add(col);
        });

        Assert.Contains(@"COMMENT 'it\'s a name'", sql);
    }

    [Fact]
    public void Column_with_codec_ttl_and_comment_together()
    {
        var sql = GenerateCreateTable(op =>
        {
            op.AddAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseAnnotationNames.MergeTree);
            op.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });
            op.Columns.Add(new AddColumnOperation { Name = "Id", ColumnType = "Int64", ClrType = typeof(long) });
            var col = new AddColumnOperation { Name = "Temp", ColumnType = "Int16", ClrType = typeof(short) };
            col.AddAnnotation(ClickHouseAnnotationNames.ColumnCodec, "Delta, ZSTD");
            col.AddAnnotation(ClickHouseAnnotationNames.ColumnTtl, "ts + INTERVAL 1 DAY");
            col.AddAnnotation(ClickHouseAnnotationNames.ColumnComment, "temperature");
            op.Columns.Add(col);
        });

        // Verify order per ClickHouse docs: COMMENT → CODEC → TTL
        var tempLine = sql.Split('\n').First(l => l.Contains("`Temp`"));
        var commentIdx = tempLine.IndexOf("COMMENT ", StringComparison.Ordinal);
        var codecIdx = tempLine.IndexOf("CODEC(", StringComparison.Ordinal);
        var ttlIdx = tempLine.IndexOf("TTL ", StringComparison.Ordinal);
        Assert.True(commentIdx < codecIdx, "COMMENT should come before CODEC");
        Assert.True(codecIdx < ttlIdx, "CODEC should come before TTL");
    }

    [Fact]
    public void Nullable_array_column_is_not_wrapped_in_Nullable()
    {
        var sql = GenerateCreateTable(op =>
        {
            op.AddAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseAnnotationNames.MergeTree);
            op.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });
            op.Columns.Add(new AddColumnOperation { Name = "Id", ColumnType = "Int64", ClrType = typeof(long) });
            op.Columns.Add(new AddColumnOperation
            {
                Name = "Tags", ColumnType = "Array(String)", ClrType = typeof(string[]), IsNullable = true
            });
        });

        Assert.Contains("`Tags` Array(String)", sql);
        Assert.DoesNotContain("Nullable(Array", sql);
    }

    [Fact]
    public void OrderBy_mixed_expressions_and_columns()
    {
        var sql = GenerateCreateTable(op =>
        {
            op.AddAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseAnnotationNames.MergeTree);
            op.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id", "toYYYYMM(ts)" });
            op.Columns.Add(new AddColumnOperation { Name = "Id", ColumnType = "Int64", ClrType = typeof(long) });
        });

        Assert.Contains("ORDER BY (`Id`, toYYYYMM(ts))", sql);
    }

    [Fact]
    public void VersionedCollapsingMergeTree_both_args()
    {
        var sql = GenerateCreateTable(op =>
        {
            op.AddAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseAnnotationNames.VersionedCollapsingMergeTree);
            op.AddAnnotation(ClickHouseAnnotationNames.VersionedCollapsingMergeTreeSign, "Sign");
            op.AddAnnotation(ClickHouseAnnotationNames.VersionedCollapsingMergeTreeVersion, "Ver");
            op.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });
            op.Columns.Add(new AddColumnOperation { Name = "Id", ColumnType = "Int64", ClrType = typeof(long) });
        });

        Assert.Contains("ENGINE = VersionedCollapsingMergeTree(`Sign`, `Ver`)", sql);
    }

    [Fact]
    public void SummingMergeTree_multiple_columns()
    {
        var sql = GenerateCreateTable(op =>
        {
            op.AddAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseAnnotationNames.SummingMergeTree);
            op.AddAnnotation(ClickHouseAnnotationNames.SummingMergeTreeColumns, new[] { "Amount", "Count" });
            op.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });
            op.Columns.Add(new AddColumnOperation { Name = "Id", ColumnType = "Int64", ClrType = typeof(long) });
        });

        Assert.Contains("ENGINE = SummingMergeTree(`Amount`, `Count`)", sql);
    }

    [Fact]
    public void CreateDatabase_generates_CREATE_DATABASE()
    {
        var sql = Generate(new ClickHouseCreateDatabaseOperation { Name = "my_db" });
        Assert.Contains("CREATE DATABASE `my_db`", sql);
    }

    [Fact]
    public void DropDatabase_generates_DROP_DATABASE()
    {
        var sql = Generate(new ClickHouseDropDatabaseOperation { Name = "my_db" });
        Assert.Contains("DROP DATABASE `my_db`", sql);
    }

    private string GenerateCreateTable(Action<CreateTableOperation> configure)
    {
        var operation = new CreateTableOperation { Name = "test_table" };
        configure(operation);
        return Generate(operation);
    }

    private string Generate(params MigrationOperation[] operations)
    {
        var optionsBuilder = new DbContextOptionsBuilder()
            .UseClickHouse("Host=localhost;Database=test");

        using var context = new DbContext(optionsBuilder.Options);
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var commands = generator.Generate(operations);
        return string.Join("\n", commands.Select(c => c.CommandText));
    }

    private static IHistoryRepository CreateHistoryRepository()
    {
        var optionsBuilder = new DbContextOptionsBuilder()
            .UseClickHouse("Host=localhost;Database=test");

        using var context = new DbContext(optionsBuilder.Options);
        return context.GetService<IHistoryRepository>();
    }
}
