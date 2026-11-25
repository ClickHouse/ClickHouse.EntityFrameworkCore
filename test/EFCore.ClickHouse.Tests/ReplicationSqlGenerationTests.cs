using ClickHouse.EntityFrameworkCore.Extensions;
using ClickHouse.EntityFrameworkCore.Infrastructure;
using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using ClickHouse.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Xunit;

namespace EFCore.ClickHouse.Tests;

/// <summary>
/// SQL shape of Replicated engines and <c>ON CLUSTER</c> DDL. <see cref="ReplicationIntegrationTests"/>
/// runs this DDL on a cluster with two replicas.
/// </summary>
public class ReplicationSqlGenerationTests
{
    private const string ExplicitPath = "/clickhouse/tables/{shard}/{database}/{table}";
    private const string ExplicitReplica = "{replica}";

    private const string NoDeduplication =
        "SETTINGS replicated_deduplication_window = 0, replicated_deduplication_window_for_async_inserts = 0";

    // Engine and the engine arguments that follow the replication arguments.
    public static TheoryData<string, string> Engines => new()
    {
        { "MergeTree", "" },
        { "ReplacingMergeTree", "`Version`, `IsDeleted`" },
        { "SummingMergeTree", "(`Amount`, `Count`)" },
        { "AggregatingMergeTree", "" },
        { "CollapsingMergeTree", "`Sign`" },
        { "VersionedCollapsingMergeTree", "`Sign`, `Version`" },
        { "GraphiteMergeTree", "'graphite_rollup'" },
    };

    [Theory]
    [MemberData(nameof(Engines))]
    public void WithReplication_uses_server_default_path_and_replica(string engine, string engineArgs)
    {
        var sql = GenerateCreateTables(t => ConfigureReplicatedEngine(t, engine, explicitPath: false));

        Assert.Contains($"ENGINE = Replicated{engine}({engineArgs})", sql);
        Assert.Contains("ORDER BY (`Id`)", sql);
    }

    [Theory]
    [MemberData(nameof(Engines))]
    public void WithReplication_with_path_and_replica_puts_them_first(string engine, string engineArgs)
    {
        var sql = GenerateCreateTables(t => ConfigureReplicatedEngine(t, engine, explicitPath: true));

        var args = engineArgs.Length == 0 ? "" : ", " + engineArgs;
        Assert.Contains($"ENGINE = Replicated{engine}('{ExplicitPath}', '{ExplicitReplica}'{args})", sql);
    }

    [Fact]
    public void WithReplication_escapes_path_and_replica()
    {
        var sql = GenerateCreateTables(t => t.HasMergeTreeEngine().WithReplication(@"/tables/it's", @"r\1"));

        Assert.Contains(@"ENGINE = ReplicatedMergeTree('/tables/it\'s', 'r\\1')", sql);
    }

    [Fact]
    public void Table_without_replication_keeps_plain_engine()
    {
        var sql = GenerateCreateTables(t => t.HasReplacingMergeTreeEngine("Version"));

        Assert.Contains("ENGINE = ReplacingMergeTree(`Version`)", sql);
        Assert.DoesNotContain("Replicated", sql);
    }

    [Fact]
    public void WithReplication_without_arguments_clears_an_earlier_path_and_replica()
    {
        using var ctx = new TableContext(t => t
            .HasMergeTreeEngine()
            .WithReplication(ExplicitPath, ExplicitReplica)
            .WithReplication());
        var entityType = ctx.Model.FindEntityType(typeof(Row))!;

        Assert.True(entityType.IsReplicated());
        Assert.Null(entityType.GetReplicatedZooKeeperPath());
        Assert.Null(entityType.GetReplicatedReplicaName());
    }

    [Theory]
    [InlineData(null, "{replica}")]
    [InlineData("/path", null)]
    [InlineData(" ", "{replica}")]
    [InlineData("/path", "")]
    public void WithReplication_requires_both_path_and_replica(string? zooKeeperPath, string? replicaName)
    {
        using var ctx = new TableContext(t => t.HasMergeTreeEngine().WithReplication(zooKeeperPath!, replicaName!));

        Assert.ThrowsAny<ArgumentException>(() => ctx.Model);
    }

    [Fact]
    public void Model_validation_rejects_replication_on_an_engine_without_a_replicated_variant()
    {
        using var ctx = new TableContext(t =>
        {
            t.HasMemoryEngine();
            ((IMutableEntityType)t.Metadata).SetReplicated(true);
        });

        var ex = Assert.Throws<InvalidOperationException>(() => ctx.Model);
        Assert.Contains("'Memory' engine has no Replicated variant", ex.Message);
    }

    [Fact]
    public void Model_validation_rejects_a_path_without_a_replica_name()
    {
        using var ctx = new TableContext(t =>
        {
            t.HasMergeTreeEngine();
            var entityType = (IMutableEntityType)t.Metadata;
            entityType.SetReplicated(true);
            entityType.SetReplicatedZooKeeperPath(ExplicitPath);
        });

        var ex = Assert.Throws<InvalidOperationException>(() => ctx.Model);
        Assert.Contains("only one of the ZooKeeper path and the replica name", ex.Message);
    }

    [Fact]
    public void CreateTable_rejects_replication_on_an_engine_without_a_replicated_variant()
    {
        var operation = new CreateTableOperation { Name = "t" };
        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseAnnotationNames.Memory);
        operation.AddAnnotation(ClickHouseAnnotationNames.Replicated, true);
        operation.Columns.Add(new AddColumnOperation { Name = "Id", Table = "t", ColumnType = "Int64", ClrType = typeof(long) });

        var ex = Assert.Throws<NotSupportedException>(() => Generate(null, operation));
        Assert.Contains("'Memory' engine has no Replicated variant", ex.Message);
    }

    [Fact]
    public void AlterTable_rejects_a_change_of_replication()
    {
        var operation = new AlterTableOperation { Name = "t" };
        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseAnnotationNames.MergeTree);
        operation.AddAnnotation(ClickHouseAnnotationNames.Replicated, true);
        operation.OldTable.AddAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseAnnotationNames.MergeTree);

        var ex = Assert.Throws<NotSupportedException>(() => Generate(null, operation));
        Assert.Contains("'Replicated'", ex.Message);
    }

    // ── ON CLUSTER ──────────────────────────────────────────────────────────

    [Fact]
    public void UseCluster_adds_ON_CLUSTER_to_every_DDL_statement()
    {
        var statements = GenerateStatements("test_cluster", DdlOperations());

        Assert.Equal(
        [
            "CREATE TABLE `t` ON CLUSTER 'test_cluster' (",
            "DROP TABLE `t` ON CLUSTER 'test_cluster'",
            "RENAME TABLE `t` TO `t2` ON CLUSTER 'test_cluster'",
            "ALTER TABLE `t` ON CLUSTER 'test_cluster' ADD COLUMN `c` Int32",
            "ALTER TABLE `t` ON CLUSTER 'test_cluster' DROP COLUMN `c`",
            "ALTER TABLE `t` ON CLUSTER 'test_cluster' MODIFY COLUMN `c` Int64",
            "ALTER TABLE `t` ON CLUSTER 'test_cluster' MODIFY COLUMN `c` REMOVE CODEC",
            "ALTER TABLE `t` ON CLUSTER 'test_cluster' RENAME COLUMN `c` TO `d`",
            "ALTER TABLE `t` ON CLUSTER 'test_cluster' ADD INDEX `ix` (`c`) TYPE minmax GRANULARITY 1",
            "ALTER TABLE `t` ON CLUSTER 'test_cluster' DROP INDEX `ix`",
            "ALTER TABLE `t` ON CLUSTER 'test_cluster' ADD CONSTRAINT `ck` CHECK (`Id` > 0)",
            "ALTER TABLE `t` ON CLUSTER 'test_cluster' DROP CONSTRAINT `ck`",
            "CREATE DATABASE `d` ON CLUSTER 'test_cluster'",
            "DROP DATABASE `d` ON CLUSTER 'test_cluster'",
        ],
        statements.Select(s => s.Split('\n')[0].TrimEnd(';', '\r', ' ')));
    }

    [Fact]
    public void Without_UseCluster_DDL_has_no_ON_CLUSTER()
    {
        var statements = GenerateStatements(null, DdlOperations());

        Assert.Equal(14, statements.Count);
        Assert.All(statements, s => Assert.DoesNotContain("ON CLUSTER", s));
    }

    [Fact]
    public void UseCluster_escapes_the_cluster_name()
    {
        var sql = Generate(@"it's\x", new DropTableOperation { Name = "t" });

        Assert.Contains(@"DROP TABLE `t` ON CLUSTER 'it\'s\\x'", sql);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("  ")]
    public void UseCluster_requires_a_cluster_name(string? clusterName)
        => Assert.ThrowsAny<ArgumentException>(() =>
            new DbContextOptionsBuilder().UseClickHouse("Host=localhost", o => o.UseCluster(clusterName!)));

    // ── Migrations history table ────────────────────────────────────────────

    [Fact]
    public void History_table_is_replicated_and_created_on_the_cluster_with_UseCluster()
    {
        var repository = CreateHistoryRepository(o => o.UseCluster("test_cluster"));

        var script = repository.GetCreateIfNotExistsScript();

        Assert.StartsWith("CREATE TABLE IF NOT EXISTS `__EFMigrationsHistory` ON CLUSTER 'test_cluster' (", script);
        // One history table for all shards: the path has no {shard}.
        Assert.Contains("ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}', '{shard}-{replica}')", script);
        Assert.Contains("ORDER BY (`MigrationId`)", script);
        Assert.Contains(NoDeduplication, script);
    }

    [Fact]
    public void History_table_is_replicated_without_ON_CLUSTER_with_UseReplicatedMigrationsHistoryTable()
    {
        var repository = CreateHistoryRepository(o => o.UseReplicatedMigrationsHistoryTable());

        var script = repository.GetCreateScript();

        Assert.Contains("ENGINE = ReplicatedMergeTree()", script);
        Assert.Contains(NoDeduplication, script);
        Assert.DoesNotContain("ON CLUSTER", script);
    }

    [Fact]
    public void History_table_is_not_replicated_by_default()
    {
        var repository = CreateHistoryRepository(_ => { });

        var script = repository.GetCreateScript();

        Assert.Contains("ENGINE = MergeTree()", script);
        Assert.DoesNotContain("Replicated", script);
        Assert.DoesNotContain("ON CLUSTER", script);
        Assert.DoesNotContain("SETTINGS", script);
    }

    [Fact]
    public void History_table_engine_can_be_changed_in_a_derived_repository()
    {
        var options = new DbContextOptionsBuilder()
            .UseClickHouse("Host=localhost;Database=test", o => o.UseCluster("test_cluster"))
            .ReplaceService<IHistoryRepository, ExplicitPathHistoryRepository>()
            .Options;
        using var context = new DbContext(options);

        var script = context.GetService<IHistoryRepository>().GetCreateScript();

        Assert.Contains("ON CLUSTER 'test_cluster'", script);
        Assert.Contains("ENGINE = ReplicatedMergeTree('/clickhouse/{cluster}/tables/{uuid}', '{replica}')", script);
    }

    private sealed class ExplicitPathHistoryRepository(HistoryRepositoryDependencies dependencies)
        : global::ClickHouse.EntityFrameworkCore.Migrations.Internal.ClickHouseHistoryRepository(dependencies)
    {
        protected override void ConfigureTable(
            Microsoft.EntityFrameworkCore.Metadata.Builders.EntityTypeBuilder<HistoryRow> history)
        {
            base.ConfigureTable(history);
            history.ToTable(TableName, t => t
                .HasMergeTreeEngine()
                .WithOrderBy("MigrationId")
                .WithReplication("/clickhouse/{cluster}/tables/{uuid}", "{replica}"));
        }
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private static MigrationOperation[] DdlOperations()
    {
        var createTable = new CreateTableOperation { Name = "t" };
        createTable.Columns.Add(new AddColumnOperation { Name = "Id", Table = "t", ColumnType = "Int64", ClrType = typeof(long) });

        var alterColumn = new AlterColumnOperation
        {
            Table = "t", Name = "c", ColumnType = "Int64", ClrType = typeof(long),
            OldColumn = new AddColumnOperation { Table = "t", Name = "c", ColumnType = "Int32", ClrType = typeof(int) },
        };
        alterColumn.OldColumn.AddAnnotation(ClickHouseAnnotationNames.ColumnCodec, "ZSTD");

        var createIndex = new CreateIndexOperation { Name = "ix", Table = "t", Columns = ["c"] };
        createIndex.AddAnnotation(ClickHouseAnnotationNames.SkippingIndexType, "minmax");
        var dropIndex = new DropIndexOperation { Name = "ix", Table = "t" };
        dropIndex.AddAnnotation(ClickHouseAnnotationNames.SkippingIndexType, "minmax");

        return
        [
            createTable,
            new DropTableOperation { Name = "t" },
            new RenameTableOperation { Name = "t", NewName = "t2" },
            new AddColumnOperation { Table = "t", Name = "c", ColumnType = "Int32", ClrType = typeof(int) },
            new DropColumnOperation { Table = "t", Name = "c" },
            alterColumn,
            new RenameColumnOperation { Table = "t", Name = "c", NewName = "d" },
            createIndex,
            dropIndex,
            new AddCheckConstraintOperation { Table = "t", Name = "ck", Sql = "`Id` > 0" },
            new DropCheckConstraintOperation { Table = "t", Name = "ck" },
            new ClickHouseCreateDatabaseOperation { Name = "d" },
            new ClickHouseDropDatabaseOperation { Name = "d" },
        ];
    }

    private static void ConfigureReplicatedEngine(TableBuilder<Row> t, string engine, bool explicitPath)
    {
        switch (engine)
        {
            case "MergeTree":
                var mergeTree = t.HasMergeTreeEngine().WithOrderBy("Id");
                if (explicitPath) mergeTree.WithReplication(ExplicitPath, ExplicitReplica); else mergeTree.WithReplication();
                break;
            case "ReplacingMergeTree":
                var replacing = t.HasReplacingMergeTreeEngine("Version", "IsDeleted").WithOrderBy("Id");
                if (explicitPath) replacing.WithReplication(ExplicitPath, ExplicitReplica); else replacing.WithReplication();
                break;
            case "SummingMergeTree":
                var summing = t.HasSummingMergeTreeEngine("Amount", "Count").WithOrderBy("Id");
                if (explicitPath) summing.WithReplication(ExplicitPath, ExplicitReplica); else summing.WithReplication();
                break;
            case "AggregatingMergeTree":
                var aggregating = t.HasAggregatingMergeTreeEngine().WithOrderBy("Id");
                if (explicitPath) aggregating.WithReplication(ExplicitPath, ExplicitReplica); else aggregating.WithReplication();
                break;
            case "CollapsingMergeTree":
                var collapsing = t.HasCollapsingMergeTreeEngine("Sign").WithOrderBy("Id");
                if (explicitPath) collapsing.WithReplication(ExplicitPath, ExplicitReplica); else collapsing.WithReplication();
                break;
            case "VersionedCollapsingMergeTree":
                var versioned = t.HasVersionedCollapsingMergeTreeEngine("Sign", "Version").WithOrderBy("Id");
                if (explicitPath) versioned.WithReplication(ExplicitPath, ExplicitReplica); else versioned.WithReplication();
                break;
            case "GraphiteMergeTree":
                var graphite = t.HasGraphiteMergeTreeEngine("graphite_rollup").WithOrderBy("Id");
                if (explicitPath) graphite.WithReplication(ExplicitPath, ExplicitReplica); else graphite.WithReplication();
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(engine), engine, null);
        }
    }

    private static string GenerateCreateTables(Action<TableBuilder<Row>> configureTable)
    {
        using var ctx = new TableContext(configureTable);
        var model = ctx.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var operations = ctx.GetService<IMigrationsModelDiffer>().GetDifferences(source: null, target: model);
        return string.Join("\n", ctx.GetService<IMigrationsSqlGenerator>().Generate(operations).Select(c => c.CommandText));
    }

    private static string Generate(string? clusterName, params MigrationOperation[] operations)
        => string.Join("\n", GenerateStatements(clusterName, operations));

    private static List<string> GenerateStatements(string? clusterName, MigrationOperation[] operations)
    {
        var options = new DbContextOptionsBuilder()
            .UseClickHouse("Host=localhost;Database=test", o =>
            {
                if (clusterName is not null)
                    o.UseCluster(clusterName);
            })
            .Options;

        using var context = new DbContext(options);
        return context.GetService<IMigrationsSqlGenerator>().Generate(operations).Select(c => c.CommandText).ToList();
    }

    private static IHistoryRepository CreateHistoryRepository(Action<ClickHouseDbContextOptionsBuilder> configure)
    {
        var options = new DbContextOptionsBuilder()
            .UseClickHouse("Host=localhost;Database=test", configure)
            .Options;

        using var context = new DbContext(options);
        return context.GetService<IHistoryRepository>();
    }

    public class Row
    {
        public long Id { get; set; }
        public ulong Version { get; set; }
        public byte IsDeleted { get; set; }
        public long Amount { get; set; }
        public long Count { get; set; }
        public sbyte Sign { get; set; }
    }

    // Each instance builds its own model, so tests with different table configurations do not share a cached model.
    private sealed class TableContext(Action<TableBuilder<Row>> configureTable) : DbContext
    {
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder
                .UseClickHouse("Host=localhost;Database=test")
                .ReplaceService<IModelCacheKeyFactory, NoModelCacheKeyFactory>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<Row>(e => e.ToTable("t", configureTable));
    }

    private sealed class NoModelCacheKeyFactory : IModelCacheKeyFactory
    {
        public object Create(DbContext context, bool designTime) => new object();
    }
}
