using ClickHouse.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Xunit;

namespace EFCore.ClickHouse.Tests;

/// <summary>
/// Runs replicated tables, <c>ON CLUSTER</c> DDL and migrations against a cluster with two replicas.
/// Each test writes through one replica and reads through the other, so a table or a history row that
/// exists on only one replica makes the test fail.
/// </summary>
public class ReplicationIntegrationTests : IClassFixture<ClickHouseClusterFixture>
{
    private const string Cluster = ClickHouseClusterFixture.ClusterName;
    private const string TwoShards = ClickHouseClusterFixture.TwoShardClusterName;

    private readonly ClickHouseClusterFixture _cluster;

    public ReplicationIntegrationTests(ClickHouseClusterFixture cluster)
    {
        _cluster = cluster;
    }

    [Fact]
    public async Task EnsureCreated_creates_every_replicated_engine_on_both_replicas_and_rows_replicate()
    {
        var db = _cluster.NewDatabaseName();

        await using (var ctx = new EnginesContext(Options(_cluster.Node1(db), o => o.UseCluster(Cluster))))
        {
            Assert.True(await ctx.Database.EnsureCreatedAsync());

            ctx.Add(new ReplacingRow { Id = 1, Version = 1, Name = "first" });
            ctx.Add(new ReplacingRow { Id = 2, Version = 1, Name = "second" });
            ctx.Add(new MergeTreeRow { Id = 7, Name = "seven" });
            await ctx.SaveChangesAsync();
        }

        var expectedEngines = new Dictionary<string, string>
        {
            ["merge_tree"] = "ReplicatedMergeTree",
            ["replacing"] = "ReplicatedReplacingMergeTree",
            ["summing"] = "ReplicatedSummingMergeTree",
            ["aggregating"] = "ReplicatedAggregatingMergeTree",
            ["collapsing"] = "ReplicatedCollapsingMergeTree",
            ["versioned_collapsing"] = "ReplicatedVersionedCollapsingMergeTree",
        };

        foreach (var (table, engine) in expectedEngines)
        {
            Assert.Equal(engine, await QueryScalarAsync<string>(_cluster.Node2ConnectionString,
                $"SELECT engine FROM system.tables WHERE database = '{db}' AND name = '{table}'"));
            Assert.Equal(2, await QueryScalarAsync<byte>(_cluster.Node2ConnectionString,
                $"SELECT total_replicas FROM system.replicas WHERE database = '{db}' AND table = '{table}'"));
        }

        await SyncReplicaAsync(_cluster.Node2ConnectionString, db, "replacing");
        await SyncReplicaAsync(_cluster.Node2ConnectionString, db, "merge_tree");

        await using (var ctx = new EnginesContext(Options(_cluster.Node2(db), o => o.UseCluster(Cluster))))
        {
            var replacing = await ctx.Set<ReplacingRow>().OrderBy(r => r.Id).Select(r => r.Name).ToListAsync();
            Assert.Equal(["first", "second"], replacing);
            Assert.Equal("seven", (await ctx.Set<MergeTreeRow>().SingleAsync()).Name);

            Assert.True(await ctx.Database.EnsureDeletedAsync());
        }

        Assert.False(await QueryScalarAsync<bool>(_cluster.Node1ConnectionString, $"EXISTS DATABASE {db}"));
        Assert.False(await QueryScalarAsync<bool>(_cluster.Node2ConnectionString, $"EXISTS DATABASE {db}"));
    }

    [Fact]
    public async Task Migrations_applied_through_one_replica_are_applied_on_the_other()
    {
        var db = _cluster.NewDatabaseName();

        // Migrate creates the database, the history table and the first table on both replicas.
        await using (var ctx = new MigrationsContext(Options(_cluster.Node1(db), o => o.UseCluster(Cluster))))
            await ctx.GetService<IMigrator>().MigrateAsync(CreateEventsMigration.MigrationId);

        Assert.Equal("ReplicatedMergeTree", await QueryScalarAsync<string>(_cluster.Node2ConnectionString,
            $"SELECT engine FROM system.tables WHERE database = '{db}' AND name = '__EFMigrationsHistory'"));
        Assert.Equal("ReplicatedReplacingMergeTree", await QueryScalarAsync<string>(_cluster.Node2ConnectionString,
            $"SELECT engine FROM system.tables WHERE database = '{db}' AND name = 'events'"));

        await SyncReplicaAsync(_cluster.Node2ConnectionString, db, "__EFMigrationsHistory");

        // The second replica sees the first migration as applied and applies only the second one.
        await using (var ctx = new MigrationsContext(Options(_cluster.Node2(db), o => o.UseCluster(Cluster))))
        {
            Assert.Equal([AddScoreMigration.MigrationId], await ctx.Database.GetPendingMigrationsAsync());
            await ctx.Database.MigrateAsync();
        }

        await SyncReplicaAsync(_cluster.Node1ConnectionString, db, "__EFMigrationsHistory");

        await using (var ctx = new MigrationsContext(Options(_cluster.Node1(db), o => o.UseCluster(Cluster))))
        {
            Assert.Empty(await ctx.Database.GetPendingMigrationsAsync());
            Assert.Equal(
                [CreateEventsMigration.MigrationId, AddScoreMigration.MigrationId],
                await ctx.Database.GetAppliedMigrationsAsync());

            // Revert the second migration and apply it again. The second history row of the migration
            // is equal to the first one, and the history table must keep it.
            var migrator = ctx.GetService<IMigrator>();
            await migrator.MigrateAsync(CreateEventsMigration.MigrationId);
            Assert.Equal([AddScoreMigration.MigrationId], await ctx.Database.GetPendingMigrationsAsync());
            await migrator.MigrateAsync();
        }

        await SyncReplicaAsync(_cluster.Node2ConnectionString, db, "__EFMigrationsHistory");

        await using (var ctx = new MigrationsContext(Options(_cluster.Node2(db), o => o.UseCluster(Cluster))))
            Assert.Empty(await ctx.Database.GetPendingMigrationsAsync());

        Assert.Equal(1UL, await QueryScalarAsync<ulong>(_cluster.Node2ConnectionString,
            $"SELECT count() FROM system.columns WHERE database = '{db}' AND table = 'events' AND name = 'Score'"));
    }

    [Fact]
    public async Task Migrations_share_one_history_across_shards_that_reuse_replica_names()
    {
        // ch1 (shard 01) and ch3 (shard 02) both have the replica name r1.
        var db = _cluster.NewDatabaseName();

        await using (var ctx = new MigrationsContext(Options(_cluster.Node1(db), o => o.UseCluster(TwoShards))))
            await ctx.GetService<IMigrator>().MigrateAsync(CreateEventsMigration.MigrationId);

        // One history table with a replica on each of the three hosts.
        Assert.Equal(3, await QueryScalarAsync<byte>(_cluster.Node3ConnectionString,
            $"SELECT total_replicas FROM system.replicas WHERE database = '{db}' AND table = '__EFMigrationsHistory'"));

        await SyncReplicaAsync(_cluster.Node3ConnectionString, db, "__EFMigrationsHistory");

        // The host on the second shard sees the first migration as applied and applies only the second one.
        await using (var ctx = new MigrationsContext(Options(_cluster.Node3(db), o => o.UseCluster(TwoShards))))
        {
            Assert.Equal([AddScoreMigration.MigrationId], await ctx.Database.GetPendingMigrationsAsync());
            await ctx.Database.MigrateAsync();
        }

        await SyncReplicaAsync(_cluster.Node1ConnectionString, db, "__EFMigrationsHistory");

        await using (var ctx = new MigrationsContext(Options(_cluster.Node1(db), o => o.UseCluster(TwoShards))))
            Assert.Empty(await ctx.Database.GetPendingMigrationsAsync());

        foreach (var connectionString in new[]
            { _cluster.Node1ConnectionString, _cluster.Node2ConnectionString, _cluster.Node3ConnectionString })
        {
            Assert.Equal(1UL, await QueryScalarAsync<ulong>(connectionString,
                $"SELECT count() FROM system.columns WHERE database = '{db}' AND table = 'events' AND name = 'Score'"));
        }
    }

    [Fact]
    public async Task Explicit_zookeeper_path_and_cluster_macro_are_used()
    {
        var db = _cluster.NewDatabaseName();

        await using (var ctx = new ExplicitPathContext(Options(_cluster.Node1(db), o => o.UseCluster("{cluster}"))))
            Assert.True(await ctx.Database.EnsureCreatedAsync());

        Assert.Equal($"/clickhouse/tables/01/{db}/explicit_path", await QueryScalarAsync<string>(
            _cluster.Node2ConnectionString,
            $"SELECT zookeeper_path FROM system.replicas WHERE database = '{db}' AND table = 'explicit_path'"));
        Assert.Equal("r2", await QueryScalarAsync<string>(_cluster.Node2ConnectionString,
            $"SELECT replica_name FROM system.replicas WHERE database = '{db}' AND table = 'explicit_path'"));
        Assert.Equal(2, await QueryScalarAsync<byte>(_cluster.Node2ConnectionString,
            $"SELECT total_replicas FROM system.replicas WHERE database = '{db}' AND table = 'explicit_path'"));
    }

    [Fact]
    public async Task Replicated_database_with_replicated_history_table_shares_migration_history()
    {
        var db = _cluster.NewDatabaseName();

        // A Replicated database replicates DDL itself and refuses ON CLUSTER, so the context does not use UseCluster.
        await ExecuteAsync(_cluster.Node1ConnectionString,
            $"CREATE DATABASE {db} ON CLUSTER '{Cluster}' " +
            $"ENGINE = Replicated('/clickhouse/databases/{db}', '{{shard}}', '{{replica}}')");

        await using (var ctx = new MigrationsContext(
            Options(_cluster.Node1(db), o => o.UseReplicatedMigrationsHistoryTable())))
        {
            await ctx.Database.MigrateAsync();
        }

        await ExecuteAsync(_cluster.Node2ConnectionString, $"SYSTEM SYNC DATABASE REPLICA {db}");
        await SyncReplicaAsync(_cluster.Node2ConnectionString, db, "__EFMigrationsHistory");

        Assert.Equal("ReplicatedMergeTree", await QueryScalarAsync<string>(_cluster.Node2ConnectionString,
            $"SELECT engine FROM system.tables WHERE database = '{db}' AND name = '__EFMigrationsHistory'"));

        await using (var ctx = new MigrationsContext(
            Options(_cluster.Node2(db), o => o.UseReplicatedMigrationsHistoryTable())))
        {
            Assert.Empty(await ctx.Database.GetPendingMigrationsAsync());
        }
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private static DbContextOptions Options(
        string connectionString,
        Action<global::ClickHouse.EntityFrameworkCore.Infrastructure.ClickHouseDbContextOptionsBuilder> configure)
        => new DbContextOptionsBuilder()
            .UseClickHouse(connectionString, configure)
            // The test migrations have no model snapshot.
            .ConfigureWarnings(w => w.Ignore(RelationalEventId.PendingModelChangesWarning))
            .Options;

    private static async Task SyncReplicaAsync(string connectionString, string database, string table)
        => await ExecuteAsync(connectionString, $"SYSTEM SYNC REPLICA `{database}`.`{table}`");

    private static async Task ExecuteAsync(string connectionString, string sql)
    {
        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(connectionString);
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<T> QueryScalarAsync<T>(string connectionString, string sql)
    {
        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(connectionString);
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        var result = await command.ExecuteScalarAsync();
        return (T)Convert.ChangeType(result!, typeof(T), System.Globalization.CultureInfo.InvariantCulture);
    }

    // ── Model ───────────────────────────────────────────────────────────────

    public abstract class EngineRow
    {
        public long Id { get; set; }
        public ulong Version { get; set; }
        public byte IsDeleted { get; set; }
        public sbyte Sign { get; set; } = 1;
        public long Amount { get; set; }
        public string Name { get; set; } = "";
    }

    public class MergeTreeRow : EngineRow;
    public class ReplacingRow : EngineRow;
    public class SummingRow : EngineRow;
    public class AggregatingRow : EngineRow;
    public class CollapsingRow : EngineRow;
    public class VersionedCollapsingRow : EngineRow;

    private sealed class EnginesContext(DbContextOptions options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<MergeTreeRow>().ToTable("merge_tree", t => t
                .HasMergeTreeEngine().WithOrderBy("Id").WithReplication());
            modelBuilder.Entity<ReplacingRow>().ToTable("replacing", t => t
                .HasReplacingMergeTreeEngine("Version", "IsDeleted").WithOrderBy("Id").WithReplication());
            modelBuilder.Entity<SummingRow>().ToTable("summing", t => t
                .HasSummingMergeTreeEngine("Amount").WithOrderBy("Id").WithReplication());
            modelBuilder.Entity<AggregatingRow>().ToTable("aggregating", t => t
                .HasAggregatingMergeTreeEngine().WithOrderBy("Id").WithReplication());
            modelBuilder.Entity<CollapsingRow>().ToTable("collapsing", t => t
                .HasCollapsingMergeTreeEngine("Sign").WithOrderBy("Id").WithReplication());
            modelBuilder.Entity<VersionedCollapsingRow>().ToTable("versioned_collapsing", t => t
                .HasVersionedCollapsingMergeTreeEngine("Sign", "Version").WithOrderBy("Id").WithReplication());
        }
    }

    private sealed class ExplicitPathContext(DbContextOptions options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<MergeTreeRow>().ToTable("explicit_path", t => t
                .HasMergeTreeEngine()
                .WithOrderBy("Id")
                .WithReplication("/clickhouse/tables/{shard}/{database}/{table}", "{replica}"));
    }

    public class Event
    {
        public long Id { get; set; }
        public ulong Version { get; set; }
        public string Name { get; set; } = "";
        public int Score { get; set; }
    }

    public sealed class MigrationsContext(DbContextOptions options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<Event>().ToTable("events", t => t
                .HasReplacingMergeTreeEngine("Version").WithOrderBy("Id").WithReplication());
    }

    // The migrations below have the shape that `dotnet ef migrations add` generates for MigrationsContext.

    [DbContext(typeof(MigrationsContext))]
    [Migration(MigrationId)]
    public class CreateEventsMigration : Migration
    {
        public const string MigrationId = "20261125000001_CreateEvents";

        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "events",
                columns: table => new
                {
                    Id = table.Column<long>(type: "Int64", nullable: false),
                    Version = table.Column<ulong>(type: "UInt64", nullable: false),
                    Name = table.Column<string>(type: "String", nullable: false),
                },
                constraints: table => table.PrimaryKey("PK_events", x => x.Id))
                .Annotation("ClickHouse:Engine", "ReplacingMergeTree")
                .Annotation("ClickHouse:OrderBy", new[] { "Id" })
                .Annotation("ClickHouse:ReplacingMergeTree:Version", "Version")
                .Annotation("ClickHouse:Replicated", true);
        }
    }

    [DbContext(typeof(MigrationsContext))]
    [Migration(MigrationId)]
    public class AddScoreMigration : Migration
    {
        public const string MigrationId = "20261125000002_AddScore";

        protected override void Up(MigrationBuilder migrationBuilder)
            => migrationBuilder.AddColumn<int>(
                name: "Score",
                table: "events",
                type: "Int32",
                nullable: false,
                defaultValue: 0);

        protected override void Down(MigrationBuilder migrationBuilder)
            => migrationBuilder.DropColumn(name: "Score", table: "events");
    }
}
