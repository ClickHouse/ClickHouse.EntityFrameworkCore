using ClickHouse.EntityFrameworkCore.Extensions;
using ClickHouse.EntityFrameworkCore.Migrations.Design;
using ClickHouse.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Xunit;

namespace EFCore.ClickHouse.Tests;

/// <summary>
/// End-to-end system tests for split, dependency-ordered migrations against a real ClickHouse.
/// The full runtime pipeline is exercised: model diff → <see cref="ClickHouseMigrationsSplitter"/>
/// → per-step SQL generation → execution → per-step recording via the provider's real
/// <see cref="IHistoryRepository"/>. Ordering is proven by the materialized-view step succeeding
/// (it would fail if its source/target tables had not been created first), and the per-step
/// history rows are what make a partial failure resumable.
/// </summary>
public class MigrationSplitSystemTests : IAsyncLifetime
{
    private string _connectionString = default!;

    public async Task InitializeAsync()
        => _connectionString = await SharedContainer.GetConnectionStringAsync();

    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task Split_steps_apply_in_dependency_order_and_record_per_step_history()
    {
        await using var ctx = CreateModelContext();
        var steps = SplitSteps(ctx);

        // Sanity: the model produces table creates and a materialized-view create, and the view
        // is ordered after the tables it reads.
        Assert.True(steps.Count >= 3);
        var mvIndex = IndexOfMaterializedView(steps);
        Assert.True(mvIndex >= 0);
        Assert.All(steps.Where(s => s.Operation is CreateTableOperation),
            s => Assert.True(s.StepNumber < steps[mvIndex].StepNumber));

        var history = ctx.GetService<IHistoryRepository>();
        var generator = ctx.GetService<IMigrationsSqlGenerator>();

        using var conn = new global::ClickHouse.Driver.ADO.ClickHouseConnection(_connectionString);
        await conn.OpenAsync();

        // Create the real EF history table, then apply every step in order, recording each as its
        // own history row exactly as the Migrator would.
        await ExecuteAsync(conn, history.GetCreateIfNotExistsScript());
        foreach (var step in steps)
        {
            foreach (var command in generator.Generate([step.Operation]))
                await ExecuteAsync(conn, command.CommandText);

            await ExecuteAsync(conn, history.GetInsertScript(new HistoryRow(StepMigrationId(step), "10.0.0")));
        }

        // Every step recorded.
        var recorded = await QueryScalarAsync(conn,
            "SELECT count() FROM `__EFMigrationsHistory`");
        Assert.Equal(steps.Count.ToString(), recorded);

        // The materialized view exists and is wired to its target.
        var engine = await QueryScalarAsync(conn,
            "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'sys_split_mv'");
        Assert.Equal("MaterializedView", engine);

        // Data flows through the view into the target — proving the whole set applied coherently.
        await ExecuteAsync(conn, "INSERT INTO sys_split_src VALUES (1, 10), (1, 5), (2, 7)");
        var total = await QueryScalarAsync(conn,
            "SELECT Total FROM sys_split_tgt FINAL WHERE Bucket = 1");
        Assert.Equal("15", total);
    }

    [Fact]
    public async Task Partial_application_is_resumable_via_per_step_history()
    {
        await using var ctx = CreateModelContext();
        var steps = SplitSteps(ctx);
        var history = ctx.GetService<IHistoryRepository>();
        var generator = ctx.GetService<IMigrationsSqlGenerator>();

        using var conn = new global::ClickHouse.Driver.ADO.ClickHouseConnection(_connectionString);
        await conn.OpenAsync();
        await ExecuteAsync(conn, history.GetCreateIfNotExistsScript());

        // Apply only the first two steps, recording their history — as if a later step had failed.
        foreach (var step in steps.Take(2))
        {
            foreach (var command in generator.Generate([step.Operation]))
                await ExecuteAsync(conn, command.CommandText);
            await ExecuteAsync(conn, history.GetInsertScript(new HistoryRow(StepMigrationId(step), "10.0.0")));
        }

        // Resume: apply only steps whose id is not already recorded. Re-running an applied step
        // (no IF NOT EXISTS) would throw, so this asserts the per-step history makes resume safe.
        var applied = await QueryAppliedIdsAsync(conn);
        foreach (var step in steps.Where(s => !applied.Contains(StepMigrationId(s))))
        {
            foreach (var command in generator.Generate([step.Operation]))
                await ExecuteAsync(conn, command.CommandText);
            await ExecuteAsync(conn, history.GetInsertScript(new HistoryRow(StepMigrationId(step), "10.0.0")));
        }

        // Fully applied and functional after the resume.
        var recorded = await QueryScalarAsync(conn, "SELECT count() FROM `__EFMigrationsHistory`");
        Assert.Equal(steps.Count.ToString(), recorded);

        await ExecuteAsync(conn, "INSERT INTO sys_split_src VALUES (5, 100)");
        var total = await QueryScalarAsync(conn,
            "SELECT Total FROM sys_split_tgt FINAL WHERE Bucket = 5");
        Assert.Equal("100", total);
    }

    private static IReadOnlyList<StepMigration> SplitSteps(DbContext ctx)
    {
        var emptyModel = new EmptyContext(ctx.Database.GetConnectionString()!)
            .GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var currModel = ctx.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var operations = ctx.GetService<IMigrationsModelDiffer>().GetDifferences(emptyModel, currModel);
        return new ClickHouseMigrationsSplitter().Split(operations);
    }

    private static int IndexOfMaterializedView(IReadOnlyList<StepMigration> steps)
    {
        for (var i = 0; i < steps.Count; i++)
            if (steps[i].Operation is ClickHouseCreateMaterializedViewOperation)
                return i;
        return -1;
    }

    // Step migration id mirrors what the scaffolder produces; only relative ordering matters here.
    private static string StepMigrationId(StepMigration step) => $"20990101000000_SysSplit_{step.StepSuffix}";

    private DbContext CreateModelContext()
    {
        var options = new DbContextOptionsBuilder()
            .UseClickHouse(_connectionString)
            .EnableServiceProviderCaching(false)
            .Options;
        return new ModelContext(options);
    }

    private static async Task ExecuteAsync(global::ClickHouse.Driver.ADO.ClickHouseConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<string?> QueryScalarAsync(global::ClickHouse.Driver.ADO.ClickHouseConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return (await cmd.ExecuteScalarAsync())?.ToString();
    }

    private static async Task<HashSet<string>> QueryAppliedIdsAsync(global::ClickHouse.Driver.ADO.ClickHouseConnection conn)
    {
        var ids = new HashSet<string>(StringComparer.Ordinal);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT MigrationId FROM `__EFMigrationsHistory`";
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            ids.Add(reader.GetString(0));
        return ids;
    }

    private sealed class ModelContext(DbContextOptions options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Src>(e => { e.HasKey(x => x.Id); e.ToTable("sys_split_src"); });
            modelBuilder.Entity<Tgt>(e =>
            {
                e.HasKey(x => x.Bucket);
                e.ToTable("sys_split_tgt", t => t.HasSummingMergeTreeEngine("Total").WithOrderBy("Bucket"));
            });
            modelBuilder.HasMaterializedView<Tgt>("sys_split_mv")
                .FromRaw("SELECT Id AS Bucket, Value AS Total FROM sys_split_src");
        }
    }

    private sealed class EmptyContext(string connectionString) : DbContext
    {
        private readonly string _connectionString = connectionString;

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.UseClickHouse(_connectionString).EnableServiceProviderCaching(false);
    }

    private class Src
    {
        public long Id { get; set; }
        public long Value { get; set; }
    }

    private class Tgt
    {
        public long Bucket { get; set; }
        public long Total { get; set; }
    }
}
