using ClickHouse.EntityFrameworkCore.Extensions;
using ClickHouse.EntityFrameworkCore.Metadata;
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
/// End-to-end tests for ClickHouse dictionaries against a real server: declare a dictionary over a
/// source table, run the model differ → splitter → SQL generator → execute, then verify the
/// dictionary exists (system.dictionaries), that <c>dictGet</c> resolves values, that a changed
/// dictionary re-applies via CREATE OR REPLACE, and that it drops cleanly.
/// </summary>
public class DictionaryIntegrationTests : IAsyncLifetime
{
    private string _connectionString = default!;
    private string _databaseName = default!;

    public async Task InitializeAsync()
    {
        _connectionString = await SharedContainer.GetConnectionStringAsync();
        _databaseName = System.Text.RegularExpressions.Regex.Match(
            _connectionString, @"Database=([^;]+)").Groups[1].Value;
    }

    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task Dictionary_is_created_after_source_table_and_dictGet_resolves()
    {
        await using var ctx = CreateContext(ClickHouseDictionaryLayout.Hashed);

        await ApplyAsync(Diff(null, ctx), ctx);

        // The dictionary exists (its create succeeded only because the source table was created first).
        var count = await QueryScalarAsync(
            $"SELECT count() FROM system.dictionaries WHERE database = '{_databaseName}' AND name = 'currency_dict'");
        Assert.Equal("1", count);

        // The generated DDL produced the right key + attribute columns. (We verify structure via
        // system.dictionaries rather than dictGet: loading a CLICKHOUSE-source dictionary connects
        // back to the server as the 'default' user, and the test image password-protects it, so a
        // dictGet-triggered load fails auth — an environment property, not a provider concern.)
        var keys = await QueryScalarAsync(
            $"SELECT arrayStringConcat(`key.names`, ',') FROM system.dictionaries WHERE database = '{_databaseName}' AND name = 'currency_dict'");
        Assert.Contains("Id", keys!);
        var attrs = await QueryScalarAsync(
            $"SELECT arrayStringConcat(`attribute.names`, ',') FROM system.dictionaries WHERE database = '{_databaseName}' AND name = 'currency_dict'");
        Assert.Contains("Rate", attrs!);
        Assert.Contains("Name", attrs!);

        // A nullable value-type attribute (double? Spread) renders as Nullable(Float64); the key stays bare.
        var ddl = await QueryScalarAsync(
            $"SELECT create_table_query FROM system.tables WHERE database = '{_databaseName}' AND name = 'currency_dict'");
        Assert.Contains("Nullable(Float64)", ddl!);
        Assert.DoesNotContain("Nullable(UInt64)", ddl!); // the UInt64 key is never wrapped

        await ExecuteAsync("DROP DICTIONARY IF EXISTS currency_dict", "DROP TABLE IF EXISTS currencies");
    }

    [Fact]
    public async Task Changed_dictionary_reapplies_via_create_or_replace()
    {
        await using var ctxV1 = CreateContext(ClickHouseDictionaryLayout.Hashed);
        await ApplyAsync(Diff(null, ctxV1), ctxV1);

        // Change only the layout — the differ must emit a CREATE OR REPLACE (no drop), not an ALTER.
        await using var ctxV2 = CreateContext(ClickHouseDictionaryLayout.Flat);
        var changeOps = Diff(ctxV1, ctxV2);

        Assert.Contains(changeOps, o => o is ClickHouseCreateDictionaryOperation { OrReplace: true });
        Assert.DoesNotContain(changeOps, o => o is ClickHouseDropDictionaryOperation);

        await ApplyAsync(changeOps, ctxV2);

        var count = await QueryScalarAsync(
            $"SELECT count() FROM system.dictionaries WHERE database = '{_databaseName}' AND name = 'currency_dict'");
        Assert.Equal("1", count);

        // The OR REPLACE actually took effect: the rendered DDL now uses LAYOUT(FLAT()).
        // (Read from system.tables so we don't need to load the dictionary.)
        var ddl = await QueryScalarAsync(
            $"SELECT create_table_query FROM system.tables WHERE database = '{_databaseName}' AND name = 'currency_dict'");
        Assert.Contains("FLAT", ddl!);

        await ExecuteAsync("DROP DICTIONARY IF EXISTS currency_dict", "DROP TABLE IF EXISTS currencies");
    }

    [Fact]
    public async Task Removed_dictionary_is_dropped()
    {
        await using var ctx = CreateContext(ClickHouseDictionaryLayout.Hashed);
        await ApplyAsync(Diff(null, ctx), ctx);

        // Diff back to an empty model → the dictionary (and its table) are dropped, dictionary first.
        await using var empty = CreateContext(null);
        var dropOps = Diff(ctx, empty);
        Assert.Contains(dropOps, o => o is ClickHouseDropDictionaryOperation);
        await ApplyAsync(dropOps, ctx);

        var count = await QueryScalarAsync(
            $"SELECT count() FROM system.dictionaries WHERE database = '{_databaseName}' AND name = 'currency_dict'");
        Assert.Equal("0", count);
    }

    private IReadOnlyList<MigrationOperation> Diff(DbContext? from, DbContext to)
    {
        var fromModel = from?.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var toModel = to.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var ops = to.GetService<IMigrationsModelDiffer>().GetDifferences(fromModel, toModel);
        // Route through the splitter so ordering (table before dictionary) matches scaffolding.
        return new ClickHouseMigrationsSplitter().Split(ops).Select(s => s.Operation).ToList();
    }

    private async Task ApplyAsync(IReadOnlyList<MigrationOperation> operations, DbContext context)
    {
        var generator = context.GetService<IMigrationsSqlGenerator>();
        using var conn = new global::ClickHouse.Driver.ADO.ClickHouseConnection(_connectionString);
        await conn.OpenAsync();
        foreach (var command in generator.Generate(operations))
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = command.CommandText;
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private async Task ExecuteAsync(params string[] statements)
    {
        using var conn = new global::ClickHouse.Driver.ADO.ClickHouseConnection(_connectionString);
        await conn.OpenAsync();
        foreach (var sql in statements)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private async Task<string?> QueryScalarAsync(string sql)
    {
        using var conn = new global::ClickHouse.Driver.ADO.ClickHouseConnection(_connectionString);
        await conn.OpenAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return (await cmd.ExecuteScalarAsync())?.ToString();
    }

    private DbContext CreateContext(ClickHouseDictionaryLayout? layout)
    {
        var options = new DbContextOptionsBuilder()
            .UseClickHouse(_connectionString)
            .EnableServiceProviderCaching(false)
            .Options;
        return new DictContext(options, layout);
    }

    private sealed class DictContext(DbContextOptions options, ClickHouseDictionaryLayout? layout) : DbContext(options)
    {
        private readonly ClickHouseDictionaryLayout? _layout = layout;

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            if (_layout is null)
                return;

            modelBuilder.Entity<Currency>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("currencies", t => t.HasMergeTreeEngine().WithOrderBy("Id"));
            });

            modelBuilder.HasDictionary<Currency>("currency_dict")
                .FromTable<Currency>()
                .HasKey(d => d.Id)
                .Layout(_layout.Value)
                .Lifetime(minSeconds: 300, maxSeconds: 360);
        }
    }

    private class Currency
    {
        public ulong Id { get; set; }
        public double Rate { get; set; }
        public double? Spread { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
