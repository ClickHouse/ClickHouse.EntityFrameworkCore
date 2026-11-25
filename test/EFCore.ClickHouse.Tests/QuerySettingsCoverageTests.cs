using ClickHouse.Driver.ADO;
using ClickHouse.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

namespace EFCore.ClickHouse.Tests;

/// <summary>
/// Checks in <c>system.query_log</c> that every statement the provider sends carries the provider
/// query settings (see <c>ClickHouseRelationalConnection.GetQuerySettings</c>). The context uses a
/// <see cref="ClickHouseDataSource"/> or a <see cref="ClickHouseConnection"/> whose settings do not
/// contain them, so only the settings that the provider adds to each statement can make the test
/// pass. When you add a code path that sends SQL to ClickHouse, add an operation that uses it to
/// this test.
/// </summary>
public class QuerySettingsCoverageTests : IClassFixture<ConnectionSettingsFixture>
{
    private readonly ConnectionSettingsFixture _fixture;

    public QuerySettingsCoverageTests(ConnectionSettingsFixture fixture)
    {
        _fixture = fixture;
    }

    public class CoverageParent
    {
        public long Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public class CoverageChild
    {
        public long Id { get; set; }
        public long ParentId { get; set; }
        public string Label { get; set; } = string.Empty;
    }

    private sealed class CoverageContext(Action<DbContextOptionsBuilder> configure) : DbContext
    {
        public DbSet<CoverageParent> Parents => Set<CoverageParent>();
        public DbSet<CoverageChild> Children => Set<CoverageChild>();

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => configure(optionsBuilder);

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<CoverageParent>(e =>
            {
                e.ToTable("coverage_parents");
                e.HasKey(x => x.Id);
            });
            modelBuilder.Entity<CoverageChild>(e =>
            {
                e.ToTable("coverage_children");
                e.HasKey(x => x.Id);
            });
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Every_statement_carries_the_provider_query_settings(bool useDbConnection)
    {
        var marker = $"query_settings_{Guid.NewGuid():N}";
        var databaseName = $"cov_{Guid.NewGuid():N}";

        var settings = new ClickHouseClientSettings(_fixture.ConnectionString) { Database = databaseName };
        Assert.False(settings.CustomSettings.ContainsKey("join_use_nulls"));

        // log_comment tags each statement of this test in system.query_log.
        settings.CustomSettings["log_comment"] = marker;

        await using var dataSource = new ClickHouseDataSource(settings);
        await using var connection = new ClickHouseConnection(settings);
        await using (var ctx = new CoverageContext(o =>
        {
            if (useDbConnection)
                o.UseClickHouse(connection);
            else
                o.UseClickHouse(dataSource);
        }))
        {
            var creator = ctx.GetService<IRelationalDatabaseCreator>();

            // Database creator: EXISTS DATABASE, CREATE DATABASE on the master connection, and
            // CREATE TABLE through EF Core commands.
            Assert.True(await ctx.Database.EnsureCreatedAsync());
            Assert.True(await creator.ExistsAsync());
            Assert.True(await creator.HasTablesAsync());

            // Inserts through SaveChanges and through BulkInsertAsync.
            ctx.Parents.AddRange(new CoverageParent { Id = 1, Name = "a" }, new CoverageParent { Id = 2, Name = "b" });
            await ctx.SaveChangesAsync();
            await ctx.BulkInsertAsync(new[] { new CoverageChild { Id = 10, ParentId = 1, Label = "x" } });

            // A LINQ LEFT JOIN. Parent 2 has no child, so the label must be null.
            var rows = await (
                    from p in ctx.Parents
                    join c in ctx.Children on p.Id equals c.ParentId into children
                    from c in children.DefaultIfEmpty()
                    orderby p.Id
                    select new { p.Id, Label = c == null ? null : c.Label })
                .ToListAsync();
            Assert.Equal([(1L, "x"), (2L, (string?)null)], rows.Select(r => (r.Id, (string?)r.Label)));

            // Raw SQL through EF Core.
            Assert.Equal(2, (await ctx.Database.SqlQueryRaw<long>(
                "SELECT count() AS Value FROM coverage_parents").ToListAsync()).Single());
            await ctx.Database.ExecuteSqlRawAsync("SELECT 1");

            // DROP DATABASE on the master connection.
            Assert.True(await ctx.Database.EnsureDeletedAsync());
        }

        var statements = await ReadQueryLogAsync(marker);

        Assert.NotEmpty(statements);
        Assert.Superset(
            new HashSet<string> { "Select", "Insert", "Create", "Drop" },
            statements.Select(s => s.Kind).ToHashSet());

        var missing = statements.Where(s => s.JoinUseNulls != "1").ToList();
        Assert.True(
            missing.Count == 0,
            "These statements did not carry join_use_nulls=1. Send SQL through an EF Core command, "
            + "or apply ClickHouseRelationalConnection.ApplyQuerySettings / CreateInsertOptions:\n"
            + string.Join("\n", missing.Select(s => $"  [{s.Kind}] {s.Query}")));
    }

    private async Task<List<(string Kind, string Query, string JoinUseNulls)>> ReadQueryLogAsync(string marker)
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await using (var flush = connection.CreateCommand())
        {
            flush.CommandText = "SYSTEM FLUSH LOGS";
            await flush.ExecuteNonQueryAsync();
        }

        await using var command = connection.CreateCommand();
        command.CommandText =
            "SELECT query_kind, query, Settings['join_use_nulls'] FROM system.query_log "
            + $"WHERE log_comment = '{marker}' AND type != 'QueryStart' "
            + "AND event_time > now() - INTERVAL 10 MINUTE";

        var statements = new List<(string, string, string)>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            statements.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2)));

        return statements;
    }
}
