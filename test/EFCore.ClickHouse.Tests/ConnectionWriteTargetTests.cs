using ClickHouse.Driver.ADO;
using ClickHouse.EntityFrameworkCore.Extensions;
using ClickHouse.EntityFrameworkCore.Storage.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Xunit;

namespace EFCore.ClickHouse.Tests;

/// <summary>
/// Inserts through <c>UseClickHouse(ClickHouseConnection)</c>, and checks that inserts, queries and the
/// master connection use the new target after <c>SetConnectionString</c> or <c>SetDbConnection</c>.
/// Target A and target B are two databases on the same server.
/// </summary>
public class ConnectionWriteTargetTests : IClassFixture<ConnectionWriteTargetFixture>
{
    private readonly ConnectionWriteTargetFixture _fixture;

    public ConnectionWriteTargetTests(ConnectionWriteTargetFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task SaveChanges_DbConnection_InsertsRows()
    {
        const string marker = nameof(SaveChanges_DbConnection_InsertsRows);
        await using var connection = new ClickHouseConnection(_fixture.ConnectionStringA);
        await using var ctx = new WriteTargetContext(o => o.UseClickHouse(connection));

        ctx.Rows.AddRange(Row(1, marker), Row(2, marker));
        await ctx.SaveChangesAsync();

        Assert.Equal(2, await ctx.Rows.CountAsync(r => r.Marker == marker));
        Assert.Equal(2, await CountAsync(_fixture.ConnectionStringA, marker));
    }

    [Fact]
    public async Task BulkInsertAsync_DbConnection_InsertsRows()
    {
        const string marker = nameof(BulkInsertAsync_DbConnection_InsertsRows);
        await using var connection = new ClickHouseConnection(_fixture.ConnectionStringA);
        await using var ctx = new WriteTargetContext(o => o.UseClickHouse(connection));

        var inserted = await ctx.BulkInsertAsync([Row(1, marker), Row(2, marker), Row(3, marker)]);

        Assert.Equal(3, inserted);
        Assert.Equal(3, await ctx.Rows.CountAsync(r => r.Marker == marker));
        Assert.Equal(3, await CountAsync(_fixture.ConnectionStringA, marker));
    }

    [Fact]
    public async Task SaveChanges_DbConnection_UsesChangedDatabase()
    {
        // Queries use ClickHouseConnection.Database, which includes ChangeDatabase. Inserts must too.
        const string marker = nameof(SaveChanges_DbConnection_UsesChangedDatabase);
        await using var connection = new ClickHouseConnection(_fixture.ConnectionStringA);
        connection.ChangeDatabase(_fixture.DatabaseB);
        await using var ctx = new WriteTargetContext(o => o.UseClickHouse(connection));

        ctx.Rows.Add(Row(1, marker));
        await ctx.SaveChangesAsync();
        await ctx.BulkInsertAsync([Row(2, marker)]);

        Assert.Equal(2, await ctx.Rows.CountAsync(r => r.Marker == marker));
        await AssertRowsInTargetBAsync(marker, 2);
    }

    [Fact]
    public async Task SetConnectionString_BeforeFirstUse_WritesToNewTarget()
    {
        const string marker = nameof(SetConnectionString_BeforeFirstUse_WritesToNewTarget);
        await using var ctx = new WriteTargetContext(o => o.UseClickHouse(_fixture.ConnectionStringA));

        ctx.Database.SetConnectionString(_fixture.ConnectionStringB);
        ctx.Rows.Add(Row(1, marker));
        await ctx.SaveChangesAsync();

        Assert.Equal(1, await ctx.Rows.CountAsync(r => r.Marker == marker));
        await AssertRowsInTargetBAsync(marker, 1);
    }

    [Fact]
    public async Task SetConnectionString_AfterFirstUse_WritesToNewTarget()
    {
        const string marker = nameof(SetConnectionString_AfterFirstUse_WritesToNewTarget);
        await using var ctx = new WriteTargetContext(o => o.UseClickHouse(_fixture.ConnectionStringA));
        await ctx.Rows.CountAsync();

        ctx.Database.SetConnectionString(_fixture.ConnectionStringB);
        ctx.Rows.Add(Row(1, marker));
        await ctx.SaveChangesAsync();
        await ctx.BulkInsertAsync([Row(2, marker)]);

        Assert.Equal(2, await ctx.Rows.CountAsync(r => r.Marker == marker));
        await AssertRowsInTargetBAsync(marker, 2);
    }

    [Fact]
    public async Task SetConnectionString_DataSource_WritesToNewTarget()
    {
        const string marker = nameof(SetConnectionString_DataSource_WritesToNewTarget);
        await using var dataSource = new ClickHouseDataSource(_fixture.ConnectionStringA);
        await using var ctx = new WriteTargetContext(o => o.UseClickHouse(dataSource));

        ctx.Database.SetConnectionString(_fixture.ConnectionStringB);
        ctx.Rows.Add(Row(1, marker));
        await ctx.SaveChangesAsync();

        Assert.Equal(1, await ctx.Rows.CountAsync(r => r.Marker == marker));
        await AssertRowsInTargetBAsync(marker, 1);
    }

    [Fact]
    public async Task SetConnectionString_BeforeFirstUse_EnsureCreatedUsesNewTarget()
    {
        var database = _fixture.DatabaseA + "_created";
        var connectionString = new ClickHouseConnectionStringBuilder(_fixture.ConnectionStringA)
        {
            Database = database
        }.ConnectionString;
        await using var ctx = new WriteTargetContext(o => o.UseClickHouse(_fixture.ConnectionStringA));

        ctx.Database.SetConnectionString(connectionString);
        try
        {
            Assert.True(await ctx.Database.EnsureCreatedAsync());

            Assert.Equal(1, await ScalarAsync(
                _fixture.ConnectionStringA,
                $"SELECT count() FROM system.tables WHERE database = '{database}' AND name = 'write_target_rows'"));
        }
        finally
        {
            await ctx.Database.EnsureDeletedAsync();
        }
    }

    [Fact]
    public async Task SetDbConnection_ConnectionString_WritesToNewConnection()
    {
        const string marker = nameof(SetDbConnection_ConnectionString_WritesToNewConnection);
        await using var ctx = new WriteTargetContext(o => o.UseClickHouse(_fixture.ConnectionStringA));
        await ctx.Rows.CountAsync();

        await using var connection = new ClickHouseConnection(_fixture.ConnectionStringB);
        ctx.Database.SetDbConnection(connection);
        ctx.Rows.Add(Row(1, marker));
        await ctx.SaveChangesAsync();
        await ctx.BulkInsertAsync([Row(2, marker)]);

        Assert.Equal(2, await ctx.Rows.CountAsync(r => r.Marker == marker));
        await AssertRowsInTargetBAsync(marker, 2);
    }

    [Fact]
    public async Task SetDbConnection_DataSource_WritesToNewConnection()
    {
        const string marker = nameof(SetDbConnection_DataSource_WritesToNewConnection);
        await using var dataSource = new ClickHouseDataSource(_fixture.ConnectionStringA);
        await using var ctx = new WriteTargetContext(o => o.UseClickHouse(dataSource));

        await using var connection = new ClickHouseConnection(_fixture.ConnectionStringB);
        ctx.Database.SetDbConnection(connection);
        ctx.Rows.Add(Row(1, marker));
        await ctx.SaveChangesAsync();

        Assert.Equal(1, await ctx.Rows.CountAsync(r => r.Marker == marker));
        await AssertRowsInTargetBAsync(marker, 1);
    }

    [Fact]
    public async Task SetDbConnection_DbConnection_WritesToNewConnection()
    {
        const string marker = nameof(SetDbConnection_DbConnection_WritesToNewConnection);
        await using var first = new ClickHouseConnection(_fixture.ConnectionStringA);
        await using var ctx = new WriteTargetContext(o => o.UseClickHouse(first));
        ctx.Rows.Add(Row(1, marker));
        await ctx.SaveChangesAsync();

        await using var second = new ClickHouseConnection(_fixture.ConnectionStringB);
        ctx.Database.SetDbConnection(second);
        ctx.Rows.Add(Row(2, marker));
        await ctx.SaveChangesAsync();

        Assert.Equal(1, await CountAsync(_fixture.ConnectionStringA, marker));
        Assert.Equal(1, await CountAsync(_fixture.ConnectionStringB, marker));
    }

    [Fact]
    public async Task CreateMasterConnection_AfterSetConnectionString_UsesNewTarget()
    {
        // The databases are on the same server, so a session setting identifies the target.
        await using var ctx = new WriteTargetContext(o => o.UseClickHouse(_fixture.ConnectionStringA));
        await ctx.Rows.CountAsync();

        ctx.Database.SetConnectionString(_fixture.ConnectionStringB + ";set_log_comment=target-b");

        Assert.Equal("target-b", await MasterLogCommentAsync(ctx));
    }

    [Fact]
    public async Task CreateMasterConnection_AfterSetDbConnection_UsesNewTarget()
    {
        await using var dataSource = new ClickHouseDataSource(_fixture.ConnectionStringA);
        await using var ctx = new WriteTargetContext(o => o.UseClickHouse(dataSource));

        var settings = new ClickHouseClientSettings(_fixture.ConnectionStringB);
        settings.CustomSettings["log_comment"] = "target-b";
        await using var connection = new ClickHouseConnection(settings);
        ctx.Database.SetDbConnection(connection);

        Assert.Equal("target-b", await MasterLogCommentAsync(ctx));
    }

    private static WriteTargetRow Row(long id, string marker) => new() { Id = id, Marker = marker };

    private async Task AssertRowsInTargetBAsync(string marker, long expected)
    {
        Assert.Equal(expected, await CountAsync(_fixture.ConnectionStringB, marker));
        Assert.Equal(0, await CountAsync(_fixture.ConnectionStringA, marker));
    }

    private static Task<long> CountAsync(string connectionString, string marker)
        => ScalarAsync(connectionString, $"SELECT count() FROM write_target_rows WHERE marker = '{marker}'");

    private static async Task<long> ScalarAsync(string connectionString, string sql)
    {
        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    private static async Task<string?> MasterLogCommentAsync(DbContext ctx)
    {
        await using var master = ctx.GetService<IClickHouseRelationalConnection>().CreateMasterConnection();
        await master.OpenAsync(CancellationToken.None);
        await using var cmd = master.DbConnection.CreateCommand();
        cmd.CommandText = "SELECT getSetting('log_comment')";
        return (string?)await cmd.ExecuteScalarAsync();
    }
}

public class WriteTargetRow
{
    public long Id { get; set; }
    public string Marker { get; set; } = string.Empty;
}

public class WriteTargetContext : DbContext
{
    private readonly Action<DbContextOptionsBuilder> _configure;

    public WriteTargetContext(Action<DbContextOptionsBuilder> configure)
    {
        _configure = configure;
    }

    public DbSet<WriteTargetRow> Rows => Set<WriteTargetRow>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => _configure(optionsBuilder);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
        => modelBuilder.Entity<WriteTargetRow>(entity =>
        {
            entity.ToTable("write_target_rows");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.Marker).HasColumnName("marker");
        });
}

public class ConnectionWriteTargetFixture : IAsyncLifetime
{
    public string ConnectionStringA { get; private set; } = string.Empty;
    public string ConnectionStringB { get; private set; } = string.Empty;
    public string DatabaseA => new ClickHouseConnectionStringBuilder(ConnectionStringA).Database;
    public string DatabaseB => new ClickHouseConnectionStringBuilder(ConnectionStringB).Database;

    public async Task InitializeAsync()
    {
        ConnectionStringA = await SharedContainer.GetConnectionStringAsync();
        ConnectionStringB = await SharedContainer.GetConnectionStringAsync();

        foreach (var connectionString in new[] { ConnectionStringA, ConnectionStringB })
        {
            await using var connection = new ClickHouseConnection(connectionString);
            await connection.OpenAsync();
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = """
                CREATE TABLE write_target_rows (
                    id Int64,
                    marker String
                ) ENGINE = MergeTree()
                ORDER BY id
                """;
            await cmd.ExecuteNonQueryAsync();
        }
    }

    public Task DisposeAsync() => Task.CompletedTask;
}
