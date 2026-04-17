using ClickHouse.Driver.ADO;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.ClickHouse.Tests;

/// <summary>
/// Regression tests for Finding 1 from the PR #10 review: the <c>join_use_nulls=1</c>
/// session setting must apply on all three <see cref="ClickHouseDbContextOptionsBuilderExtensions.UseClickHouse"/>
/// overloads — connection string, raw <see cref="System.Data.Common.DbConnection"/>, and
/// <see cref="System.Data.Common.DbDataSource"/> — not just the connection-string path.
/// </summary>
public class ConnectionSettingsTests : IClassFixture<ConnectionSettingsFixture>
{
    private readonly ConnectionSettingsFixture _fixture;

    public ConnectionSettingsTests(ConnectionSettingsFixture fixture)
    {
        _fixture = fixture;
    }

    private class MinimalContext : DbContext
    {
        private readonly Action<DbContextOptionsBuilder> _configure;

        public MinimalContext(Action<DbContextOptionsBuilder> configure)
        {
            _configure = configure;
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => _configure(optionsBuilder);
    }

    [Fact]
    public async Task UseClickHouse_ConnectionString_AppliesJoinUseNulls()
    {
        await using var ctx = new MinimalContext(o => o.UseClickHouse(_fixture.ConnectionString));
        Assert.True(await GetJoinUseNullsAsync(ctx));
    }

    [Fact]
    public async Task UseClickHouse_DbConnection_AppliesJoinUseNulls()
    {
        // User hands us a bare ClickHouseConnection with NO session settings baked in.
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await using var ctx = new MinimalContext(o => o.UseClickHouse(connection));

        Assert.True(await GetJoinUseNullsAsync(ctx));
    }

    [Fact]
    public async Task UseClickHouse_DbDataSource_AppliesJoinUseNulls()
    {
        // User hands us a ClickHouseDataSource without the setting pre-applied.
        await using var dataSource = new ClickHouseDataSource(_fixture.ConnectionString);
        await using var ctx = new MinimalContext(o => o.UseClickHouse(dataSource));

        Assert.True(await GetJoinUseNullsAsync(ctx));
    }

    [Fact]
    public async Task DisableJoinNullSemantics_SkipsInjection()
    {
        // When the user calls DisableJoinNullSemantics(), we must not touch the connection
        // string. Used to opt out when a ClickHouse server/profile forbids SET for this setting.
        await using var ctx = new MinimalContext(
            o => o.UseClickHouse(_fixture.ConnectionString, ch => ch.DisableJoinNullSemantics()));

        // Default server value is 0.
        Assert.False(await GetJoinUseNullsAsync(ctx));
    }

    [Fact]
    public async Task DisableJoinNullSemantics_SkipsInjection_OnDbConnectionPath()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await using var ctx = new MinimalContext(
            o => o.UseClickHouse(connection, ch => ch.DisableJoinNullSemantics()));

        Assert.False(await GetJoinUseNullsAsync(ctx));
    }

    [Fact]
    public async Task UseClickHouse_DbConnection_RespectsUserOptOut()
    {
        // If the user explicitly sets set_join_use_nulls=0 in the connection string,
        // the provider must NOT override that choice.
        var optOutConnectionString = _fixture.ConnectionString
            + (_fixture.ConnectionString.EndsWith(';') ? "" : ";")
            + "set_join_use_nulls=0";

        await using var connection = new ClickHouseConnection(optOutConnectionString);
        await using var ctx = new MinimalContext(o => o.UseClickHouse(connection));

        Assert.False(await GetJoinUseNullsAsync(ctx));
    }

    private static async Task<bool> GetJoinUseNullsAsync(DbContext ctx)
    {
        var connection = ctx.Database.GetDbConnection();
        var wasClosed = connection.State != System.Data.ConnectionState.Open;
        if (wasClosed)
            await ctx.Database.OpenConnectionAsync();

        try
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT getSetting('join_use_nulls')";
            var result = await cmd.ExecuteScalarAsync();
            return result is bool b ? b : bool.Parse(result?.ToString() ?? "false");
        }
        finally
        {
            if (wasClosed)
                await ctx.Database.CloseConnectionAsync();
        }
    }
}

public class ConnectionSettingsFixture : IAsyncLifetime
{
    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        ConnectionString = await SharedContainer.GetConnectionStringAsync();
    }

    public Task DisposeAsync() => Task.CompletedTask;
}
