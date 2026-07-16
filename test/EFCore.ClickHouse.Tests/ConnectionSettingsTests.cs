using ClickHouse.Driver.ADO;
using ClickHouse.EntityFrameworkCore.Storage.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
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

    [Fact]
    public async Task UseClickHouse_DbConnection_PreservesCodeOnlySettings()
    {
        // Settings that only exist in code (no connection-string equivalent) must survive the
        // join_use_nulls injection performed on Open. Previously
        // the provider rewrote ConnectionString, which rebuilt the driver's settings from the
        // string alone and silently dropped SkipServerCertificateValidation.
        var settings = new ClickHouseClientSettings(_fixture.ConnectionString)
        {
            SkipServerCertificateValidation = true
        };

        await using var connection = new ClickHouseConnection(settings);
        await using var ctx = new MinimalContext(o => o.UseClickHouse(connection));

        Assert.True(await GetJoinUseNullsAsync(ctx));
        Assert.True(connection.Settings.SkipServerCertificateValidation);
    }

    [Fact]
    public async Task UseClickHouse_DbDataSource_PreservesCodeOnlySettings()
    {
        var settings = new ClickHouseClientSettings(_fixture.ConnectionString)
        {
            SkipServerCertificateValidation = true
        };

        await using var dataSource = new ClickHouseDataSource(settings);
        await using var ctx = new MinimalContext(o => o.UseClickHouse(dataSource));

        Assert.True(await GetJoinUseNullsAsync(ctx));

        var connection = (ClickHouseConnection)ctx.Database.GetDbConnection();
        Assert.True(connection.Settings.SkipServerCertificateValidation);
    }

    [Fact]
    public void CreateMasterConnection_DbDataSource_PreservesCodeOnlySettings()
    {
        // The master connection (used for CREATE/DROP DATABASE) must not lose code-only settings
        // by round-tripping through a connection string. Exercises several settings that have no
        // connection-string representation, not just SkipServerCertificateValidation.
        var settings = new ClickHouseClientSettings(_fixture.ConnectionString)
        {
            SkipServerCertificateValidation = true,
            BearerToken = "token-abc",
            CustomHeaders = new Dictionary<string, string> { ["X-Custom"] = "value" }
        };

        using var dataSource = new ClickHouseDataSource(settings);
        using var ctx = new MinimalContext(o => o.UseClickHouse(dataSource));

        var master = ctx.Database.GetService<IClickHouseRelationalConnection>().CreateMasterConnection();
        try
        {
            var masterConnection = (ClickHouseConnection)master.DbConnection;
            Assert.Equal("default", masterConnection.Settings.Database);
            Assert.True(masterConnection.Settings.SkipServerCertificateValidation);
            Assert.Equal("token-abc", masterConnection.Settings.BearerToken);
            Assert.Equal("value", masterConnection.Settings.CustomHeaders["X-Custom"]);
        }
        finally
        {
            master.Dispose();
        }
    }

    [Fact]
    public void CreateMasterConnection_PreservesUserJoinUseNullsOptOut()
    {
        // If the user opted out via set_join_use_nulls=0, the master connection must carry that
        // through so the injection on Open sees it and does not override the choice.
        var optOutConnectionString = _fixture.ConnectionString
            + (_fixture.ConnectionString.EndsWith(';') ? "" : ";")
            + "set_join_use_nulls=0";

        using var connection = new ClickHouseConnection(optOutConnectionString);
        using var ctx = new MinimalContext(o => o.UseClickHouse(connection));

        var master = ctx.Database.GetService<IClickHouseRelationalConnection>().CreateMasterConnection();
        try
        {
            var masterConnection = (ClickHouseConnection)master.DbConnection;
            Assert.Equal("0", masterConnection.Settings.CustomSettings["join_use_nulls"].ToString());
        }
        finally
        {
            master.Dispose();
        }
    }

    [Fact]
    public void CreateMasterConnection_DbConnection_PreservesCodeOnlySettings()
    {
        var settings = new ClickHouseClientSettings(_fixture.ConnectionString)
        {
            SkipServerCertificateValidation = true
        };

        using var connection = new ClickHouseConnection(settings);
        using var ctx = new MinimalContext(o => o.UseClickHouse(connection));

        var master = ctx.Database.GetService<IClickHouseRelationalConnection>().CreateMasterConnection();
        try
        {
            var masterConnection = (ClickHouseConnection)master.DbConnection;
            Assert.Equal("default", masterConnection.Settings.Database);
            Assert.True(masterConnection.Settings.SkipServerCertificateValidation);
        }
        finally
        {
            master.Dispose();
        }
    }

    [Fact]
    public void EnsureJoinUseNulls_NonClickHouseConnection_FallsBackToConnectionString()
    {
        // Defensive fallback: for a DbConnection that is not a ClickHouseConnection we cannot
        // mutate driver settings, so join_use_nulls is injected by rewriting the connection string.
        using var connection = new FakeDbConnection("Host=localhost;Port=8123");
        using var ctx = new MinimalContext(o => o.UseClickHouse(connection));

        ctx.Database.OpenConnection();

        Assert.Contains("set_join_use_nulls=1", connection.ConnectionString, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void EnsureJoinUseNulls_NonClickHouseConnection_LeavesExistingSettingUntouched()
    {
        // If the connection string already configures join_use_nulls, the fallback must not append.
        const string configured = "Host=localhost;Port=8123;set_join_use_nulls=0";
        using var connection = new FakeDbConnection(configured);
        using var ctx = new MinimalContext(o => o.UseClickHouse(connection));

        ctx.Database.OpenConnection();

        Assert.Equal(configured, connection.ConnectionString);
    }

    [Fact]
    public void CreateMasterConnection_NonClickHouseConnection_FallsBackToConnectionString()
    {
        // When neither a ClickHouseDataSource nor a ClickHouseConnection is available, the master
        // connection is built from the connection string.
        using var connection = new FakeDbConnection("Host=localhost;Port=8123;Database=app");
        using var ctx = new MinimalContext(o => o.UseClickHouse(connection));

        var master = ctx.Database.GetService<IClickHouseRelationalConnection>().CreateMasterConnection();
        try
        {
            var masterConnection = (ClickHouseConnection)master.DbConnection;
            Assert.Equal("default", masterConnection.Settings.Database);
        }
        finally
        {
            master.Dispose();
        }
    }

    [Fact]
    public void CreateMasterConnection_NonClickHouseDataSource_UsesDataSourceConnectionString()
    {
        // A non-ClickHouse DbDataSource does not set a connection string on the EF options, so the
        // fallback must read it from the data source itself rather than the (empty) options value.
        using var dataSource = new FakeDbDataSource("Host=localhost;Port=8123;Database=app");
        using var ctx = new MinimalContext(o => o.UseClickHouse(dataSource));

        var master = ctx.Database.GetService<IClickHouseRelationalConnection>().CreateMasterConnection();
        try
        {
            var masterConnection = (ClickHouseConnection)master.DbConnection;
            Assert.Equal("default", masterConnection.Settings.Database);
        }
        finally
        {
            master.Dispose();
        }
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

/// <summary>
/// Minimal non-ClickHouse <see cref="System.Data.Common.DbConnection"/> used to exercise the
/// provider's defensive fallback paths that only run when the underlying connection is not a
/// <see cref="ClickHouseConnection"/>.
/// </summary>
internal sealed class FakeDbConnection : System.Data.Common.DbConnection
{
    private System.Data.ConnectionState _state = System.Data.ConnectionState.Closed;

    public FakeDbConnection(string connectionString) => ConnectionString = connectionString;

    [System.Diagnostics.CodeAnalysis.AllowNull]
    public override string ConnectionString { get; set; }

    public override string Database => "app";

    public override string DataSource => "fake";

    public override string ServerVersion => "0.0";

    public override System.Data.ConnectionState State => _state;

    public override void Open() => _state = System.Data.ConnectionState.Open;

    public override void Close() => _state = System.Data.ConnectionState.Closed;

    public override void ChangeDatabase(string databaseName) { }

    protected override System.Data.Common.DbTransaction BeginDbTransaction(System.Data.IsolationLevel isolationLevel)
        => throw new NotSupportedException();

    protected override System.Data.Common.DbCommand CreateDbCommand()
        => throw new NotSupportedException();
}

/// <summary>
/// Minimal non-ClickHouse <see cref="System.Data.Common.DbDataSource"/> used to verify that
/// <c>CreateMasterConnection</c> falls back to the data source's own connection string.
/// </summary>
internal sealed class FakeDbDataSource : System.Data.Common.DbDataSource
{
    private readonly string _connectionString;

    public FakeDbDataSource(string connectionString) => _connectionString = connectionString;

    public override string ConnectionString => _connectionString;

    protected override System.Data.Common.DbConnection CreateDbConnection()
        => new FakeDbConnection(_connectionString);
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
