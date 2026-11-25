using System.Data.Common;
using ClickHouse.Driver.ADO;
using ClickHouse.EntityFrameworkCore.Storage.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

namespace EFCore.ClickHouse.Tests;

/// <summary>
/// Tests for the connection settings on the three <c>UseClickHouse</c> overloads: connection
/// string, <see cref="DbConnection"/> and <see cref="DbDataSource"/>. The provider must send
/// <c>join_use_nulls=1</c> with the queries that EF Core runs, keep settings that have no
/// connection-string key, and not give a connection a new driver client.
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

    public enum ConfigurationPath
    {
        ConnectionString,
        DbConnection,
        DbDataSource
    }

    /// <summary>
    /// Makes a context for one configuration path. The returned disposable owns the connection or
    /// data source that the test gives to the context.
    /// </summary>
    private static (MinimalContext Context, IAsyncDisposable? Owned) CreateContext(
        ConfigurationPath path,
        string connectionString,
        Action<global::ClickHouse.EntityFrameworkCore.Infrastructure.ClickHouseDbContextOptionsBuilder>? clickHouseOptions = null,
        Action<DbContextOptionsBuilder>? configure = null)
    {
        switch (path)
        {
            case ConfigurationPath.ConnectionString:
                return (new MinimalContext(o =>
                {
                    o.UseClickHouse(connectionString, clickHouseOptions);
                    configure?.Invoke(o);
                }), null);

            case ConfigurationPath.DbConnection:
            {
                var connection = new ClickHouseConnection(connectionString);
                return (new MinimalContext(o =>
                {
                    o.UseClickHouse(connection, clickHouseOptions);
                    configure?.Invoke(o);
                }), connection);
            }

            default:
            {
                var dataSource = new ClickHouseDataSource(connectionString);
                return (new MinimalContext(o =>
                {
                    o.UseClickHouse(dataSource, clickHouseOptions);
                    configure?.Invoke(o);
                }), dataSource);
            }
        }
    }

    [Theory]
    [InlineData(ConfigurationPath.ConnectionString)]
    [InlineData(ConfigurationPath.DbConnection)]
    [InlineData(ConfigurationPath.DbDataSource)]
    public async Task UseClickHouse_AppliesJoinUseNulls(ConfigurationPath path)
    {
        var (ctx, owned) = CreateContext(path, _fixture.ConnectionString);
        await using (owned)
        await using (ctx)
        {
            // The second query runs on the command that the first query returned to the connection.
            Assert.True(await GetJoinUseNullsAsync(ctx));
            Assert.True(await GetJoinUseNullsAsync(ctx));
        }
    }

    [Theory]
    [InlineData(ConfigurationPath.ConnectionString)]
    [InlineData(ConfigurationPath.DbConnection)]
    [InlineData(ConfigurationPath.DbDataSource)]
    public async Task UseClickHouse_LeftJoinReturnsNullForMissingRow(ConfigurationPath path)
    {
        var (ctx, owned) = CreateContext(path, _fixture.ConnectionString);
        await using (owned)
        await using (ctx)
        {
            Assert.Null(await GetLeftJoinValueAsync(ctx));
        }
    }

    [Theory]
    [InlineData(ConfigurationPath.ConnectionString)]
    [InlineData(ConfigurationPath.DbConnection)]
    [InlineData(ConfigurationPath.DbDataSource)]
    public async Task DisableJoinNullSemantics_SkipsJoinUseNulls(ConfigurationPath path)
    {
        // Used when a ClickHouse server or profile forbids a change to this setting.
        var (ctx, owned) = CreateContext(path, _fixture.ConnectionString, ch => ch.DisableJoinNullSemantics());
        await using (owned)
        await using (ctx)
        {
            // The server default is 0, so the LEFT JOIN returns the column default.
            Assert.False(await GetJoinUseNullsAsync(ctx));
            Assert.Equal(0, await GetLeftJoinValueAsync(ctx));
        }
    }

    [Theory]
    [InlineData(ConfigurationPath.ConnectionString)]
    [InlineData(ConfigurationPath.DbConnection)]
    [InlineData(ConfigurationPath.DbDataSource)]
    public async Task UseClickHouse_AppliesJoinUseNulls_WhenAnInterceptorWrapsCommands(ConfigurationPath path)
    {
        // A profiler can replace each command with a wrapper in CommandCreated. The provider must
        // add the settings to the driver command before that happens.
        var interceptor = new WrappingCommandInterceptor();
        var (ctx, owned) = CreateContext(path, _fixture.ConnectionString, configure: o => o.AddInterceptors(interceptor));
        await using (owned)
        await using (ctx)
        {
            Assert.True(await GetJoinUseNullsAsync(ctx));
            Assert.Null(await GetLeftJoinValueAsync(ctx));
            Assert.True(interceptor.WrappedCommands > 0);
        }
    }

    [Fact]
    public async Task RelationalCommand_WithoutLogger_AppliesJoinUseNulls()
    {
        // Code that runs an IRelationalCommand without a logger skips the interceptors, so the
        // command class must add the settings itself.
        await using var dataSource = new ClickHouseDataSource(_fixture.ConnectionString);
        await using var ctx = new MinimalContext(o => o.UseClickHouse(dataSource));

        var connection = ctx.GetService<IRelationalConnection>();
        var command = ctx.GetService<IRelationalCommandBuilderFactory>()
            .Create()
            .Append("SELECT getSetting('join_use_nulls')")
            .Build();

        await connection.OpenAsync(CancellationToken.None);
        try
        {
            var result = await command.ExecuteScalarAsync(
                new RelationalCommandParameterObject(connection, null, null, context: null, logger: null));
            Assert.True(result is bool b ? b : bool.Parse(result?.ToString() ?? "false"));
        }
        finally
        {
            await connection.CloseAsync();
        }
    }

    [Theory]
    [InlineData(ConfigurationPath.ConnectionString)]
    [InlineData(ConfigurationPath.DbConnection)]
    [InlineData(ConfigurationPath.DbDataSource)]
    public async Task UseClickHouse_KeepsUserJoinUseNullsValue(ConfigurationPath path)
    {
        // A value that the user sets in the connection settings must win.
        var (ctx, owned) = CreateContext(path, WithSetting(_fixture.ConnectionString, "set_join_use_nulls=0"));
        await using (owned)
        await using (ctx)
        {
            Assert.False(await GetJoinUseNullsAsync(ctx));
        }
    }

    [Fact]
    public async Task UseClickHouse_DbConnection_DoesNotChangeConnection()
    {
        var settings = new ClickHouseClientSettings(_fixture.ConnectionString)
        {
            SkipServerCertificateValidation = true
        };

        await using var connection = new ClickHouseConnection(settings);
        var settingsBefore = connection.Settings;

        await using var ctx = new MinimalContext(o => o.UseClickHouse(connection));
        Assert.True(await GetJoinUseNullsAsync(ctx));

        // The same settings object means the same driver client and HTTP connection pool.
        Assert.Same(settingsBefore, connection.Settings);
        Assert.True(connection.Settings.SkipServerCertificateValidation);
        Assert.False(connection.Settings.CustomSettings.ContainsKey("join_use_nulls"));

        // A command that the application makes on its own connection gets no provider setting.
        Assert.False(await GetJoinUseNullsThroughConnectionAsync(connection));
    }

    [Fact]
    public async Task UseClickHouse_DbDataSource_ConnectionUsesDataSourceClient()
    {
        var settings = new ClickHouseClientSettings(_fixture.ConnectionString)
        {
            SkipServerCertificateValidation = true
        };

        await using var dataSource = new ClickHouseDataSource(settings);
        await using var ctx = new MinimalContext(o => o.UseClickHouse(dataSource));

        Assert.True(await GetJoinUseNullsAsync(ctx));

        // The same settings object means the connection still uses the data source client, so the
        // context does not make an HTTP connection pool of its own.
        var connection = (ClickHouseConnection)ctx.Database.GetDbConnection();
        Assert.Same(dataSource.Settings, connection.Settings);
        Assert.True(connection.Settings.SkipServerCertificateValidation);
    }

    [Fact]
    public void CreateMasterConnection_DbDataSource_UsesDataSourceClient()
    {
        // The master connection runs CREATE DATABASE and DROP DATABASE. It must keep the settings
        // that have no connection-string key.
        var settings = new ClickHouseClientSettings(_fixture.ConnectionString)
        {
            SkipServerCertificateValidation = true,
            BearerToken = "token-abc",
            CustomHeaders = new Dictionary<string, string> { ["X-Custom"] = "value" }
        };

        using var dataSource = new ClickHouseDataSource(settings);
        using var ctx = new MinimalContext(o => o.UseClickHouse(dataSource));

        using var master = ctx.Database.GetService<IClickHouseRelationalConnection>().CreateMasterConnection();
        var masterConnection = (ClickHouseConnection)master.DbConnection;

        Assert.Equal("default", masterConnection.Database);
        Assert.Same(dataSource.Settings, masterConnection.Settings);
        Assert.True(masterConnection.Settings.SkipServerCertificateValidation);
        Assert.Equal("token-abc", masterConnection.Settings.BearerToken);
        Assert.Equal("value", masterConnection.Settings.CustomHeaders["X-Custom"]);
    }

    [Fact]
    public void CreateMasterConnection_DbConnection_UsesConnectionClient()
    {
        var settings = new ClickHouseClientSettings(_fixture.ConnectionString)
        {
            SkipServerCertificateValidation = true
        };

        using var connection = new ClickHouseConnection(settings);
        using var ctx = new MinimalContext(o => o.UseClickHouse(connection));

        using var master = ctx.Database.GetService<IClickHouseRelationalConnection>().CreateMasterConnection();
        var masterConnection = (ClickHouseConnection)master.DbConnection;

        Assert.Equal("default", masterConnection.Database);
        Assert.Same(connection.Settings, masterConnection.Settings);
        Assert.NotEqual("default", connection.Database);
    }

    [Fact]
    public void CreateMasterConnection_ConnectionString_UsesProviderClient()
    {
        using var ctx = new MinimalContext(o => o.UseClickHouse(_fixture.ConnectionString));
        var connection = (ClickHouseConnection)ctx.Database.GetDbConnection();

        using var master = ctx.Database.GetService<IClickHouseRelationalConnection>().CreateMasterConnection();
        var masterConnection = (ClickHouseConnection)master.DbConnection;

        Assert.Equal("default", masterConnection.Database);
        Assert.Same(connection.Settings, masterConnection.Settings);
    }

    [Fact]
    public void CreateMasterConnection_KeepsUserJoinUseNullsValue()
    {
        using var connection = new ClickHouseConnection(
            WithSetting(_fixture.ConnectionString, "set_join_use_nulls=0"));
        using var ctx = new MinimalContext(o => o.UseClickHouse(connection));

        using var master = ctx.Database.GetService<IClickHouseRelationalConnection>().CreateMasterConnection();
        var masterConnection = (ClickHouseConnection)master.DbConnection;

        Assert.Equal("0", masterConnection.Settings.CustomSettings["join_use_nulls"].ToString());
    }

    [Theory]
    [InlineData(ConfigurationPath.ConnectionString)]
    [InlineData(ConfigurationPath.DbConnection)]
    [InlineData(ConfigurationPath.DbDataSource)]
    public async Task EnsureCreated_And_EnsureDeleted_UseMasterConnection(ConfigurationPath path)
    {
        // The database does not exist yet, so CREATE DATABASE must run on the master connection.
        var databaseName = $"master_{path}_{Guid.NewGuid():N}".ToLowerInvariant();
        var connectionString = WithDatabase(_fixture.ConnectionString, databaseName);

        var (ctx, owned) = CreateContext(path, connectionString);
        await using (owned)
        await using (ctx)
        {
            Assert.True(await ctx.Database.EnsureCreatedAsync());
            Assert.True(await DatabaseExistsAsync(databaseName));

            Assert.True(await ctx.Database.EnsureDeletedAsync());
            Assert.False(await DatabaseExistsAsync(databaseName));
        }
    }

    [Fact]
    public void EnsureJoinUseNulls_NonClickHouseConnection_FallsBackToConnectionString()
    {
        // For a DbConnection that is not a ClickHouseConnection, the provider cannot get to the
        // driver commands, so it adds join_use_nulls to the connection string.
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
    public void ApplyQuerySettings_NonClickHouseCommand_IsIgnored()
    {
        using var connection = new FakeDbConnection("Host=localhost;Port=8123");
        using var ctx = new MinimalContext(o => o.UseClickHouse(connection));

        // Must not throw: a wrapper command gets the setting from the connection string instead.
        ctx.Database.GetService<IClickHouseRelationalConnection>().ApplyQuerySettings(new FakeDbCommand());
    }

    [Fact]
    public void CreateMasterConnection_NonClickHouseConnection_FallsBackToConnectionString()
    {
        // When the connection is not a ClickHouseConnection, the master connection is built from
        // the connection string.
        using var connection = new FakeDbConnection("Host=localhost;Port=8123;Database=app");
        using var ctx = new MinimalContext(o => o.UseClickHouse(connection));

        using var master = ctx.Database.GetService<IClickHouseRelationalConnection>().CreateMasterConnection();
        var masterConnection = (ClickHouseConnection)master.DbConnection;
        Assert.Equal("default", masterConnection.Database);
    }

    [Fact]
    public void CreateMasterConnection_NonClickHouseDataSource_UsesDataSourceConnectionString()
    {
        // A non-ClickHouse DbDataSource does not set a connection string on the EF options, so the
        // fallback must read it from the data source itself rather than the (empty) options value.
        using var dataSource = new FakeDbDataSource("Host=localhost;Port=8123;Database=app");
        using var ctx = new MinimalContext(o => o.UseClickHouse(dataSource));

        using var master = ctx.Database.GetService<IClickHouseRelationalConnection>().CreateMasterConnection();
        var masterConnection = (ClickHouseConnection)master.DbConnection;
        Assert.Equal("default", masterConnection.Database);
    }

    /// <summary>
    /// Reads <c>join_use_nulls</c> through an EF Core query, which is the path that gets the
    /// provider setting.
    /// </summary>
    private static async Task<bool> GetJoinUseNullsAsync(DbContext ctx)
        => (await ctx.Database
                .SqlQueryRaw<bool>("SELECT getSetting('join_use_nulls') AS Value")
                .ToListAsync())
            .Single();

    /// <summary>
    /// Runs a LEFT JOIN through EF Core that has no matching row on the right side.
    /// </summary>
    private static async Task<int?> GetLeftJoinValueAsync(DbContext ctx)
        => (await ctx.Database
                .SqlQueryRaw<int?>(
                    "SELECT r.x AS Value FROM (SELECT 1 AS id) AS l "
                    + "LEFT JOIN (SELECT 2 AS id, toInt32(5) AS x) AS r ON l.id = r.id")
                .ToListAsync())
            .Single();

    private static async Task<bool> GetJoinUseNullsThroughConnectionAsync(DbConnection connection)
    {
        var wasClosed = connection.State != System.Data.ConnectionState.Open;
        if (wasClosed)
            await connection.OpenAsync();

        try
        {
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT getSetting('join_use_nulls')";
            var result = await cmd.ExecuteScalarAsync();
            return result is bool b ? b : bool.Parse(result?.ToString() ?? "false");
        }
        finally
        {
            if (wasClosed)
                await connection.CloseAsync();
        }
    }

    private async Task<bool> DatabaseExistsAsync(string databaseName)
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"EXISTS DATABASE `{databaseName}`";
        return Convert.ToInt32(await cmd.ExecuteScalarAsync()) == 1;
    }

    private static string WithSetting(string connectionString, string setting)
        => connectionString + (connectionString.EndsWith(';') ? "" : ";") + setting;

    private static string WithDatabase(string connectionString, string databaseName)
        => new ClickHouseConnectionStringBuilder(connectionString) { Database = databaseName }.ConnectionString;
}

/// <summary>
/// Minimal non-ClickHouse <see cref="DbConnection"/> used to exercise the provider's fallback
/// paths that only run when the underlying connection is not a <see cref="ClickHouseConnection"/>.
/// </summary>
internal sealed class FakeDbConnection : DbConnection
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

    protected override DbTransaction BeginDbTransaction(System.Data.IsolationLevel isolationLevel)
        => throw new NotSupportedException();

    protected override DbCommand CreateDbCommand()
        => throw new NotSupportedException();
}

/// <summary>
/// Minimal non-ClickHouse <see cref="DbCommand"/>, for example a command that a profiler wraps.
/// </summary>
internal sealed class FakeDbCommand : DbCommand
{
    [System.Diagnostics.CodeAnalysis.AllowNull]
    public override string CommandText { get; set; }

    public override int CommandTimeout { get; set; }

    public override System.Data.CommandType CommandType { get; set; }

    public override bool DesignTimeVisible { get; set; }

    public override System.Data.UpdateRowSource UpdatedRowSource { get; set; }

    protected override DbConnection? DbConnection { get; set; }

    protected override DbParameterCollection DbParameterCollection => throw new NotSupportedException();

    protected override DbTransaction? DbTransaction { get; set; }

    public override void Cancel() { }

    public override int ExecuteNonQuery() => throw new NotSupportedException();

    public override object ExecuteScalar() => throw new NotSupportedException();

    public override void Prepare() { }

    protected override DbParameter CreateDbParameter() => throw new NotSupportedException();

    protected override DbDataReader ExecuteDbDataReader(System.Data.CommandBehavior behavior)
        => throw new NotSupportedException();
}

/// <summary>
/// Replaces each command with a <see cref="WrappedDbCommand"/>, as a profiler does.
/// </summary>
internal sealed class WrappingCommandInterceptor : Microsoft.EntityFrameworkCore.Diagnostics.DbCommandInterceptor
{
    public int WrappedCommands { get; private set; }

    public override DbCommand CommandCreated(
        Microsoft.EntityFrameworkCore.Diagnostics.CommandEndEventData eventData, DbCommand result)
    {
        WrappedCommands++;
        return new WrappedDbCommand(result);
    }
}

/// <summary>
/// A <see cref="DbCommand"/> that sends every call to the command it wraps.
/// </summary>
internal sealed class WrappedDbCommand(DbCommand inner) : DbCommand
{
    [System.Diagnostics.CodeAnalysis.AllowNull]
    public override string CommandText { get => inner.CommandText; set => inner.CommandText = value; }

    public override int CommandTimeout { get => inner.CommandTimeout; set => inner.CommandTimeout = value; }

    public override System.Data.CommandType CommandType { get => inner.CommandType; set => inner.CommandType = value; }

    public override bool DesignTimeVisible { get => inner.DesignTimeVisible; set => inner.DesignTimeVisible = value; }

    public override System.Data.UpdateRowSource UpdatedRowSource
    {
        get => inner.UpdatedRowSource;
        set => inner.UpdatedRowSource = value;
    }

    protected override DbConnection? DbConnection { get => inner.Connection; set => inner.Connection = value; }

    protected override DbParameterCollection DbParameterCollection => inner.Parameters;

    protected override DbTransaction? DbTransaction { get => inner.Transaction; set => inner.Transaction = value; }

    public override void Cancel() => inner.Cancel();

    public override int ExecuteNonQuery() => inner.ExecuteNonQuery();

    public override object? ExecuteScalar() => inner.ExecuteScalar();

    public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        => inner.ExecuteNonQueryAsync(cancellationToken);

    public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
        => inner.ExecuteScalarAsync(cancellationToken);

    public override void Prepare() => inner.Prepare();

    protected override DbParameter CreateDbParameter() => inner.CreateParameter();

    protected override DbDataReader ExecuteDbDataReader(System.Data.CommandBehavior behavior)
        => inner.ExecuteReader(behavior);

    protected override Task<DbDataReader> ExecuteDbDataReaderAsync(
        System.Data.CommandBehavior behavior, CancellationToken cancellationToken)
        => inner.ExecuteReaderAsync(behavior, cancellationToken);

    protected override void Dispose(bool disposing)
    {
        if (disposing)
            inner.Dispose();

        base.Dispose(disposing);
    }
}

/// <summary>
/// Minimal non-ClickHouse <see cref="DbDataSource"/> used to verify that
/// <c>CreateMasterConnection</c> falls back to the data source's own connection string.
/// </summary>
internal sealed class FakeDbDataSource : DbDataSource
{
    private readonly string _connectionString;

    public FakeDbDataSource(string connectionString) => _connectionString = connectionString;

    public override string ConnectionString => _connectionString;

    protected override DbConnection CreateDbConnection()
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
