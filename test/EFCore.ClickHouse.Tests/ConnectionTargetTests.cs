using System.Data.Common;
using ClickHouse.Driver;
using ClickHouse.Driver.ADO;
using ClickHouse.EntityFrameworkCore.Storage.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

namespace EFCore.ClickHouse.Tests;

/// <summary>
/// Checks which server and database the read connection, the write client and the master
/// connection resolve to. These tests do not connect to a server: creating and opening a
/// <see cref="ClickHouseConnection"/> or a client does not send requests.
/// </summary>
public class ConnectionTargetTests
{
    private const string TargetA = "Host=host-a;Port=8123;Database=db_a";
    private const string TargetB = "Host=host-b;Port=8124;Database=db_b";

    private sealed class TargetContext : DbContext
    {
        private readonly Action<DbContextOptionsBuilder> _configure;

        public TargetContext(Action<DbContextOptionsBuilder> configure)
        {
            _configure = configure;
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => _configure(optionsBuilder);
    }

    private sealed class PooledTargetContext : DbContext
    {
        public PooledTargetContext(DbContextOptions<PooledTargetContext> options)
            : base(options)
        {
        }
    }

    private sealed class WrappingInterceptor : DbConnectionInterceptor
    {
        public override InterceptionResult<DbConnection> ConnectionCreating(
            ConnectionCreatingEventData eventData,
            InterceptionResult<DbConnection> result)
            => InterceptionResult<DbConnection>.SuppressWithResult(new FakeDbConnection(TargetA));
    }

    private static IClickHouseClient WriteClient(DbContext ctx)
        => ctx.GetService<IClickHouseRelationalConnection>().GetClickHouseClient();

    private static ClickHouseClientSettings ReadSettings(DbContext ctx)
        => ((ClickHouseConnection)ctx.Database.GetDbConnection()).Settings;

    private static ClickHouseClientSettings WriteSettings(DbContext ctx)
        => WriteClient(ctx).Settings;

    private static ClickHouseClientSettings MasterSettings(DbContext ctx)
    {
        using var master = ctx.GetService<IClickHouseRelationalConnection>().CreateMasterConnection();
        return ((ClickHouseConnection)master.DbConnection).Settings;
    }

    [Fact]
    public void GetClickHouseClient_DbConnection_CopiesConnectionSettings()
    {
        var settings = new ClickHouseClientSettings(TargetA)
        {
            SkipServerCertificateValidation = true,
            BearerToken = "token-abc",
        };
        using var connection = new ClickHouseConnection(settings);
        using var ctx = new TargetContext(o => o.UseClickHouse(connection));

        var client = WriteClient(ctx);

        Assert.NotSame(connection.Settings, client.Settings);
        Assert.Equal("host-a", client.Settings.Host);
        Assert.Equal(8123, client.Settings.Port);
        Assert.Equal("db_a", client.Settings.Database);
        Assert.True(client.Settings.SkipServerCertificateValidation);
        Assert.Equal("token-abc", client.Settings.BearerToken);
    }

    [Fact]
    public void GetClickHouseClient_DbConnection_ReusesClient()
    {
        using var connection = new ClickHouseConnection(TargetA);
        using var ctx = new TargetContext(o => o.UseClickHouse(connection));

        var first = WriteClient(ctx);
        var second = WriteClient(ctx);

        Assert.Same(first, second);
    }

    [Fact]
    public void GetClickHouseClient_DbConnection_UsesDatabaseOfConnection()
    {
        // Reads send ClickHouseConnection.Database, which includes ChangeDatabase.
        // Inserts send the client database, so the two must agree.
        using var connection = new ClickHouseConnection(TargetA);
        connection.ChangeDatabase("db_changed");
        using var ctx = new TargetContext(o => o.UseClickHouse(connection));

        var first = WriteClient(ctx);
        Assert.Equal("db_changed", first.Settings.Database);

        connection.ChangeDatabase("db_changed_again");
        var second = WriteClient(ctx);

        Assert.NotSame(first, second);
        Assert.Equal("db_changed_again", second.Settings.Database);
    }

    [Fact]
    public void GetClickHouseClient_DbConnection_FollowsReplacedSettings()
    {
        using var connection = new ClickHouseConnection(TargetA);
        using var ctx = new TargetContext(o => o.UseClickHouse(connection));

        var first = WriteClient(ctx);
        connection.Settings = new ClickHouseClientSettings(TargetB);
        var second = WriteClient(ctx);

        Assert.NotSame(first, second);
        Assert.Equal("host-b", second.Settings.Host);
    }

    [Fact]
    public async Task GetClickHouseClient_DbConnection_CreatesNewClientAfterDispose()
    {
        using var connection = new ClickHouseConnection(TargetA);
        using var ctx = new TargetContext(o => o.UseClickHouse(connection));
        var relational = ctx.GetService<IRelationalConnection>();

        var first = WriteClient(ctx);
        relational.Dispose();
        var second = WriteClient(ctx);
        await relational.DisposeAsync();
        var third = WriteClient(ctx);

        Assert.NotSame(first, second);
        Assert.NotSame(second, third);
    }

    [Fact]
    public void GetClickHouseClient_DbConnection_KeepsClientAfterPoolReset()
    {
        using var connection = new ClickHouseConnection(TargetA);
        using var ctx = new TargetContext(o => o.UseClickHouse(connection));

        var first = WriteClient(ctx);
        ((IResettableService)ctx.GetService<IRelationalConnection>()).ResetState();

        Assert.Same(first, WriteClient(ctx));
    }

    [Fact]
    public void GetClickHouseClient_ConnectionString_UsesDataSourceClient()
    {
        using var ctx = new TargetContext(o => o.UseClickHouse(TargetA));

        var client = WriteClient(ctx);

        Assert.Same(client, WriteClient(ctx));
        Assert.Equal("host-a", client.Settings.Host);
    }

    [Fact]
    public void GetClickHouseClient_ConnectionString_UsesChangedDatabase()
    {
        using var ctx = new TargetContext(o => o.UseClickHouse(TargetA));
        var dataSourceClient = WriteClient(ctx);

        ctx.Database.GetDbConnection().ChangeDatabase("db_changed");

        Assert.NotSame(dataSourceClient, WriteClient(ctx));
        Assert.Equal("db_changed", WriteSettings(ctx).Database);
        Assert.Equal("host-a", WriteSettings(ctx).Host);
    }

    [Fact]
    public void GetClickHouseClient_DataSource_KeepsDataSourceClientAfterOpen()
    {
        // Open adds join_use_nulls to a copy of the connection settings. The connection still
        // comes from the data source, so inserts still use the data source client.
        using var dataSource = new ClickHouseDataSource(TargetA);
        using var ctx = new TargetContext(o => o.UseClickHouse(dataSource));

        ctx.Database.OpenConnection();
        ctx.Database.CloseConnection();

        Assert.NotSame(dataSource.Settings, ReadSettings(ctx));
        Assert.Same(dataSource.GetClient(), WriteClient(ctx));
        Assert.Equal("host-a", MasterSettings(ctx).Host);
    }

    [Fact]
    public void GetClickHouseClient_WrappedConnection_UsesDataSourceClient()
    {
        using var ctx = new TargetContext(o => o
            .UseClickHouse(TargetA)
            .AddInterceptors(new WrappingInterceptor()));

        Assert.IsType<FakeDbConnection>(ctx.Database.GetDbConnection());
        Assert.Equal("host-a", WriteSettings(ctx).Host);
    }

    [Fact]
    public void GetClickHouseClient_NonClickHouseConnection_Throws()
    {
        using var connection = new FakeDbConnection(TargetA);
        using var ctx = new TargetContext(o => o.UseClickHouse(connection));

        var ex = Assert.Throws<InvalidOperationException>(() => WriteClient(ctx));
        Assert.Contains(nameof(FakeDbConnection), ex.Message);
    }

    [Fact]
    public void GetClickHouseClient_NonClickHouseDataSource_Throws()
    {
        using var dataSource = new FakeDbDataSource(TargetA);
        using var ctx = new TargetContext(o => o.UseClickHouse(dataSource));

        var ex = Assert.Throws<InvalidOperationException>(() => WriteClient(ctx));
        Assert.Contains(nameof(FakeDbConnection), ex.Message);
    }

    [Fact]
    public void SetConnectionString_BeforeFirstUse_ConnectionUsesNewTarget()
    {
        using var ctx = new TargetContext(o => o.UseClickHouse(TargetA));

        ctx.Database.SetConnectionString(TargetB);

        Assert.Equal("host-b", ReadSettings(ctx).Host);
        Assert.Equal("host-b", WriteSettings(ctx).Host);
        Assert.Equal("host-b", MasterSettings(ctx).Host);
    }

    [Fact]
    public void SetConnectionString_AfterFirstUse_WritesAndMasterUseNewTarget()
    {
        using var ctx = new TargetContext(o => o.UseClickHouse(TargetA));
        Assert.Equal("host-a", ReadSettings(ctx).Host);
        Assert.Equal("host-a", WriteSettings(ctx).Host);

        ctx.Database.SetConnectionString(TargetB);

        Assert.Equal("host-b", ReadSettings(ctx).Host);
        Assert.Equal("host-b", WriteSettings(ctx).Host);
        Assert.Equal("db_b", WriteSettings(ctx).Database);
        Assert.Equal("host-b", MasterSettings(ctx).Host);
    }

    [Fact]
    public void SetConnectionString_UsesSharedDataSourceForNewTarget()
    {
        using var first = new TargetContext(o => o.UseClickHouse(TargetA));
        using var second = new TargetContext(o => o.UseClickHouse(TargetB));

        first.Database.SetConnectionString(TargetB);

        Assert.Same(WriteClient(second), WriteClient(first));
    }

    [Fact]
    public void SetConnectionString_DataSource_DropsDataSource()
    {
        using var dataSource = new ClickHouseDataSource(TargetA);
        using var ctx = new TargetContext(o => o.UseClickHouse(dataSource));

        ctx.Database.SetConnectionString(TargetB);

        Assert.Equal("host-b", ReadSettings(ctx).Host);
        Assert.Equal("host-b", WriteSettings(ctx).Host);
        Assert.NotSame(dataSource.GetClient(), WriteClient(ctx));
        Assert.Equal("host-b", MasterSettings(ctx).Host);
    }

    [Fact]
    public void SetConnectionString_DbConnection_WritesAndMasterUseNewTarget()
    {
        using var connection = new ClickHouseConnection(TargetA);
        using var ctx = new TargetContext(o => o.UseClickHouse(connection));
        Assert.Equal("host-a", WriteSettings(ctx).Host);

        ctx.Database.SetConnectionString(TargetB);

        Assert.Equal("host-b", connection.Settings.Host);
        Assert.Equal("host-b", WriteSettings(ctx).Host);
        Assert.Equal("host-b", MasterSettings(ctx).Host);
    }

    [Fact]
    public void SetConnectionString_DbConnection_KeepsChangedDatabase()
    {
        // The driver keeps the ChangeDatabase value when the connection string changes.
        using var connection = new ClickHouseConnection(TargetA);
        connection.ChangeDatabase("db_changed");
        using var ctx = new TargetContext(o => o.UseClickHouse(connection));

        ctx.Database.SetConnectionString(TargetB);

        Assert.Equal("db_changed", connection.Database);
        Assert.Equal("host-b", WriteSettings(ctx).Host);
        Assert.Equal("db_changed", WriteSettings(ctx).Database);
    }

    [Fact]
    public void SetConnectionString_DbConnection_SameString_KeepsCodeOnlySettings()
    {
        // The base setter does not change a connection that already has the string, so the
        // connection keeps its code-only settings, and inserts and the master connection must too.
        var settings = new ClickHouseClientSettings(TargetA)
        {
            SkipServerCertificateValidation = true,
            BearerToken = "token-abc",
        };
        using var connection = new ClickHouseConnection(settings);
        using var ctx = new TargetContext(o => o.UseClickHouse(connection));

        ctx.Database.SetConnectionString(connection.ConnectionString);

        Assert.Equal("token-abc", connection.Settings.BearerToken);
        Assert.Equal("token-abc", WriteSettings(ctx).BearerToken);
        Assert.True(WriteSettings(ctx).SkipServerCertificateValidation);
        Assert.Equal("token-abc", MasterSettings(ctx).BearerToken);
        Assert.True(MasterSettings(ctx).SkipServerCertificateValidation);
    }

    [Fact]
    public void SetDbConnection_ConnectionString_WritesAndMasterUseNewConnection()
    {
        using var ctx = new TargetContext(o => o.UseClickHouse(TargetA));
        Assert.Equal("host-a", WriteSettings(ctx).Host);

        using var connection = new ClickHouseConnection(TargetB);
        ctx.Database.SetDbConnection(connection);

        Assert.Equal("host-b", ReadSettings(ctx).Host);
        Assert.Equal("host-b", WriteSettings(ctx).Host);
        Assert.Equal("host-b", MasterSettings(ctx).Host);
    }

    [Fact]
    public void SetDbConnection_DataSource_WritesAndMasterUseNewConnection()
    {
        using var dataSource = new ClickHouseDataSource(TargetA);
        using var ctx = new TargetContext(o => o.UseClickHouse(dataSource));
        Assert.Equal("host-a", WriteSettings(ctx).Host);

        using var connection = new ClickHouseConnection(TargetB);
        ctx.Database.SetDbConnection(connection);

        Assert.Equal("host-b", ReadSettings(ctx).Host);
        Assert.Equal("host-b", WriteSettings(ctx).Host);
        Assert.Equal("host-b", MasterSettings(ctx).Host);
    }

    [Fact]
    public void SetDbConnection_DbConnection_ReplacesWriteClient()
    {
        using var first = new ClickHouseConnection(TargetA);
        using var ctx = new TargetContext(o => o.UseClickHouse(first));
        var firstClient = WriteClient(ctx);

        using var second = new ClickHouseConnection(TargetB);
        ctx.Database.SetDbConnection(second);

        var secondClient = WriteClient(ctx);
        Assert.NotSame(firstClient, secondClient);
        Assert.Equal("host-b", secondClient.Settings.Host);
    }

    [Fact]
    public void SetDbConnection_Null_ThenSetConnectionString_UsesNewTarget()
    {
        using var ctx = new TargetContext(o => o.UseClickHouse(TargetA));
        Assert.Equal("host-a", ReadSettings(ctx).Host);

        ctx.Database.SetDbConnection(null);
        ctx.Database.SetConnectionString(TargetB);

        Assert.Equal("host-b", ReadSettings(ctx).Host);
        Assert.Equal("host-b", WriteSettings(ctx).Host);
        Assert.Equal("host-b", MasterSettings(ctx).Host);
    }

    [Fact]
    public void PooledContext_KeepsReadAndWriteTargetsTogetherAfterReset()
    {
        var options = new DbContextOptionsBuilder<PooledTargetContext>().UseClickHouse(TargetA).Options;
        var factory = new PooledDbContextFactory<PooledTargetContext>(options);

        PooledTargetContext leased;
        using (var ctx = factory.CreateDbContext())
        {
            leased = ctx;
            Assert.Equal("host-a", ReadSettings(ctx).Host);
            ctx.Database.SetConnectionString(TargetB);
        }

        using (var ctx = factory.CreateDbContext())
        {
            Assert.Same(leased, ctx);
            Assert.Equal(ReadSettings(ctx).Host, WriteSettings(ctx).Host);
            Assert.Equal(ReadSettings(ctx).Host, MasterSettings(ctx).Host);
        }
    }
}
