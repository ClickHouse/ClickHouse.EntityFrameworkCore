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
/// Checks which server and database the read connection, the insert client and the master
/// connection resolve to. Inserts use the client of the current connection and the database of
/// that connection. These tests do not connect to a server: creating and opening a
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

    private sealed class WrapCreatedConnectionInterceptor : DbConnectionInterceptor
    {
        public override DbConnection ConnectionCreated(ConnectionCreatedEventData eventData, DbConnection result)
            => new WrappedDbConnection(result);
    }

    private static IClickHouseClient WriteClient(DbContext ctx)
        => ctx.GetService<IClickHouseRelationalConnection>().GetClickHouseClient();

    private static ClickHouseClientSettings ReadSettings(DbContext ctx)
        => ((ClickHouseConnection)ctx.Database.GetDbConnection()).Settings;

    private static ClickHouseClientSettings WriteSettings(DbContext ctx)
        => WriteClient(ctx).Settings;

    // The database that an insert goes to: the insert options database, or else the client database.
    private static string WriteDatabase(DbContext ctx)
    {
        var connection = ctx.GetService<IClickHouseRelationalConnection>();
        var client = connection.GetClickHouseClient();
        return connection.CreateInsertOptions(client)?.Database ?? client.Settings.Database;
    }

    private static IClickHouseClient ReadClient(DbContext ctx)
        => ((ClickHouseConnection)ctx.Database.GetDbConnection()).GetClient();

    private static ClickHouseClientSettings MasterSettings(DbContext ctx)
    {
        using var master = ctx.GetService<IClickHouseRelationalConnection>().CreateMasterConnection();
        return ((ClickHouseConnection)master.DbConnection).Settings;
    }

    [Fact]
    public void GetClickHouseClient_DbConnection_UsesConnectionClient()
    {
        var settings = new ClickHouseClientSettings(TargetA)
        {
            SkipServerCertificateValidation = true,
            BearerToken = "token-abc",
        };
        using var connection = new ClickHouseConnection(settings);
        using var ctx = new TargetContext(o => o.UseClickHouse(connection));

        var client = WriteClient(ctx);

        Assert.Same(connection.GetClient(), client);
        Assert.Same(client, WriteClient(ctx));
        Assert.Same(connection.Settings, client.Settings);
        Assert.True(client.Settings.SkipServerCertificateValidation);
        Assert.Equal("token-abc", client.Settings.BearerToken);
    }

    [Fact]
    public void GetClickHouseClient_DbConnection_UsesDatabaseOfConnection()
    {
        // Queries send ClickHouseConnection.Database, which includes ChangeDatabase, and inserts
        // must use the same database.
        using var connection = new ClickHouseConnection(TargetA);
        connection.ChangeDatabase("db_changed");
        using var ctx = new TargetContext(o => o.UseClickHouse(connection));

        Assert.Equal("db_changed", WriteDatabase(ctx));

        connection.ChangeDatabase("db_changed_again");

        Assert.Equal("db_changed_again", WriteDatabase(ctx));
        Assert.Same(connection.GetClient(), WriteClient(ctx));
    }

    [Fact]
    public void CreateInsertOptions_DatabaseOfClient_ReturnsNoDatabase()
    {
        using var connection = new ClickHouseConnection(TargetA);
        using var ctx = new TargetContext(o => o.UseClickHouse(connection, ch => ch.DisableJoinNullSemantics()));
        var relational = ctx.GetService<IClickHouseRelationalConnection>();

        Assert.Null(relational.CreateInsertOptions(relational.GetClickHouseClient()));
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
        Assert.Same(connection.GetClient(), second);
        Assert.Equal("host-b", second.Settings.Host);
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

        Assert.Same(ReadClient(ctx), client);
        Assert.Same(client, WriteClient(ctx));
        Assert.Equal("host-a", client.Settings.Host);
    }

    [Fact]
    public void GetClickHouseClient_ConnectionString_UsesChangedDatabase()
    {
        using var ctx = new TargetContext(o => o.UseClickHouse(TargetA));
        var dataSourceClient = WriteClient(ctx);

        ctx.Database.GetDbConnection().ChangeDatabase("db_changed");

        Assert.Same(dataSourceClient, WriteClient(ctx));
        Assert.Equal("db_changed", WriteDatabase(ctx));
        Assert.Equal("host-a", WriteSettings(ctx).Host);
    }

    [Fact]
    public void GetClickHouseClient_DataSource_KeepsDataSourceClientAfterOpen()
    {
        // Open does not change the connection settings, so the connection, the inserts and the
        // master connection all use the data source client.
        using var dataSource = new ClickHouseDataSource(TargetA);
        using var ctx = new TargetContext(o => o.UseClickHouse(dataSource));

        ctx.Database.OpenConnection();
        ctx.Database.CloseConnection();

        Assert.Same(dataSource.Settings, ReadSettings(ctx));
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

        var before = ctx.Database.GetDbConnection();

        ctx.Database.SetConnectionString(TargetB);

        // The closed connection from the data source is replaced by a connection from the data
        // source for the new string, so reads and inserts share its client.
        Assert.NotSame(before, ctx.Database.GetDbConnection());
        Assert.Equal("host-b", ReadSettings(ctx).Host);
        Assert.Same(ReadClient(ctx), WriteClient(ctx));
        Assert.Equal("db_b", WriteDatabase(ctx));
        Assert.Equal("host-b", MasterSettings(ctx).Host);
    }

    [Fact]
    public void SetConnectionString_SameString_KeepsSharedClient()
    {
        // EF Core compares the string with the normalized string of the driver connection, so the
        // same string would otherwise rebuild the client of the connection.
        using var ctx = new TargetContext(o => o.UseClickHouse(TargetA));
        var client = ReadClient(ctx);

        ctx.Database.SetConnectionString(TargetA);

        Assert.Same(client, ReadClient(ctx));
        Assert.Same(client, WriteClient(ctx));
    }

    [Fact]
    public void SetConnectionString_OpenConnection_ThrowsAndKeepsTarget()
    {
        // The driver does not change the settings of an open connection, and EF Core lets it throw.
        using var ctx = new TargetContext(o => o.UseClickHouse(TargetA));
        ctx.Database.OpenConnection();
        var connection = ctx.Database.GetDbConnection();

        Assert.Throws<InvalidOperationException>(() => ctx.Database.SetConnectionString(TargetB));

        Assert.Same(connection, ctx.Database.GetDbConnection());
        Assert.Equal("host-a", ReadSettings(ctx).Host);
        Assert.Same(ReadClient(ctx), WriteClient(ctx));
        ctx.Database.CloseConnection();
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
    public void SetConnectionString_DbConnection_InsertsUseDatabaseOfConnection()
    {
        // EF Core writes the string into the connection of the user. Whichever database the driver
        // then selects, inserts must use the database that queries use.
        using var connection = new ClickHouseConnection(TargetA);
        connection.ChangeDatabase("db_changed");
        using var ctx = new TargetContext(o => o.UseClickHouse(connection));

        ctx.Database.SetConnectionString(TargetB);

        Assert.Equal("host-b", WriteSettings(ctx).Host);
        Assert.Equal(connection.Database, WriteDatabase(ctx));
    }

    [Fact]
    public void SetConnectionString_KeepsCommandTimeout()
    {
        using var ctx = new TargetContext(o => o.UseClickHouse(TargetA));
        ctx.Database.SetCommandTimeout(123);
        Assert.Equal("host-a", ReadSettings(ctx).Host);

        ctx.Database.SetConnectionString(TargetB);

        Assert.Equal("host-b", ReadSettings(ctx).Host);
        Assert.Equal(123, ctx.Database.GetCommandTimeout());
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
        Assert.Same(second.GetClient(), secondClient);
        Assert.Equal("host-b", secondClient.Settings.Host);
    }

    [Fact]
    public void SetDbConnection_WrapperConnection_DropsDataSource()
    {
        // A wrapper cannot give its client. Inserts must fail, not go to the old data source.
        using var ctx = new TargetContext(o => o.UseClickHouse(TargetA));
        Assert.Equal("host-a", WriteSettings(ctx).Host);

        using var wrapper = new WrappedDbConnection(new ClickHouseConnection(TargetB));
        ctx.Database.SetDbConnection(wrapper);

        Assert.Throws<InvalidOperationException>(() => WriteClient(ctx));
    }

    [Fact]
    public void SetDbConnection_ThenSetConnectionString_WritesStringIntoThatConnection()
    {
        using var ctx = new TargetContext(o => o.UseClickHouse(TargetA));
        Assert.Equal("host-a", ReadSettings(ctx).Host);

        using var connection = new ClickHouseConnection(TargetA);
        ctx.Database.SetDbConnection(connection);
        ctx.Database.SetConnectionString(TargetB);

        Assert.Same(connection, ctx.Database.GetDbConnection());
        Assert.Equal("host-b", connection.Settings.Host);
        Assert.Same(connection.GetClient(), WriteClient(ctx));
    }

    [Fact]
    public void SetDbConnection_CurrentWrapper_KeepsDataSource()
    {
        // An interceptor wraps the connection that the provider gets from the data source. Setting
        // that wrapper again does not change the connection, so inserts keep the data source.
        using var ctx = new TargetContext(o => o
            .UseClickHouse(TargetA)
            .AddInterceptors(new WrapCreatedConnectionInterceptor()));
        var wrapper = ctx.Database.GetDbConnection();
        Assert.IsType<WrappedDbConnection>(wrapper);

        ctx.Database.SetDbConnection(wrapper);

        Assert.Same(wrapper, ctx.Database.GetDbConnection());
        Assert.Equal("host-a", WriteSettings(ctx).Host);
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

/// <summary>
/// A <see cref="DbConnection"/> that sends every call to the connection it wraps, as a profiler does.
/// </summary>
internal sealed class WrappedDbConnection(DbConnection inner) : DbConnection
{
    [System.Diagnostics.CodeAnalysis.AllowNull]
    public override string ConnectionString { get => inner.ConnectionString; set => inner.ConnectionString = value; }

    public override string Database => inner.Database;

    public override string DataSource => inner.DataSource;

    public override string ServerVersion => inner.ServerVersion;

    public override System.Data.ConnectionState State => inner.State;

    public override void ChangeDatabase(string databaseName) => inner.ChangeDatabase(databaseName);

    public override void Open() => inner.Open();

    public override void Close() => inner.Close();

    protected override DbTransaction BeginDbTransaction(System.Data.IsolationLevel isolationLevel)
        => inner.BeginTransaction(isolationLevel);

    protected override DbCommand CreateDbCommand() => inner.CreateCommand();

    protected override void Dispose(bool disposing)
    {
        if (disposing)
            inner.Dispose();

        base.Dispose(disposing);
    }
}
