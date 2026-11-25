using System.Data;
using System.Data.Common;
using ClickHouse.Driver;
using ClickHouse.Driver.ADO;
using ClickHouse.EntityFrameworkCore.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal;

public class ClickHouseRelationalConnection : RelationalConnection, IClickHouseRelationalConnection
{
    private readonly DbDataSource? _dataSource;
    private readonly bool _joinNullSemanticsDisabled;

    public ClickHouseRelationalConnection(
        RelationalConnectionDependencies dependencies,
        ClickHouseDataSourceManager dataSourceManager)
        : base(dependencies)
    {
        var extension = dependencies.ContextOptions.FindExtension<ClickHouseOptionsExtension>();
        _dataSource = dataSourceManager.GetDataSource(extension);
        _joinNullSemanticsDisabled = extension?.JoinNullSemanticsDisabled ?? false;
    }

    // Used by CreateMasterConnection only
    private ClickHouseRelationalConnection(
        RelationalConnectionDependencies dependencies,
        DbDataSource? dataSource,
        bool joinNullSemanticsDisabled)
        : base(dependencies)
    {
        _dataSource = dataSource;
        _joinNullSemanticsDisabled = joinNullSemanticsDisabled;
    }

    protected override DbConnection CreateDbConnection()
        => _dataSource?.CreateConnection()
           ?? new ClickHouseConnection(
               ClickHouseClientIdentity.CreateSettings(
                   _joinNullSemanticsDisabled
                       ? ConnectionString!
                       : ClickHouseDataSourceManager.EnsureDefaultSettings(ConnectionString!)));

    protected override bool SupportsAmbientTransactions => false;

    private const string JoinUseNullsSetting = "join_use_nulls";

    /// <summary>
    /// Adds <c>join_use_nulls=1</c> to a command that EF Core runs, unless join null semantics are
    /// disabled or the connection settings already set <c>join_use_nulls</c>. With the ClickHouse
    /// default (0), a LEFT JOIN returns column defaults instead of NULL, and EF Core cannot detect a
    /// missing navigation. The driver sends command settings with the HTTP request and lets them
    /// override connection settings. The provider does not change the connection settings, because
    /// the driver then makes a new client, with a new HTTP connection pool, for the connection.
    /// </summary>
    public virtual void ApplyJoinNullSemantics(DbCommand command)
    {
        if (_joinNullSemanticsDisabled
            || command is not ClickHouseCommand clickHouseCommand
            || command.Connection is ClickHouseConnection connection
               && connection.Settings.CustomSettings.ContainsKey(JoinUseNullsSetting))
        {
            return;
        }

        clickHouseCommand.CustomSettings.TryAdd(JoinUseNullsSetting, "1");
    }

    public override bool Open(bool errorsExpected = false)
    {
        EnsureJoinUseNullsInConnectionString();
        return base.Open(errorsExpected);
    }

    public override Task<bool> OpenAsync(CancellationToken cancellationToken, bool errorsExpected = false)
    {
        EnsureJoinUseNullsInConnectionString();
        return base.OpenAsync(cancellationToken, errorsExpected);
    }

    /// <summary>
    /// For a connection that is not a <see cref="ClickHouseConnection"/>, for example a wrapper, the
    /// provider cannot get to the driver commands. It therefore adds <c>set_join_use_nulls=1</c> to
    /// the connection string of a closed connection, unless the string already sets
    /// <c>join_use_nulls</c>.
    /// </summary>
    private void EnsureJoinUseNullsInConnectionString()
    {
        if (_joinNullSemanticsDisabled
            || DbConnection is ClickHouseConnection
            || DbConnection.State != ConnectionState.Closed)
        {
            return;
        }

        var cs = DbConnection.ConnectionString;
        if (string.IsNullOrEmpty(cs)
            || cs.Contains("join_use_nulls", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        DbConnection.ConnectionString = ClickHouseDataSourceManager.EnsureDefaultSettings(cs);
    }

    /// <summary>
    /// Returns a connection to the <c>default</c> database, for <c>CREATE DATABASE</c> and
    /// <c>DROP DATABASE</c>. For a <see cref="ClickHouseConnection"/>, the master connection is a
    /// clone that uses the same driver client. It therefore has all the settings of the connection,
    /// including settings that have no connection-string key, and it uses the same HTTP connection
    /// pool. The driver sends the database of the connection with each query.
    /// </summary>
    public IClickHouseRelationalConnection CreateMasterConnection()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        if (DbConnection is ClickHouseConnection clickHouseConnection)
        {
            // A clone does not own the client, so disposing the master connection keeps the client.
            var masterConnection = (ClickHouseConnection)clickHouseConnection.Clone();
            masterConnection.ChangeDatabase("default");
            optionsBuilder.UseClickHouse(masterConnection, contextOwnsConnection: true);
        }
        else
        {
            var masterConnectionString = new ClickHouseConnectionStringBuilder(
                _dataSource?.ConnectionString ?? ConnectionString)
            {
                Database = "default"
            }.ConnectionString;

            optionsBuilder.UseClickHouse(masterConnectionString);
        }

        return new ClickHouseRelationalConnection(
            Dependencies with { ContextOptions = optionsBuilder.Options },
            dataSource: null,
            _joinNullSemanticsDisabled);
    }

    public override IDbContextTransaction BeginTransaction(IsolationLevel isolationLevel)
        => new ClickHouseTransaction();

    public override IDbContextTransaction BeginTransaction()
        => BeginTransaction(IsolationLevel.Unspecified);

    public override Task<IDbContextTransaction> BeginTransactionAsync(
        CancellationToken cancellationToken = default)
        => BeginTransactionAsync(IsolationLevel.Unspecified, cancellationToken);

    public override Task<IDbContextTransaction> BeginTransactionAsync(
        IsolationLevel isolationLevel,
        CancellationToken cancellationToken = default)
        => Task.FromResult<IDbContextTransaction>(new ClickHouseTransaction());

    public IClickHouseClient GetClickHouseClient()
    {
        if (_dataSource is ClickHouseDataSource clickHouseDataSource)
            return clickHouseDataSource.GetClient();

        throw new InvalidOperationException(
            "Cannot obtain IClickHouseClient. The connection must be configured with a connection string " +
            "or ClickHouseDataSource, not a raw DbConnection.");
    }
}
