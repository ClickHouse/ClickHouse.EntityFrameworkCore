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
    /// Returns the ClickHouse settings that EF Core semantics need. Every statement that the
    /// provider sends must carry them: <see cref="ApplyQuerySettings"/> adds them to a command, and
    /// <see cref="CreateInsertOptions"/> adds them to an insert. The provider sends them with each
    /// statement and does not change the settings of a connection or data source, because the
    /// driver then makes a new client, with a new HTTP connection pool, for the connection.
    /// </summary>
    /// <remarks>
    /// <c>join_use_nulls=1</c>: with the ClickHouse default (0), a LEFT JOIN returns column defaults
    /// instead of NULL, and EF Core cannot detect a missing navigation.
    /// <c>DisableJoinNullSemantics()</c> removes it.
    /// </remarks>
    protected virtual IEnumerable<KeyValuePair<string, object>> GetQuerySettings()
    {
        if (!_joinNullSemanticsDisabled)
            yield return new(JoinUseNullsSetting, "1");
    }

    /// <summary>
    /// Adds the provider query settings to a driver command. A setting that the connection settings
    /// already contain is left out, so the user value wins. The driver sends command settings with
    /// the HTTP request and lets them override connection settings.
    /// </summary>
    public virtual void ApplyQuerySettings(DbCommand command)
    {
        if (command is not ClickHouseCommand clickHouseCommand)
            return;

        var connectionSettings = (command.Connection as ClickHouseConnection)?.Settings;
        foreach (var (name, value) in GetQuerySettings())
        {
            if (connectionSettings?.CustomSettings.ContainsKey(name) != true)
                clickHouseCommand.CustomSettings.TryAdd(name, value);
        }
    }

    /// <summary>
    /// Returns the options for an insert through <paramref name="client"/>, with the provider query
    /// settings that the client settings do not already contain. Returns null when no setting is
    /// missing, so the driver uses its default options.
    /// </summary>
    public virtual InsertOptions? CreateInsertOptions(IClickHouseClient client)
    {
        Dictionary<string, object>? settings = null;
        foreach (var (name, value) in GetQuerySettings())
        {
            if (!client.Settings.CustomSettings.ContainsKey(name))
                (settings ??= new Dictionary<string, object>())[name] = value;
        }

        return settings is null ? null : new InsertOptions { CustomSettings = settings };
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
