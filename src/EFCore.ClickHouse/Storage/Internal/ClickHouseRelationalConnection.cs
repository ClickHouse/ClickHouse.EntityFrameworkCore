using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using ClickHouse.Driver;
using ClickHouse.Driver.ADO;
using ClickHouse.EntityFrameworkCore.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal;

public class ClickHouseRelationalConnection : RelationalConnection, IClickHouseRelationalConnection
{
    private readonly ClickHouseDataSourceManager? _dataSourceManager;
    private readonly bool _joinNullSemanticsDisabled;

    // The data source for new connections. A connection string gets a provider-owned data source,
    // which DataSource creates on first need from _dataSourceConnectionString.
    private DbDataSource? _dataSource;
    private string? _dataSourceConnectionString;

    // The connection that CreateDbConnection got from the data source, until SetDbConnection
    // replaces it, and the connection that EF Core uses, which can be a wrapper around it.
    private DbConnection? _dataSourceConnection;
    private DbConnection? _currentConnection;

    public ClickHouseRelationalConnection(
        RelationalConnectionDependencies dependencies,
        ClickHouseDataSourceManager dataSourceManager)
        : base(dependencies)
    {
        var extension = dependencies.ContextOptions.FindExtension<ClickHouseOptionsExtension>();
        _dataSourceManager = dataSourceManager;
        _joinNullSemanticsDisabled = extension?.JoinNullSemanticsDisabled ?? false;
        _dataSource = extension?.DataSource;

        if (_dataSource is null && extension?.Connection is null)
            _dataSourceConnectionString = extension?.ConnectionString;
    }

    // Used by CreateMasterConnection only
    private ClickHouseRelationalConnection(
        RelationalConnectionDependencies dependencies,
        bool joinNullSemanticsDisabled)
        : base(dependencies)
    {
        _joinNullSemanticsDisabled = joinNullSemanticsDisabled;
    }

    private DbDataSource? DataSource
    {
        get
        {
            if (_dataSource is null
                && !string.IsNullOrWhiteSpace(_dataSourceConnectionString)
                && _dataSourceManager is not null)
            {
                _dataSource = _dataSourceManager.GetOrCreateDataSource(
                    _dataSourceConnectionString, _joinNullSemanticsDisabled);
            }

            return _dataSource;
        }
    }

    [AllowNull]
    public override DbConnection DbConnection
    {
        get => _currentConnection = base.DbConnection;
        set => base.DbConnection = value;
    }

    protected override DbConnection CreateDbConnection()
    {
        if (DataSource is { } dataSource)
            return _dataSourceConnection = dataSource.CreateConnection();

        return new ClickHouseConnection(
            ClickHouseClientIdentity.CreateSettings(
                _joinNullSemanticsDisabled
                    ? ConnectionString!
                    : ClickHouseDataSourceManager.EnsureDefaultSettings(ConnectionString!)));
    }

    /// <summary>
    /// Sets the connection string. The provider uses its own data source for the string, with one
    /// client and HTTP connection pool for each distinct string, as for <c>UseClickHouse(string)</c>.
    /// The new data source replaces a data source from <c>UseClickHouse(DbDataSource)</c>. When the
    /// current connection came from a data source and is closed, the provider replaces it with a
    /// connection from the new data source, so it keeps the shared client. Otherwise EF Core writes
    /// the string into the current connection.
    /// </summary>
    public override string? ConnectionString
    {
        get => base.ConnectionString;
        set
        {
            var useDataSource = !string.IsNullOrWhiteSpace(value) && _dataSourceManager is not null;

            // EF Core writes the string into a connection whose string is different, and the string of
            // a driver connection is normalized, so even the same string would rebuild the client.
            if (useDataSource && _dataSourceConnection is { State: ConnectionState.Closed })
            {
                // Remove the connection, so that EF Core gets the next one from CreateDbConnection.
                // Removing it resets the command timeout, which a new connection string must keep.
                var commandTimeout = CommandTimeout;
                base.SetDbConnection(null, contextOwnsConnection: true);
                CommandTimeout = commandTimeout;
                _dataSourceConnection = null;
                _currentConnection = null;
            }

            base.ConnectionString = value;
            _dataSource = null;
            _dataSourceConnectionString = useDataSource ? value : null;
        }
    }

    /// <summary>
    /// Sets the connection. A connection that is not null and is not the current connection replaces
    /// the configured data source, and inserts use the client of that connection.
    /// </summary>
    public override void SetDbConnection(DbConnection? value, bool contextOwnsConnection)
    {
        var isCurrent = value is not null && ReferenceEquals(value, _currentConnection);

        base.SetDbConnection(value, contextOwnsConnection);

        if (isCurrent)
            return;

        _currentConnection = value;
        _dataSourceConnection = null;

        if (value is not null)
        {
            _dataSource = null;
            _dataSourceConnectionString = null;
        }
    }

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
    /// Returns the options for an insert through <paramref name="client"/>: the provider query
    /// settings that the client settings do not already contain, and the database of the current
    /// connection when it is not the database of the client, for example after
    /// <see cref="DbConnection.ChangeDatabase(string)"/>. Returns null when neither is needed, so the
    /// driver uses its default options.
    /// </summary>
    public virtual InsertOptions? CreateInsertOptions(IClickHouseClient client)
    {
        Dictionary<string, object>? settings = null;
        foreach (var (name, value) in GetQuerySettings())
        {
            if (!client.Settings.CustomSettings.ContainsKey(name))
                (settings ??= new Dictionary<string, object>())[name] = value;
        }

        var database = DbConnection.Database;
        if (string.IsNullOrEmpty(database) || database == client.Settings.Database)
            database = null;

        return settings is null && database is null
            ? null
            : new InsertOptions { CustomSettings = settings, Database = database };
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

    /// <summary>
    /// Returns the client for inserts. For a <see cref="ClickHouseConnection"/>, this is the client of
    /// the current connection, so inserts use its server, settings and HTTP connection pool, and
    /// follow <c>SetConnectionString</c> and <c>SetDbConnection</c>. <see cref="CreateInsertOptions"/>
    /// adds the database of the connection. For another connection type, for example a wrapper, this
    /// is the client of the configured <see cref="ClickHouseDataSource"/>.
    /// </summary>
    public IClickHouseClient GetClickHouseClient()
    {
        var connection = DbConnection;
        if (connection is ClickHouseConnection clickHouseConnection)
            return clickHouseConnection.GetClient();

        if (DataSource is ClickHouseDataSource clickHouseDataSource)
            return clickHouseDataSource.GetClient();

        throw new InvalidOperationException(
            $"Inserts need a {nameof(ClickHouseConnection)}, but the connection is a '{connection.GetType().Name}' " +
            $"and no {nameof(ClickHouseDataSource)} is configured. Configure the context with a connection " +
            $"string, a {nameof(ClickHouseDataSource)} or a {nameof(ClickHouseConnection)}.");
    }
}
