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
    private readonly ClickHouseDataSourceManager? _dataSourceManager;
    private readonly bool _joinNullSemanticsDisabled;
    private DbDataSource? _dataSource;

    // The connection that CreateDbConnection got from _dataSource. Only this connection can use
    // the data source client and settings.
    private DbConnection? _dataSourceConnection;

    // Insert client for a ClickHouseConnection that does not use the data source client, and the
    // connection settings object that it copies.
    private ClickHouseClient? _connectionClient;
    private ClickHouseClientSettings? _connectionClientSource;

    public ClickHouseRelationalConnection(
        RelationalConnectionDependencies dependencies,
        ClickHouseDataSourceManager dataSourceManager)
        : base(dependencies)
    {
        var extension = dependencies.ContextOptions.FindExtension<ClickHouseOptionsExtension>();
        _dataSourceManager = dataSourceManager;
        _dataSource = dataSourceManager.GetDataSource(extension);
        _joinNullSemanticsDisabled = extension?.JoinNullSemanticsDisabled ?? false;
    }

    // Used by CreateMasterConnection only
    private ClickHouseRelationalConnection(
        RelationalConnectionDependencies dependencies,
        bool joinNullSemanticsDisabled)
        : base(dependencies)
    {
        _joinNullSemanticsDisabled = joinNullSemanticsDisabled;
    }

    /// <summary>
    /// Sets the connection string, and uses the provider-owned data source for it. This removes a
    /// <see cref="DbDataSource"/> that was configured with <c>UseClickHouse(DbDataSource)</c>.
    /// </summary>
    public override string? ConnectionString
    {
        get => base.ConnectionString;
        set
        {
            var dataSource = string.IsNullOrWhiteSpace(value)
                ? null
                : _dataSourceManager?.GetOrCreateDataSource(value, _joinNullSemanticsDisabled);

            // The base setter also applies the string to the current connection, if there is one.
            // That connection then does not come from the new data source.
            base.ConnectionString = value;
            _dataSource = dataSource;
            _dataSourceConnection = null;
            DisposeConnectionClient();
        }
    }

    /// <summary>
    /// Sets the connection. A connection that is not null replaces the configured data source.
    /// </summary>
    public override void SetDbConnection(DbConnection? value, bool contextOwnsConnection)
    {
        base.SetDbConnection(value, contextOwnsConnection);

        if (value is not null)
            _dataSource = null;

        _dataSourceConnection = null;
        DisposeConnectionClient();
    }

    // Dispose and DisposeAsync call these with disposeDbConnection: true. A pooled context keeps
    // its connection between leases, so it keeps the insert client too.
    protected override void ResetState(bool disposeDbConnection)
    {
        base.ResetState(disposeDbConnection);
        if (disposeDbConnection)
            DisposeConnectionClient();
    }

    protected override async ValueTask ResetStateAsync(bool disposeDbConnection)
    {
        await base.ResetStateAsync(disposeDbConnection).ConfigureAwait(false);
        if (disposeDbConnection)
            DisposeConnectionClient();
    }

    protected override DbConnection CreateDbConnection()
    {
        if (_dataSource is not null)
            return _dataSourceConnection = _dataSource.CreateConnection();

        return new ClickHouseConnection(
            ClickHouseClientIdentity.CreateSettings(
                _joinNullSemanticsDisabled
                    ? ConnectionString!
                    : ClickHouseDataSourceManager.EnsureDefaultSettings(ConnectionString!)));
    }

    protected override bool SupportsAmbientTransactions => false;

    /// <summary>
    /// Applies <c>join_use_nulls=1</c> to a closed connection before it opens, unless the user set
    /// <c>join_use_nulls</c> or disabled join null semantics. With the ClickHouse default (0), a
    /// LEFT JOIN returns column defaults instead of NULL, and EF Core cannot detect a missing
    /// navigation. The setting must be on the connection, because the driver sends it as a URL
    /// parameter with each HTTP request; a <c>SET</c> statement does not persist between requests.
    /// The connection-string path already has the setting from
    /// <see cref="ClickHouseDataSourceManager.EnsureDefaultSettings"/>.
    /// </summary>
    public override bool Open(bool errorsExpected = false)
    {
        EnsureJoinUseNulls();
        return base.Open(errorsExpected);
    }

    public override Task<bool> OpenAsync(CancellationToken cancellationToken, bool errorsExpected = false)
    {
        EnsureJoinUseNulls();
        return base.OpenAsync(cancellationToken, errorsExpected);
    }

    private void EnsureJoinUseNulls()
    {
        if (_joinNullSemanticsDisabled || DbConnection.State != ConnectionState.Closed)
            return;

        // Assigning ConnectionString rebuilds the driver settings from the string, which loses
        // settings that have no connection-string key, such as SkipServerCertificateValidation.
        if (DbConnection is ClickHouseConnection clickHouseConnection)
        {
            var settings = clickHouseConnection.Settings;
            if (settings.CustomSettings.ContainsKey(JoinUseNullsSetting))
                return;

            var updatedSettings = new ClickHouseClientSettings(settings);
            updatedSettings.CustomSettings[JoinUseNullsSetting] = "1";
            clickHouseConnection.Settings = updatedSettings;
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

    private const string JoinUseNullsSetting = "join_use_nulls";

    public IClickHouseRelationalConnection CreateMasterConnection()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        if (TryGetClickHouseClientSettings(out var settings))
        {
            // Copy the settings object, not the connection string, to keep settings that have no
            // connection-string key. The master connection owns the new connection and disposes it.
            var masterSettings = new ClickHouseClientSettings(settings) { Database = "default" };
            optionsBuilder.UseClickHouse(new ClickHouseConnection(masterSettings), contextOwnsConnection: true);
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

    private bool TryGetClickHouseClientSettings(out ClickHouseClientSettings settings)
    {
        var connection = DbConnection;
        if (GetDataSourceOfConnection(connection) is { } dataSource)
        {
            settings = dataSource.Settings;
            return true;
        }

        if (connection is ClickHouseConnection clickHouseConnection)
        {
            settings = clickHouseConnection.Settings;
            return true;
        }

        settings = null!;
        return false;
    }

    /// <summary>
    /// Returns the <see cref="ClickHouseDataSource"/> that has the same target as the connection, or
    /// null. A <see cref="ClickHouseConnection"/> must come from the data source and still have the
    /// data source database, because <c>ChangeDatabase</c> and a new connection string change the
    /// target of the connection only. Another connection type (for example, a connection that an
    /// interceptor wraps) cannot give its settings, so it uses the data source.
    /// </summary>
    private ClickHouseDataSource? GetDataSourceOfConnection(DbConnection connection)
    {
        if (_dataSource is not ClickHouseDataSource dataSource)
            return null;

        if (connection is not ClickHouseConnection)
            return dataSource;

        return ReferenceEquals(connection, _dataSourceConnection)
               && connection.Database == dataSource.Settings.Database
            ? dataSource
            : null;
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
    /// Returns the client for inserts, with the same target as the current connection. Inserts go to
    /// the client database and queries go to <see cref="System.Data.Common.DbConnection.Database"/>.
    /// For a connection from the <see cref="ClickHouseDataSource"/>, this is the data source client.
    /// For another <see cref="ClickHouseConnection"/>, the driver does not make the connection client
    /// public, so this object owns a client with a copy of the connection settings and database.
    /// </summary>
    public IClickHouseClient GetClickHouseClient()
    {
        var connection = DbConnection;
        if (GetDataSourceOfConnection(connection) is { } dataSource)
            return dataSource.GetClient();

        if (connection is not ClickHouseConnection clickHouseConnection)
        {
            throw new InvalidOperationException(
                $"Inserts need a {nameof(ClickHouseConnection)}, but the connection is a " +
                $"'{connection.GetType().Name}'. Configure the context with a connection string, a " +
                $"{nameof(ClickHouseDataSource)} or a {nameof(ClickHouseConnection)}.");
        }

        var settings = clickHouseConnection.Settings;
        var database = clickHouseConnection.Database;
        if (_connectionClient is null
            || !ReferenceEquals(_connectionClientSource, settings)
            || _connectionClient.Settings.Database != database)
        {
            DisposeConnectionClient();
            _connectionClient = new ClickHouseClient(new ClickHouseClientSettings(settings) { Database = database });
            _connectionClientSource = settings;
        }

        return _connectionClient;
    }

    private void DisposeConnectionClient()
    {
        _connectionClient?.Dispose();
        _connectionClient = null;
        _connectionClientSource = null;
    }
}
