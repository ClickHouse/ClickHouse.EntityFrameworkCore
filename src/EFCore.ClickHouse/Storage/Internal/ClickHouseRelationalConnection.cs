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
            dataSource: null,
            _joinNullSemanticsDisabled);
    }

    private bool TryGetClickHouseClientSettings(out ClickHouseClientSettings settings)
    {
        if (_dataSource is ClickHouseDataSource clickHouseDataSource)
        {
            settings = clickHouseDataSource.Settings;
            return true;
        }

        if (DbConnection is ClickHouseConnection clickHouseConnection)
        {
            settings = clickHouseConnection.Settings;
            return true;
        }

        settings = null!;
        return false;
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
