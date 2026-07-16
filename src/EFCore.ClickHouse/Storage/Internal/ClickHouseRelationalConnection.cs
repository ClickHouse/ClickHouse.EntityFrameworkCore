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
    /// Ensures <c>join_use_nulls=1</c> is present in the underlying connection's connection
    /// string before it's opened. Required because ClickHouse's default (0) makes LEFT JOIN
    /// return column defaults rather than NULL, which breaks EF Core's null-based navigation
    /// detection.
    ///
    /// The ClickHouse.Driver HTTP protocol is stateless by default (<c>UseSession=False</c>):
    /// a standalone <c>SET join_use_nulls = 1</c> statement does not persist to subsequent
    /// queries. Instead the driver applies <c>set_*</c> parameters (the driver's
    /// <c>CustomSettings</c>) as URL parameters on every query, so we must record the setting on
    /// the connection before it is opened.
    ///
    /// Applies on all paths. For the connection-string path the setting is already baked in by
    /// <see cref="ClickHouseDataSourceManager.EnsureDefaultSettings"/>; this override covers
    /// the <see cref="DbConnection"/> and <see cref="DbDataSource"/> paths where EF hands us
    /// connection objects we didn't construct. We only mutate when the connection is Closed
    /// and the user has not explicitly configured <c>join_use_nulls</c>.
    ///
    /// For a <see cref="ClickHouseConnection"/> we inject the setting by copying and reassigning
    /// its <see cref="ClickHouseConnection.Settings"/> rather than rewriting
    /// <see cref="DbConnection.ConnectionString"/>. Assigning the connection string rebuilds the
    /// driver's settings purely from the string, silently discarding code-only settings such as
    /// <c>SkipServerCertificateValidation</c> or a custom <c>HttpClient</c>. Only the string
    /// fallback (for a non-ClickHouse <see cref="DbConnection"/>) mutates the connection string.
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

        // Prefer mutating the strongly-typed settings so that code-only settings (which have no
        // connection-string equivalent) are preserved. Rewriting ConnectionString would rebuild
        // the driver's settings from the string alone and drop them.
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
            // Clone the driver settings and only swap the database. Round-tripping through a
            // connection string here would drop code-only settings that have no connection-string
            // representation (SkipServerCertificateValidation, BearerToken, HttpClient,
            // HttpClientFactory, LoggerFactory, CustomHeaders, ApplicationInfo, EnableDebugMode,
            // and the parameter/read converters), breaking database create/drop against servers
            // that depend on them (e.g. a self-signed certificate). The connection is owned by the
            // context so it is disposed with the master connection.
            var masterSettings = new ClickHouseClientSettings(settings) { Database = "default" };
            optionsBuilder.UseClickHouse(new ClickHouseConnection(masterSettings), contextOwnsConnection: true);
        }
        else
        {
            var masterConnectionString = new ClickHouseConnectionStringBuilder(ConnectionString)
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
