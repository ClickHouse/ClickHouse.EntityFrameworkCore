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
               _joinNullSemanticsDisabled
                   ? ConnectionString!
                   : ClickHouseDataSourceManager.EnsureDefaultSettings(ConnectionString!));

    protected override bool SupportsAmbientTransactions => false;

    /// <summary>
    /// Ensures <c>join_use_nulls=1</c> is present in the underlying connection's connection
    /// string before it's opened. Required because ClickHouse's default (0) makes LEFT JOIN
    /// return column defaults rather than NULL, which breaks EF Core's null-based navigation
    /// detection.
    ///
    /// The ClickHouse.Driver HTTP protocol is stateless by default (<c>UseSession=False</c>):
    /// a standalone <c>SET join_use_nulls = 1</c> statement does not persist to subsequent
    /// queries. Instead the driver applies <c>set_*</c> connection-string parameters as URL
    /// parameters on every query, so we must mutate the connection string itself.
    ///
    /// Applies on all paths. For the connection-string path the setting is already baked in by
    /// <see cref="ClickHouseDataSourceManager.EnsureDefaultSettings"/>; this override covers
    /// the <see cref="DbConnection"/> and <see cref="DbDataSource"/> paths where EF hands us
    /// connection objects we didn't construct. We only mutate when the connection is Closed
    /// and the user has not explicitly configured <c>join_use_nulls</c>.
    /// </summary>
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

    private void EnsureJoinUseNullsInConnectionString()
    {
        if (_joinNullSemanticsDisabled || DbConnection.State != ConnectionState.Closed)
            return;

        var cs = DbConnection.ConnectionString;
        if (string.IsNullOrEmpty(cs)
            || cs.Contains("join_use_nulls", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        DbConnection.ConnectionString = ClickHouseDataSourceManager.EnsureDefaultSettings(cs);
    }

    public IClickHouseRelationalConnection CreateMasterConnection()
    {
        var connectionStringBuilder = new ClickHouseConnectionStringBuilder(
            _dataSource?.ConnectionString ?? ConnectionString)
        {
            Database = "default"
        };

        var masterConnectionString = connectionStringBuilder.ConnectionString;

        var contextOptions = new DbContextOptionsBuilder()
            .UseClickHouse(masterConnectionString)
            .Options;

        return new ClickHouseRelationalConnection(
            Dependencies with { ContextOptions = contextOptions },
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
