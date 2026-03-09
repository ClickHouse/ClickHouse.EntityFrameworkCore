using System.Data;
using System.Data.Common;
using ClickHouse.Driver.ADO;
using ClickHouse.EntityFrameworkCore.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal;

public class ClickHouseRelationalConnection : RelationalConnection, IClickHouseRelationalConnection
{
    private readonly DbDataSource? _dataSource;

    public ClickHouseRelationalConnection(
        RelationalConnectionDependencies dependencies,
        ClickHouseDataSourceManager dataSourceManager)
        : base(dependencies)
    {
        var extension = dependencies.ContextOptions.FindExtension<ClickHouseOptionsExtension>();
        _dataSource = dataSourceManager.GetDataSource(extension);
    }

    // Used by CreateMasterConnection only
    private ClickHouseRelationalConnection(
        RelationalConnectionDependencies dependencies,
        DbDataSource? dataSource)
        : base(dependencies)
    {
        _dataSource = dataSource;
    }

    protected override DbConnection CreateDbConnection()
        => _dataSource?.CreateConnection()
           ?? new ClickHouseConnection(ConnectionString!);

    protected override bool SupportsAmbientTransactions => false;

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
            dataSource: null);
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
}
