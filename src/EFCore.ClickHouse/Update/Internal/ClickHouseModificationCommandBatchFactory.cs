using ClickHouse.EntityFrameworkCore.Infrastructure.Internal;
using ClickHouse.EntityFrameworkCore.Storage.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Update;

namespace ClickHouse.EntityFrameworkCore.Update.Internal;

public class ClickHouseModificationCommandBatchFactory : IModificationCommandBatchFactory
{
    private const int DefaultMaxBatchSize = 1000;
    private readonly IClickHouseRelationalConnection _connection;
    private readonly int _maxBatchSize;

    public ClickHouseModificationCommandBatchFactory(
        ModificationCommandBatchFactoryDependencies dependencies,
        IClickHouseRelationalConnection connection)
    {
        _connection = connection;

        _maxBatchSize = dependencies.CurrentContext.Context.GetService<IDbContextOptions>()
            .Extensions.OfType<ClickHouseOptionsExtension>()
            .FirstOrDefault()?.MaxBatchSize ?? DefaultMaxBatchSize;
    }

    public ModificationCommandBatch Create()
        => new ClickHouseModificationCommandBatch(_connection, _maxBatchSize);
}
