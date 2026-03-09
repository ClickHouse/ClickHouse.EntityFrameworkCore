using Microsoft.EntityFrameworkCore.Update;

namespace ClickHouse.EntityFrameworkCore.Update.Internal;

public class ClickHouseModificationCommandBatchFactory : IModificationCommandBatchFactory
{
    public ClickHouseModificationCommandBatchFactory(ModificationCommandBatchFactoryDependencies dependencies)
    {
    }

    public ModificationCommandBatch Create()
        => throw new NotSupportedException(
            "SaveChanges write operations are not supported by ClickHouse.EntityFrameworkCore yet. " +
            "This provider currently supports read-only query scenarios.");
}
