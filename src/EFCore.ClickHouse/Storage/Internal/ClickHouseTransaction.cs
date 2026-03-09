using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal;

/// <summary>
/// ClickHouse does not support transactions, so this is just one big no-op
/// </summary>
public class ClickHouseTransaction : IDbContextTransaction
{
    public Guid TransactionId { get; } = Guid.NewGuid();

    public void Dispose() { }
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    public void Commit() { }
    public void Rollback() { }
    public Task CommitAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
    public Task RollbackAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
}
