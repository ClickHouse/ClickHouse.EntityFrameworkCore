using Microsoft.EntityFrameworkCore.Migrations;

namespace ClickHouse.EntityFrameworkCore.Migrations.Internal;

/// <summary>
/// No-op database lock for ClickHouse (no transaction/lock support).
/// </summary>
public class ClickHouseMigrationDatabaseLock : IMigrationsDatabaseLock
{
    public ClickHouseMigrationDatabaseLock(IHistoryRepository historyRepository)
    {
        HistoryRepository = historyRepository;
    }

    public IHistoryRepository HistoryRepository { get; }

    public void Dispose() { }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
