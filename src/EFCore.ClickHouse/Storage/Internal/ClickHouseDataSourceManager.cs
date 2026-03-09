using System.Collections.Concurrent;
using System.Data.Common;
using ClickHouse.Driver.ADO;
using ClickHouse.EntityFrameworkCore.Infrastructure.Internal;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal;

public class ClickHouseDataSourceManager : IDisposable, IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, ClickHouseDataSource> _dataSources = new();
    private volatile int _isDisposed;

    public DbDataSource? GetDataSource(ClickHouseOptionsExtension? extension)
        => extension switch
        {
            { DataSource: { } dataSource } => dataSource,
            { Connection: not null } => null,
            { ConnectionString: { } connectionString } => GetOrCreateDataSource(connectionString),
            _ => null
        };

    private ClickHouseDataSource GetOrCreateDataSource(string connectionString)
    {
        if (_dataSources.TryGetValue(connectionString, out var existing))
            return existing;

        var newDataSource = new ClickHouseDataSource(connectionString);
        var added = _dataSources.GetOrAdd(connectionString, newDataSource);

        if (!ReferenceEquals(added, newDataSource))
            newDataSource.Dispose();
        else if (_isDisposed == 1)
        {
            newDataSource.Dispose();
            throw new ObjectDisposedException(nameof(ClickHouseDataSourceManager));
        }

        return added;
    }

    public void Dispose()
    {
        if (Interlocked.CompareExchange(ref _isDisposed, 1, 0) == 0)
        {
            foreach (var dataSource in _dataSources.Values)
                dataSource.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.CompareExchange(ref _isDisposed, 1, 0) == 0)
        {
            foreach (var dataSource in _dataSources.Values)
                await dataSource.DisposeAsync().ConfigureAwait(false);
        }
    }
}
