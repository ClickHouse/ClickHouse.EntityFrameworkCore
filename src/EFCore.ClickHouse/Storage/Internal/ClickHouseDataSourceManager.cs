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
        var effectiveConnectionString = EnsureDefaultSettings(connectionString);

        if (_dataSources.TryGetValue(effectiveConnectionString, out var existing))
            return existing;

        var newDataSource = new ClickHouseDataSource(effectiveConnectionString);
        var added = _dataSources.GetOrAdd(effectiveConnectionString, newDataSource);

        if (!ReferenceEquals(added, newDataSource))
            newDataSource.Dispose();
        else if (_isDisposed == 1)
        {
            newDataSource.Dispose();
            throw new ObjectDisposedException(nameof(ClickHouseDataSourceManager));
        }

        return added;
    }

    /// <summary>
    /// Ensures that required ClickHouse session settings are present in the connection string.
    /// Uses the <c>set_</c> prefix convention supported by ClickHouse.Driver.
    /// Does not overwrite settings the user has explicitly configured.
    /// </summary>
    internal static string EnsureDefaultSettings(string connectionString)
    {
        // join_use_nulls=1: LEFT/RIGHT/FULL JOINs produce NULL for non-matching rows
        // (ClickHouse default is 0 → default values, which breaks EF Core's null-based
        // navigation detection and LEFT JOIN semantics).
        const string key = "set_join_use_nulls";
        if (!connectionString.Contains(key, StringComparison.OrdinalIgnoreCase))
        {
            connectionString += (connectionString.EndsWith(';') ? "" : ";") + key + "=1";
        }

        return connectionString;
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
