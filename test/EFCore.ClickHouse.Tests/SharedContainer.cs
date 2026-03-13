using Testcontainers.ClickHouse;

namespace EFCore.ClickHouse.Tests;

/// <summary>
/// Provides a single shared ClickHouse container for all integration tests.
/// Each fixture gets an isolated database via <see cref="GetConnectionStringAsync"/>.
/// </summary>
public static class SharedContainer
{
    private static readonly SemaphoreSlim Lock = new(1, 1);
    private static ClickHouseContainer? _container;
    private static string? _baseConnectionString;
    private static int _dbCounter;

    /// <summary>
    /// Returns a connection string pointing to a fresh, isolated database
    /// on the shared ClickHouse container.
    /// </summary>
    public static async Task<string> GetConnectionStringAsync()
    {
        await EnsureContainerAsync();

        var dbName = $"test_{Interlocked.Increment(ref _dbCounter)}";

        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(_baseConnectionString!);
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"CREATE DATABASE {dbName}";
        await cmd.ExecuteNonQueryAsync();

        // Replace the default database in the connection string
        return _baseConnectionString!.Contains("Database=")
            ? System.Text.RegularExpressions.Regex.Replace(_baseConnectionString!, @"Database=[^;]*", $"Database={dbName}")
            : _baseConnectionString + $";Database={dbName}";
    }

    private static async Task EnsureContainerAsync()
    {
        if (_baseConnectionString is not null)
            return;

        await Lock.WaitAsync();
        try
        {
            if (_baseConnectionString is not null)
                return;

            _container = new ClickHouseBuilder("clickhouse/clickhouse-server:latest").Build();
            await _container.StartAsync();
            _baseConnectionString = _container.GetConnectionString();
        }
        finally
        {
            Lock.Release();
        }
    }
}
