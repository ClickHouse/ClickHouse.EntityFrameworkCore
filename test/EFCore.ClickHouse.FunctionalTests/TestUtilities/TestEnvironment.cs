using Testcontainers.ClickHouse;

namespace Microsoft.EntityFrameworkCore.TestUtilities;

public static class TestEnvironment
{
    private static readonly object Sync = new();
    private static ClickHouseContainer? _container;
    private static string? _defaultConnection;

    public static string DefaultConnection
    {
        get
        {
            var configured = Environment.GetEnvironmentVariable("CLICKHOUSE_TEST_CONNECTION");
            if (!string.IsNullOrWhiteSpace(configured))
                return configured;

            if (_defaultConnection is not null)
                return _defaultConnection;

            lock (Sync)
            {
                if (_defaultConnection is not null)
                    return _defaultConnection;

                _container = new ClickHouseBuilder("clickhouse/clickhouse-server:latest").Build();
                _container.StartAsync().GetAwaiter().GetResult();
                _defaultConnection = _container.GetConnectionString();
                AppDomain.CurrentDomain.ProcessExit += (_, _) =>
                {
                    try
                    {
                        _container.DisposeAsync().AsTask().GetAwaiter().GetResult();
                    }
                    catch
                    {
                        // Best effort disposal on process shutdown.
                    }
                };
                return _defaultConnection;
            }
        }
    }
}
