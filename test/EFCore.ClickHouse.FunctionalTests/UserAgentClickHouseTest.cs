using ClickHouse.Driver.ADO;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;

namespace Microsoft.EntityFrameworkCore;

/// <summary>
/// Verifies end-to-end that the provider tags its ClickHouse HTTP requests with the
/// <c>lib</c> User-Agent token, by reading it back from <c>system.query_log</c>.
/// </summary>
public class UserAgentClickHouseTest
{
    private sealed class ProbeContext(string connectionString) : DbContext
    {
        private readonly string _connectionString = connectionString;

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.UseClickHouse(_connectionString);
    }

    [Fact]
    public async Task Queries_are_tagged_with_lib_in_user_agent()
    {
        var connectionString = TestEnvironment.DefaultConnection;
        var marker = $"ua_probe_{Guid.NewGuid():N}";

        // Issue a query through EF Core's (tagged) connection. The marker (a GUID) is
        // concatenated rather than interpolated to keep clear of EF1002.
        var probeSql = "SELECT 1 /* " + marker + " */";
        await using (var context = new ProbeContext(connectionString))
        {
            await context.Database.ExecuteSqlRawAsync(probeSql);
        }

        // Use a separate, untagged connection for verification so the lib tag we assert
        // on can only have come from the provider's own connection.
        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        using (var flush = connection.CreateCommand())
        {
            flush.CommandText = "SYSTEM FLUSH LOGS";
            await flush.ExecuteNonQueryAsync();
        }

        using var query = connection.CreateCommand();
        query.CommandText =
            "SELECT count() FROM system.query_log " +
            $"WHERE query LIKE '%{marker}%' " +
            "AND http_user_agent LIKE '%lib:ClickHouse.EntityFrameworkCore%' " +
            "AND event_time > now() - INTERVAL 5 MINUTE";

        var count = Convert.ToInt64(await query.ExecuteScalarAsync());
        Assert.True(count > 0,
            "Expected EF Core's query to carry lib:ClickHouse.EntityFrameworkCore in the HTTP User-Agent.");
    }
}
