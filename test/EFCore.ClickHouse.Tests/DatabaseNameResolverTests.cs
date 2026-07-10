using ClickHouse.EntityFrameworkCore.Storage.Internal;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace EFCore.ClickHouse.Tests;

public class DatabaseNameResolverTests
{
    [Fact]
    public void ConnectionDatabase_without_schema_is_used()
    {
        var database = ClickHouseDatabaseNameResolver.Resolve("connection_db", null, NullLogger.Instance);

        Assert.Equal("connection_db", database);
    }

    [Fact]
    public void Schema_without_connection_database_is_used()
    {
        var database = ClickHouseDatabaseNameResolver.Resolve(null, "schema_db", NullLogger.Instance);

        Assert.Equal("schema_db", database);
    }

    [Fact]
    public void Schema_overrides_different_connection_database()
    {
        var database = ClickHouseDatabaseNameResolver.Resolve("connection_db", "schema_db", NullLogger.Instance);

        Assert.Equal("schema_db", database);
    }

    [Fact]
    public void Identical_schema_and_connection_database_use_schema()
    {
        var database = ClickHouseDatabaseNameResolver.Resolve("same_db", "same_db", NullLogger.Instance);

        Assert.Equal("same_db", database);
    }
}
