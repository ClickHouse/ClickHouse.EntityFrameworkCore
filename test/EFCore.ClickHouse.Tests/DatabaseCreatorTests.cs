using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Testcontainers.ClickHouse;
using Xunit;

namespace EFCore.ClickHouse.Tests;

public class DatabaseCreatorFixture : IAsyncLifetime
{
    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        ConnectionString = await SharedContainer.GetConnectionStringAsync();
    }

    public Task DisposeAsync() => Task.CompletedTask;
}

/// <summary>
/// Integration tests for ClickHouseDatabaseCreator, testing both sync and async
/// database lifecycle operations (create, exists, hasTables, delete).
/// </summary>
public class DatabaseCreatorTests : IClassFixture<DatabaseCreatorFixture>
{
    private readonly DatabaseCreatorFixture _fixture;

    public DatabaseCreatorTests(DatabaseCreatorFixture fixture)
    {
        _fixture = fixture;
    }

    private DbContext CreateContext(string databaseName)
    {
        var builder = new global::ClickHouse.Driver.ADO.ClickHouseConnectionStringBuilder(
            _fixture.ConnectionString)
        {
            Database = databaseName
        };

        var optionsBuilder = new DbContextOptionsBuilder();
        optionsBuilder.UseClickHouse(builder.ConnectionString);
        return new DbContext(optionsBuilder.Options);
    }

    // --- Async Methods ---

    [Fact]
    public async Task ExistsAsync_ReturnsFalseForNonExistentDatabase()
    {
        var dbName = $"test_notexist_{Guid.NewGuid():N}";
        using var ctx = CreateContext(dbName);
        var creator = ctx.GetService<IRelationalDatabaseCreator>();

        Assert.False(await creator.ExistsAsync());
    }

    [Fact]
    public async Task CreateAsync_And_ExistsAsync_Work()
    {
        var dbName = $"test_create_{Guid.NewGuid():N}";
        using var ctx = CreateContext(dbName);
        var creator = ctx.GetService<IRelationalDatabaseCreator>();

        try
        {
            Assert.False(await creator.ExistsAsync());
            await creator.CreateAsync();
            Assert.True(await creator.ExistsAsync());
        }
        finally
        {
            await creator.DeleteAsync();
        }
    }

    [Fact]
    public async Task DeleteAsync_RemovesDatabase()
    {
        var dbName = $"test_delete_{Guid.NewGuid():N}";
        using var ctx = CreateContext(dbName);
        var creator = ctx.GetService<IRelationalDatabaseCreator>();

        await creator.CreateAsync();
        Assert.True(await creator.ExistsAsync());

        await creator.DeleteAsync();
        Assert.False(await creator.ExistsAsync());
    }

    [Fact]
    public async Task HasTablesAsync_ReturnsFalseForEmptyDatabase()
    {
        var dbName = $"test_empty_{Guid.NewGuid():N}";
        using var ctx = CreateContext(dbName);
        var creator = ctx.GetService<IRelationalDatabaseCreator>();

        await creator.CreateAsync();
        try
        {
            Assert.False(await creator.HasTablesAsync());
        }
        finally
        {
            await creator.DeleteAsync();
        }
    }

    [Fact]
    public async Task HasTablesAsync_ReturnsTrueWhenTablesExist()
    {
        var dbName = $"test_tables_{Guid.NewGuid():N}";
        using var ctx = CreateContext(dbName);
        var creator = ctx.GetService<IRelationalDatabaseCreator>();

        await creator.CreateAsync();
        try
        {
            // Create a table via raw SQL
            var connStr = new global::ClickHouse.Driver.ADO.ClickHouseConnectionStringBuilder(
                _fixture.ConnectionString)
            {
                Database = dbName
            }.ConnectionString;

            using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(connStr);
            await connection.OpenAsync();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "CREATE TABLE test_t (id Int64) ENGINE = MergeTree() ORDER BY id";
            await cmd.ExecuteNonQueryAsync();

            Assert.True(await creator.HasTablesAsync());
        }
        finally
        {
            await creator.DeleteAsync();
        }
    }

    // --- Sync Methods ---

    [Fact]
    public void Exists_ReturnsFalseForNonExistentDatabase()
    {
        var dbName = $"test_syncne_{Guid.NewGuid():N}";
        using var ctx = CreateContext(dbName);
        var creator = ctx.GetService<IRelationalDatabaseCreator>();

        Assert.False(creator.Exists());
    }

    [Fact]
    public void Create_And_Exists_Work()
    {
        var dbName = $"test_synccr_{Guid.NewGuid():N}";
        using var ctx = CreateContext(dbName);
        var creator = ctx.GetService<IRelationalDatabaseCreator>();

        try
        {
            Assert.False(creator.Exists());
            creator.Create();
            Assert.True(creator.Exists());
        }
        finally
        {
            creator.Delete();
        }
    }

    [Fact]
    public void Delete_RemovesDatabase()
    {
        var dbName = $"test_syncdl_{Guid.NewGuid():N}";
        using var ctx = CreateContext(dbName);
        var creator = ctx.GetService<IRelationalDatabaseCreator>();

        creator.Create();
        Assert.True(creator.Exists());

        creator.Delete();
        Assert.False(creator.Exists());
    }

    [Fact]
    public void HasTables_ReturnsFalseForEmptyDatabase()
    {
        var dbName = $"test_syncem_{Guid.NewGuid():N}";
        using var ctx = CreateContext(dbName);
        var creator = ctx.GetService<IRelationalDatabaseCreator>();

        creator.Create();
        try
        {
            Assert.False(creator.HasTables());
        }
        finally
        {
            creator.Delete();
        }
    }

    [Fact]
    public async Task CreateAsync_IsIdempotent_WithIfNotExists()
    {
        var dbName = $"test_idemp_{Guid.NewGuid():N}";
        using var ctx = CreateContext(dbName);
        var creator = ctx.GetService<IRelationalDatabaseCreator>();

        try
        {
            await creator.CreateAsync();
            // Calling Create again should not throw (uses CREATE DATABASE IF NOT EXISTS)
            await creator.CreateAsync();
            Assert.True(await creator.ExistsAsync());
        }
        finally
        {
            await creator.DeleteAsync();
        }
    }

    [Fact]
    public async Task DeleteAsync_IsIdempotent_WithIfExists()
    {
        var dbName = $"test_dlidm_{Guid.NewGuid():N}";
        using var ctx = CreateContext(dbName);
        var creator = ctx.GetService<IRelationalDatabaseCreator>();

        await creator.CreateAsync();
        await creator.DeleteAsync();
        // Calling Delete again should not throw (uses DROP DATABASE IF EXISTS)
        await creator.DeleteAsync();
        Assert.False(await creator.ExistsAsync());
    }
}
