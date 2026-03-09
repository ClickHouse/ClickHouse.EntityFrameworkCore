using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EFCore.ClickHouse.Tests;

public class TestEntity
{
    public long Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public int Age { get; set; }
    public bool IsActive { get; set; }
}

public class TestDbContext : DbContext
{
    public DbSet<TestEntity> TestEntities => Set<TestEntity>();

    private readonly string _connectionString;

    public TestDbContext(string connectionString)
    {
        _connectionString = connectionString;
    }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseClickHouse(_connectionString);
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<TestEntity>(entity =>
        {
            entity.ToTable("test_entities");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.Name).HasColumnName("name");
            entity.Property(e => e.Age).HasColumnName("age");
            entity.Property(e => e.IsActive).HasColumnName("is_active");
        });
    }
}

public class ClickHouseFixture : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder("clickhouse/clickhouse-server:latest").Build();

    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        ConnectionString = _container.GetConnectionString();

        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(ConnectionString);
        await connection.OpenAsync();

        using var createCmd = connection.CreateCommand();
        createCmd.CommandText = """
            CREATE TABLE test_entities (
                id Int64,
                name String,
                age Int32,
                is_active Bool
            ) ENGINE = MergeTree()
            ORDER BY id
            """;
        await createCmd.ExecuteNonQueryAsync();

        using var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = """
            INSERT INTO test_entities (id, name, age, is_active) VALUES
            (1, 'Alice', 30, 1),
            (2, 'Bob', 25, 1),
            (3, 'Charlie', 35, 0),
            (4, 'Diana', 28, 1),
            (5, 'Eve', 22, 0),
            (6, 'Frank', 40, 1),
            (7, 'Grace', 33, 1),
            (8, 'Hank', 27, 0),
            (9, 'Ivy', 31, 1),
            (10, 'Jack', 29, 0)
            """;
        await insertCmd.ExecuteNonQueryAsync();
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }
}

public class ReadQueryIntegrationTests : IClassFixture<ClickHouseFixture>
{
    private readonly ClickHouseFixture _fixture;

    public ReadQueryIntegrationTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Where_OrderBy_Take_ReturnsCorrectResults()
    {
        await using var context = new TestDbContext(_fixture.ConnectionString);

        // Age > 25: Alice(30), Charlie(35), Diana(28), Frank(40), Grace(33), Hank(27), Ivy(31), Jack(29)
        // Ordered by Name: Alice, Charlie, Diana
        var results = await context.TestEntities
            .Where(e => e.Age > 25)
            .OrderBy(e => e.Name)
            .Take(3)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(3, results.Count);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal("Charlie", results[1].Name);
        Assert.Equal("Diana", results[2].Name);
    }

    [Fact]
    public async Task Where_Bool_ReturnsCorrectResults()
    {
        await using var context = new TestDbContext(_fixture.ConnectionString);

        var results = await context.TestEntities
            .Where(e => e.IsActive)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(6, results.Count);
        Assert.All(results, r => Assert.True(r.IsActive));
    }

    [Fact]
    public async Task Skip_Take_ReturnsCorrectResults()
    {
        await using var context = new TestDbContext(_fixture.ConnectionString);

        var results = await context.TestEntities
            .OrderBy(e => e.Id)
            .Skip(2)
            .Take(3)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(3, results.Count);
        Assert.Equal(3, results[0].Id);
        Assert.Equal(4, results[1].Id);
        Assert.Equal(5, results[2].Id);
    }

    [Fact]
    public async Task FirstOrDefault_ReturnsCorrectResult()
    {
        await using var context = new TestDbContext(_fixture.ConnectionString);

        var result = await context.TestEntities
            .OrderBy(e => e.Id)
            .AsNoTracking()
            .FirstOrDefaultAsync();

        Assert.NotNull(result);
        Assert.Equal(1, result.Id);
        Assert.Equal("Alice", result.Name);
    }

    [Fact]
    public async Task Select_Projection_ReturnsCorrectResults()
    {
        await using var context = new TestDbContext(_fixture.ConnectionString);

        var results = await context.TestEntities
            .Where(e => e.Age > 30)
            .Select(e => new { e.Name, e.Age })
            .OrderBy(e => e.Age)
            .AsNoTracking()
            .ToListAsync();

        Assert.True(results.Count >= 3);
        Assert.All(results, r => Assert.True(r.Age > 30));
    }

    [Fact]
    public async Task Where_WithIntComparison_ReturnsCorrectResults()
    {
        await using var context = new TestDbContext(_fixture.ConnectionString);

        var results = await context.TestEntities
            .Where(e => e.Id > 5)
            .OrderBy(e => e.Name)
            .Take(10)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(5, results.Count);
        Assert.All(results, r => Assert.True(r.Id > 5));
    }

    [Fact]
    public async Task SingleOrDefault_WithPredicate_ReturnsCorrectResult()
    {
        await using var context = new TestDbContext(_fixture.ConnectionString);

        var result = await context.TestEntities
            .AsNoTracking()
            .SingleOrDefaultAsync(e => e.Name == "Alice");

        Assert.NotNull(result);
        Assert.Equal(1, result.Id);
        Assert.Equal(30, result.Age);
        Assert.True(result.IsActive);
    }

    [Fact]
    public async Task Count_ReturnsCorrectResult()
    {
        await using var context = new TestDbContext(_fixture.ConnectionString);

        var count = await context.TestEntities
            .Where(e => e.IsActive)
            .CountAsync();

        Assert.Equal(6, count);
    }
}
