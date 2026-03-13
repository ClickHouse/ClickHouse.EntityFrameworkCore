using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EFCore.ClickHouse.Tests;

public class DateTime64Entity
{
    public long Id { get; set; }
    public DateTime Timestamp { get; set; }
}

public class DateTimeTzEntity
{
    public long Id { get; set; }
    public DateTime Created { get; set; }
}

public class FixedStringEntity
{
    public long Id { get; set; }
    public string Code { get; set; } = string.Empty;
}

public class ParameterizedTypeDbContext : DbContext
{
    public DbSet<DateTime64Entity> DateTime64Entities => Set<DateTime64Entity>();
    public DbSet<DateTimeTzEntity> DateTimeTzEntities => Set<DateTimeTzEntity>();
    public DbSet<FixedStringEntity> FixedStringEntities => Set<FixedStringEntity>();

    private readonly string _connectionString;

    public ParameterizedTypeDbContext(string connectionString)
    {
        _connectionString = connectionString;
    }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseClickHouse(_connectionString);
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<DateTime64Entity>(entity =>
        {
            entity.ToTable("dt64_test");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.Timestamp).HasColumnName("ts").HasColumnType("DateTime64(6, 'UTC')");
        });

        modelBuilder.Entity<DateTimeTzEntity>(entity =>
        {
            entity.ToTable("dt_tz_test");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.Created).HasColumnName("created").HasColumnType("DateTime('UTC')");
        });

        modelBuilder.Entity<FixedStringEntity>(entity =>
        {
            entity.ToTable("fs_test");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.Code).HasColumnName("code").HasColumnType("FixedString(10)");
        });
    }
}

public class ParameterizedTypeFixture : IAsyncLifetime
{
    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        ConnectionString = await SharedContainer.GetConnectionStringAsync();

        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(ConnectionString);
        await connection.OpenAsync();

        using var cmd1 = connection.CreateCommand();
        cmd1.CommandText = """
            CREATE TABLE dt64_test (
                id Int64,
                ts DateTime64(6, 'UTC')
            ) ENGINE = MergeTree()
            ORDER BY id
            """;
        await cmd1.ExecuteNonQueryAsync();

        using var cmd2 = connection.CreateCommand();
        cmd2.CommandText = """
            CREATE TABLE dt_tz_test (
                id Int64,
                created DateTime('UTC')
            ) ENGINE = MergeTree()
            ORDER BY id
            """;
        await cmd2.ExecuteNonQueryAsync();

        using var cmd3 = connection.CreateCommand();
        cmd3.CommandText = """
            CREATE TABLE fs_test (
                id Int64,
                code FixedString(10)
            ) ENGINE = MergeTree()
            ORDER BY id
            """;
        await cmd3.ExecuteNonQueryAsync();

        using var insert1 = connection.CreateCommand();
        insert1.CommandText = """
            INSERT INTO dt64_test (id, ts) VALUES
            (1, '2024-06-15 10:30:45.123456'),
            (2, '2024-06-15 12:00:00.000000'),
            (3, '2024-06-15 23:59:59.999999')
            """;
        await insert1.ExecuteNonQueryAsync();

        using var insert2 = connection.CreateCommand();
        insert2.CommandText = """
            INSERT INTO dt_tz_test (id, created) VALUES
            (1, '2024-06-15 10:30:45'),
            (2, '2024-06-15 12:00:00')
            """;
        await insert2.ExecuteNonQueryAsync();

        using var insert3 = connection.CreateCommand();
        insert3.CommandText = """
            INSERT INTO fs_test (id, code) VALUES
            (1, 'ABC'),
            (2, 'XYZXYZXYZ0'),
            (3, 'HELLO')
            """;
        await insert3.ExecuteNonQueryAsync();
    }

    public Task DisposeAsync() => Task.CompletedTask;
}

public class ParameterizedTypeMappingTests : IClassFixture<ParameterizedTypeFixture>
{
    private readonly ParameterizedTypeFixture _fixture;

    public ParameterizedTypeMappingTests(ParameterizedTypeFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task DateTime64_WithPrecisionAndTimezone_ReturnsCorrectValues()
    {
        await using var context = new ParameterizedTypeDbContext(_fixture.ConnectionString);

        var results = await context.DateTime64Entities
            .OrderBy(e => e.Id)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(3, results.Count);
        Assert.Equal(new DateTime(2024, 6, 15, 10, 30, 45, 123).AddTicks(4560), results[0].Timestamp);
        Assert.Equal(new DateTime(2024, 6, 15, 12, 0, 0), results[1].Timestamp);
    }

    [Fact]
    public async Task DateTime_WithTimezone_ReturnsCorrectValues()
    {
        await using var context = new ParameterizedTypeDbContext(_fixture.ConnectionString);

        var results = await context.DateTimeTzEntities
            .OrderBy(e => e.Id)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Equal(new DateTime(2024, 6, 15, 10, 30, 45), results[0].Created);
        Assert.Equal(new DateTime(2024, 6, 15, 12, 0, 0), results[1].Created);
    }

    [Fact]
    public async Task FixedString_ReturnsCorrectValues()
    {
        await using var context = new ParameterizedTypeDbContext(_fixture.ConnectionString);

        var results = await context.FixedStringEntities
            .OrderBy(e => e.Id)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(3, results.Count);
        // FixedString pads with null bytes — the driver may return them trimmed or padded
        Assert.StartsWith("ABC", results[0].Code);
        Assert.StartsWith("XYZXYZXYZ0", results[1].Code);
        Assert.StartsWith("HELLO", results[2].Code);
    }

    [Fact]
    public async Task DateTime64_Where_FiltersCorrectly()
    {
        await using var context = new ParameterizedTypeDbContext(_fixture.ConnectionString);

        var cutoff = new DateTime(2024, 6, 15, 12, 0, 0);
        var results = await context.DateTime64Entities
            .Where(e => e.Timestamp >= cutoff)
            .OrderBy(e => e.Id)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Equal(2, results[0].Id);
        Assert.Equal(3, results[1].Id);
    }

    [Fact]
    public async Task FixedString_Where_FiltersCorrectly()
    {
        await using var context = new ParameterizedTypeDbContext(_fixture.ConnectionString);

        var result = await context.FixedStringEntities
            .AsNoTracking()
            .SingleOrDefaultAsync(e => e.Id == 2);

        Assert.NotNull(result);
        Assert.StartsWith("XYZXYZXYZ0", result.Code);
    }
}
