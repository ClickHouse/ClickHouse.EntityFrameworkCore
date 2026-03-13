using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EFCore.ClickHouse.Tests;

public class FloatSpecialEntity
{
    public long Id { get; set; }
    public float ValFloat32 { get; set; }
    public double ValFloat64 { get; set; }
}

public class FloatSpecialDbContext : DbContext
{
    public DbSet<FloatSpecialEntity> FloatSpecials => Set<FloatSpecialEntity>();

    private readonly string _connectionString;

    public FloatSpecialDbContext(string connectionString)
    {
        _connectionString = connectionString;
    }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseClickHouse(_connectionString);
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<FloatSpecialEntity>(entity =>
        {
            entity.ToTable("float_specials");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.ValFloat32).HasColumnName("val_float32");
            entity.Property(e => e.ValFloat64).HasColumnName("val_float64");
        });
    }
}

public class FloatSpecialFixture : IAsyncLifetime
{
    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        ConnectionString = await SharedContainer.GetConnectionStringAsync();

        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(ConnectionString);
        await connection.OpenAsync();

        using var createCmd = connection.CreateCommand();
        createCmd.CommandText = """
            CREATE TABLE float_specials (
                id Int64,
                val_float32 Float32,
                val_float64 Float64
            ) ENGINE = MergeTree()
            ORDER BY id
            """;
        await createCmd.ExecuteNonQueryAsync();

        using var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = """
            INSERT INTO float_specials VALUES
            (1, CAST('NaN' AS Float32), CAST('NaN' AS Float64)),
            (2, CAST('Inf' AS Float32), CAST('Inf' AS Float64)),
            (3, CAST('-Inf' AS Float32), CAST('-Inf' AS Float64)),
            (4, 0.0, 0.0),
            (5, 3.14, 2.718281828459045),
            (6, -1.5, -1.5)
            """;
        await insertCmd.ExecuteNonQueryAsync();
    }

    public Task DisposeAsync() => Task.CompletedTask;
}

/// <summary>
/// Integration tests for Float32/Float64 special values (NaN, +Infinity, -Infinity)
/// round-tripping through ClickHouse.
/// </summary>
public class FloatSpecialValueTests : IClassFixture<FloatSpecialFixture>
{
    private readonly FloatSpecialFixture _fixture;

    public FloatSpecialValueTests(FloatSpecialFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task ReadAll_DeserializesSpecialValues()
    {
        await using var ctx = new FloatSpecialDbContext(_fixture.ConnectionString);

        var rows = await ctx.FloatSpecials
            .OrderBy(e => e.Id)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(6, rows.Count);

        // Row 1: NaN
        Assert.True(float.IsNaN(rows[0].ValFloat32), "Float32 NaN not deserialized");
        Assert.True(double.IsNaN(rows[0].ValFloat64), "Float64 NaN not deserialized");

        // Row 2: +Infinity
        Assert.True(float.IsPositiveInfinity(rows[1].ValFloat32), "Float32 +Inf not deserialized");
        Assert.True(double.IsPositiveInfinity(rows[1].ValFloat64), "Float64 +Inf not deserialized");

        // Row 3: -Infinity
        Assert.True(float.IsNegativeInfinity(rows[2].ValFloat32), "Float32 -Inf not deserialized");
        Assert.True(double.IsNegativeInfinity(rows[2].ValFloat64), "Float64 -Inf not deserialized");

        // Row 4: Zero
        Assert.Equal(0.0f, rows[3].ValFloat32);
        Assert.Equal(0.0, rows[3].ValFloat64);

        // Row 5: Normal positive
        Assert.Equal(3.14f, rows[4].ValFloat32, 0.001f);
        Assert.Equal(2.718281828459045, rows[4].ValFloat64, 1e-10);

        // Row 6: Normal negative
        Assert.Equal(-1.5f, rows[5].ValFloat32);
        Assert.Equal(-1.5, rows[5].ValFloat64);
    }

    [Fact]
    public async Task NaN_ReadsCorrectly()
    {
        await using var ctx = new FloatSpecialDbContext(_fixture.ConnectionString);

        var row = await ctx.FloatSpecials
            .AsNoTracking()
            .SingleAsync(e => e.Id == 1);

        Assert.True(float.IsNaN(row.ValFloat32));
        Assert.True(double.IsNaN(row.ValFloat64));
    }

    [Fact]
    public async Task PositiveInfinity_ReadsCorrectly()
    {
        await using var ctx = new FloatSpecialDbContext(_fixture.ConnectionString);

        var row = await ctx.FloatSpecials
            .AsNoTracking()
            .SingleAsync(e => e.Id == 2);

        Assert.True(float.IsPositiveInfinity(row.ValFloat32));
        Assert.True(double.IsPositiveInfinity(row.ValFloat64));
    }

    [Fact]
    public async Task NegativeInfinity_ReadsCorrectly()
    {
        await using var ctx = new FloatSpecialDbContext(_fixture.ConnectionString);

        var row = await ctx.FloatSpecials
            .AsNoTracking()
            .SingleAsync(e => e.Id == 3);

        Assert.True(float.IsNegativeInfinity(row.ValFloat32));
        Assert.True(double.IsNegativeInfinity(row.ValFloat64));
    }

    [Fact]
    public async Task Comparison_ExcludesNaN()
    {
        await using var ctx = new FloatSpecialDbContext(_fixture.ConnectionString);

        // In IEEE 754, NaN comparisons always return false.
        // ClickHouse follows this: NaN is NOT > 0, NOT <= 0, NOT == 0.
        var countPositive = await ctx.FloatSpecials
            .Where(e => e.ValFloat32 > 0.0f)
            .CountAsync();

        // +Inf (id=2) and 3.14 (id=5) are > 0. NaN is not.
        Assert.Equal(2, countPositive);
    }

    [Fact]
    public async Task Comparison_InfinityIsGreaterThanNormal()
    {
        await using var ctx = new FloatSpecialDbContext(_fixture.ConnectionString);

        var results = await ctx.FloatSpecials
            .Where(e => e.ValFloat64 > 100.0)
            .AsNoTracking()
            .ToListAsync();

        // Only +Infinity (id=2) is > 100. NaN is not.
        Assert.Single(results);
        Assert.Equal(2, results[0].Id);
        Assert.True(double.IsPositiveInfinity(results[0].ValFloat64));
    }

    [Fact]
    public async Task Comparison_NegativeInfinityIsLessThanAll()
    {
        await using var ctx = new FloatSpecialDbContext(_fixture.ConnectionString);

        var results = await ctx.FloatSpecials
            .Where(e => e.ValFloat64 < -100.0)
            .AsNoTracking()
            .ToListAsync();

        // Only -Infinity (id=3) is < -100.
        Assert.Single(results);
        Assert.Equal(3, results[0].Id);
        Assert.True(double.IsNegativeInfinity(results[0].ValFloat64));
    }

    [Fact]
    public async Task OrderBy_SpecialValuesSort()
    {
        await using var ctx = new FloatSpecialDbContext(_fixture.ConnectionString);

        var results = await ctx.FloatSpecials
            .OrderBy(e => e.ValFloat64)
            .Select(e => e.Id)
            .ToListAsync();

        // ClickHouse sort order: -Inf, -1.5, 0.0, 2.718..., +Inf, NaN
        // NaN sorts last in ClickHouse
        Assert.Equal(6, results.Count);
        Assert.Equal(3, results[0]); // -Inf
        Assert.Equal(1, results[5]); // NaN (sorts last)
    }
}
