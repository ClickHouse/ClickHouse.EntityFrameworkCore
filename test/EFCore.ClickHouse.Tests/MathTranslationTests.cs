using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EFCore.ClickHouse.Tests;

public class FloatEntity
{
    public long Id { get; set; }
    public string Label { get; set; } = string.Empty;
    public double ValFloat64 { get; set; }
    public float ValFloat32 { get; set; }
}

public class FloatDbContext : DbContext
{
    public DbSet<FloatEntity> Floats => Set<FloatEntity>();

    private readonly string _connectionString;

    public FloatDbContext(string connectionString)
    {
        _connectionString = connectionString;
    }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseClickHouse(_connectionString);
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<FloatEntity>(entity =>
        {
            entity.ToTable("float_test");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.Label).HasColumnName("label");
            entity.Property(e => e.ValFloat64).HasColumnName("val_f64");
            entity.Property(e => e.ValFloat32).HasColumnName("val_f32");
        });
    }
}

public class FloatFixture : IAsyncLifetime
{
    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        ConnectionString = await SharedContainer.GetConnectionStringAsync();

        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(ConnectionString);
        await connection.OpenAsync();

        using var createCmd = connection.CreateCommand();
        createCmd.CommandText = """
            CREATE TABLE float_test (
                id Int64,
                label String,
                val_f64 Float64,
                val_f32 Float32
            ) ENGINE = MergeTree()
            ORDER BY id
            """;
        await createCmd.ExecuteNonQueryAsync();

        using var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = """
            INSERT INTO float_test (id, label, val_f64, val_f32) VALUES
            (1, 'normal', 42.5, 42.5),
            (2, 'negative', -10.0, -10.0),
            (3, 'pos_inf', inf, inf),
            (4, 'neg_inf', -inf, -inf),
            (5, 'nan', nan, nan),
            (6, 'zero', 0.0, 0.0)
            """;
        await insertCmd.ExecuteNonQueryAsync();
    }

    public Task DisposeAsync() => Task.CompletedTask;
}

public class MathTranslationTests : IClassFixture<ClickHouseFixture>
{
    private readonly ClickHouseFixture _fixture;

    public MathTranslationTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Math_Abs_TranslatesCorrectly()
    {
        await using var context = new TestDbContext(_fixture.ConnectionString);

        // Age - 30: Alice=0, Bob=-5, Charlie=5, ...
        var results = await context.TestEntities
            .Select(e => new { e.Name, AbsDiff = Math.Abs(e.Age - 30) })
            .OrderBy(x => x.Name)
            .AsNoTracking()
            .ToListAsync();

        var alice = results.First(r => r.Name == "Alice");
        Assert.Equal(0, alice.AbsDiff); // |30-30|
        var bob = results.First(r => r.Name == "Bob");
        Assert.Equal(5, bob.AbsDiff); // |25-30|
    }

    [Fact]
    public async Task Math_Floor_TranslatesCorrectly()
    {
        await using var context = new TestDbContext(_fixture.ConnectionString);

        var results = await context.TestEntities
            .Select(e => new { e.Name, Floored = Math.Floor((double)e.Age / 3.0) })
            .OrderBy(x => x.Name)
            .AsNoTracking()
            .ToListAsync();

        var alice = results.First(r => r.Name == "Alice");
        Assert.Equal(10.0, alice.Floored); // floor(30/3) = 10
        var bob = results.First(r => r.Name == "Bob");
        Assert.Equal(8.0, bob.Floored); // floor(25/3) = 8
    }

    [Fact]
    public async Task Math_Ceiling_TranslatesCorrectly()
    {
        await using var context = new TestDbContext(_fixture.ConnectionString);

        var results = await context.TestEntities
            .Select(e => new { e.Name, Ceiled = Math.Ceiling((double)e.Age / 3.0) })
            .OrderBy(x => x.Name)
            .AsNoTracking()
            .ToListAsync();

        var alice = results.First(r => r.Name == "Alice");
        Assert.Equal(10.0, alice.Ceiled); // ceil(30/3) = 10
        var bob = results.First(r => r.Name == "Bob");
        Assert.Equal(9.0, bob.Ceiled); // ceil(25/3) = 9 (8.333... → 9)
    }

    [Fact]
    public async Task Math_Round_NoDecimals_TranslatesCorrectly()
    {
        await using var context = new TestDbContext(_fixture.ConnectionString);

        var results = await context.TestEntities
            .Select(e => new { e.Name, Rounded = Math.Round((double)e.Age / 3.0) })
            .OrderBy(x => x.Name)
            .AsNoTracking()
            .ToListAsync();

        var alice = results.First(r => r.Name == "Alice");
        Assert.Equal(10.0, alice.Rounded); // round(10.0) = 10
        var bob = results.First(r => r.Name == "Bob");
        Assert.Equal(8.0, bob.Rounded); // round(8.333) = 8
    }

    [Fact]
    public async Task Math_Round_WithDecimals_TranslatesCorrectly()
    {
        await using var context = new TestDbContext(_fixture.ConnectionString);

        var results = await context.TestEntities
            .Select(e => new { e.Name, Rounded = Math.Round((double)e.Age / 3.0, 2) })
            .OrderBy(x => x.Name)
            .AsNoTracking()
            .ToListAsync();

        var bob = results.First(r => r.Name == "Bob");
        Assert.Equal(8.33, bob.Rounded); // round(25/3, 2) = 8.33
    }

    [Fact]
    public async Task Math_Sqrt_TranslatesCorrectly()
    {
        await using var context = new TestDbContext(_fixture.ConnectionString);

        var results = await context.TestEntities
            .Select(e => new { e.Name, SqrtAge = Math.Sqrt(e.Age) })
            .OrderBy(x => x.Name)
            .AsNoTracking()
            .ToListAsync();

        var eve = results.First(r => r.Name == "Eve");
        Assert.Equal(Math.Sqrt(22), eve.SqrtAge, 10);
        var frank = results.First(r => r.Name == "Frank");
        Assert.Equal(Math.Sqrt(40), frank.SqrtAge, 10);
    }

    [Fact]
    public async Task Math_Pow_TranslatesCorrectly()
    {
        await using var context = new TestDbContext(_fixture.ConnectionString);

        var results = await context.TestEntities
            .Where(e => e.Name == "Bob")
            .Select(e => new { e.Name, Squared = Math.Pow(e.Age, 2) })
            .AsNoTracking()
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(625.0, results[0].Squared); // 25^2
    }

    [Fact]
    public async Task Math_Log_TranslatesCorrectly()
    {
        await using var context = new TestDbContext(_fixture.ConnectionString);

        var results = await context.TestEntities
            .Where(e => e.Name == "Alice")
            .Select(e => new { e.Name, LogAge = Math.Log(e.Age) })
            .AsNoTracking()
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(Math.Log(30), results[0].LogAge, 8);
    }

    [Fact]
    public async Task Math_Log10_TranslatesCorrectly()
    {
        await using var context = new TestDbContext(_fixture.ConnectionString);

        var results = await context.TestEntities
            .Where(e => e.Name == "Alice")
            .Select(e => new { e.Name, Log10Age = Math.Log10(e.Age) })
            .AsNoTracking()
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(Math.Log10(30), results[0].Log10Age, 10);
    }

    [Fact]
    public async Task Math_Exp_TranslatesCorrectly()
    {
        await using var context = new TestDbContext(_fixture.ConnectionString);

        // Use a small value to avoid overflow
        var results = await context.TestEntities
            .Where(e => e.Name == "Bob")
            .Select(e => new { e.Name, ExpVal = Math.Exp((double)e.Age / 10.0) })
            .AsNoTracking()
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(Math.Exp(2.5), results[0].ExpVal, 10);
    }

    [Fact]
    public async Task Math_Sign_TranslatesCorrectly()
    {
        await using var context = new TestDbContext(_fixture.ConnectionString);

        var results = await context.TestEntities
            .Where(e => e.Name == "Alice")
            .Select(e => new { e.Name, SignVal = Math.Sign(e.Age - 30) })
            .AsNoTracking()
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(0, results[0].SignVal); // sign(30-30) = 0
    }

    [Fact]
    public async Task Math_SinCos_TranslateCorrectly()
    {
        await using var context = new TestDbContext(_fixture.ConnectionString);

        var results = await context.TestEntities
            .Where(e => e.Name == "Alice")
            .Select(e => new
            {
                e.Name,
                SinVal = Math.Sin((double)e.Age),
                CosVal = Math.Cos((double)e.Age),
            })
            .AsNoTracking()
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(Math.Sin(30.0), results[0].SinVal, 10);
        Assert.Equal(Math.Cos(30.0), results[0].CosVal, 10);
    }

    [Fact]
    public async Task Math_Truncate_TranslatesCorrectly()
    {
        await using var context = new TestDbContext(_fixture.ConnectionString);

        var results = await context.TestEntities
            .Select(e => new { e.Name, Truncated = Math.Truncate((double)e.Age / 3.0) })
            .OrderBy(x => x.Name)
            .AsNoTracking()
            .ToListAsync();

        var bob = results.First(r => r.Name == "Bob");
        Assert.Equal(8.0, bob.Truncated); // truncate(8.333) = 8
    }

    [Fact]
    public async Task Math_Where_WithMathFunction()
    {
        await using var context = new TestDbContext(_fixture.ConnectionString);

        // Filter: sqrt(age) > 5.5 → age > 30.25 → Alice(30 no), Charlie(35 yes), Frank(40 yes), Grace(33 yes), Ivy(31 yes)
        var results = await context.TestEntities
            .Where(e => Math.Sqrt(e.Age) > 5.5)
            .OrderBy(e => e.Name)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(4, results.Count);
        Assert.Equal("Charlie", results[0].Name);
        Assert.Equal("Frank", results[1].Name);
        Assert.Equal("Grace", results[2].Name);
        Assert.Equal("Ivy", results[3].Name);
    }
}

/// <summary>
/// Tests for math translator special cases that aren't in the simple
/// dictionary lookup: Log(x, base), IsPositiveInfinity, IsNegativeInfinity.
/// </summary>
public class MathSpecialCaseTests : IClassFixture<FloatFixture>
{
    private readonly FloatFixture _fixture;

    public MathSpecialCaseTests(FloatFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Math_LogWithBase_TranslatesCorrectly()
    {
        // Math.Log(x, base) → log(x) / log(base)
        await using var context = new FloatDbContext(_fixture.ConnectionString);

        var results = await context.Floats
            .Where(e => e.Label == "normal")
            .Select(e => new { e.Label, LogBase10 = Math.Log(e.ValFloat64, 10) })
            .AsNoTracking()
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(Math.Log(42.5, 10), results[0].LogBase10, 8);
    }

    [Fact]
    public async Task Math_LogWithBase2_TranslatesCorrectly()
    {
        await using var context = new FloatDbContext(_fixture.ConnectionString);

        var results = await context.Floats
            .Where(e => e.Label == "normal")
            .Select(e => new { e.Label, LogBase2 = Math.Log(e.ValFloat64, 2) })
            .AsNoTracking()
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(Math.Log(42.5, 2), results[0].LogBase2, 6);
    }

    [Fact]
    public async Task Double_IsPositiveInfinity_FiltersCorrectly()
    {
        // double.IsPositiveInfinity(x) → isInfinite(x) AND x > 0
        await using var context = new FloatDbContext(_fixture.ConnectionString);

        var results = await context.Floats
            .Where(e => double.IsPositiveInfinity(e.ValFloat64))
            .AsNoTracking()
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal("pos_inf", results[0].Label);
    }

    [Fact]
    public async Task Double_IsNegativeInfinity_FiltersCorrectly()
    {
        // double.IsNegativeInfinity(x) → isInfinite(x) AND x < 0
        await using var context = new FloatDbContext(_fixture.ConnectionString);

        var results = await context.Floats
            .Where(e => double.IsNegativeInfinity(e.ValFloat64))
            .AsNoTracking()
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal("neg_inf", results[0].Label);
    }

    [Fact]
    public async Task Float_IsPositiveInfinity_FiltersCorrectly()
    {
        // float.IsPositiveInfinity(x) → isInfinite(x) AND x > 0
        await using var context = new FloatDbContext(_fixture.ConnectionString);

        var results = await context.Floats
            .Where(e => float.IsPositiveInfinity(e.ValFloat32))
            .AsNoTracking()
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal("pos_inf", results[0].Label);
    }

    [Fact]
    public async Task Float_IsNegativeInfinity_FiltersCorrectly()
    {
        // float.IsNegativeInfinity(x) → isInfinite(x) AND x < 0
        await using var context = new FloatDbContext(_fixture.ConnectionString);

        var results = await context.Floats
            .Where(e => float.IsNegativeInfinity(e.ValFloat32))
            .AsNoTracking()
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal("neg_inf", results[0].Label);
    }
}
