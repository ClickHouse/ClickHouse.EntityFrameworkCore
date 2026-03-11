using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.ClickHouse.Tests;

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
