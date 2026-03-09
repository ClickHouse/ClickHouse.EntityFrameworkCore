using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.ClickHouse.Tests;

/// <summary>
/// Tests complex query patterns and edge cases using the shared ClickHouseFixture (test_entities table).
/// </summary>
public class QueryPatternEdgeCaseTests : IClassFixture<ClickHouseFixture>
{
    private readonly ClickHouseFixture _fixture;

    public QueryPatternEdgeCaseTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    // --- Chained / Complex Where ---

    [Fact]
    public async Task ChainedWhere_MultipleFilters()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var results = await ctx.TestEntities
            .Where(e => e.Age > 25)
            .Where(e => e.IsActive)
            .OrderBy(e => e.Name)
            .AsNoTracking()
            .ToListAsync();

        Assert.All(results, r =>
        {
            Assert.True(r.Age > 25);
            Assert.True(r.IsActive);
        });
    }

    [Fact]
    public async Task ComplexBoolean_AndOrCombination()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        // (Age > 35 AND IsActive) OR Name == "Eve"
        var results = await ctx.TestEntities
            .Where(e => (e.Age > 35 && e.IsActive) || e.Name == "Eve")
            .OrderBy(e => e.Name)
            .AsNoTracking()
            .ToListAsync();

        // Frank(40, active), Eve(22, inactive)
        Assert.Equal(2, results.Count);
        Assert.Equal("Eve", results[0].Name);
        Assert.Equal("Frank", results[1].Name);
    }

    [Fact]
    public async Task Not_Operator()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var results = await ctx.TestEntities
            .Where(e => !e.IsActive)
            .OrderBy(e => e.Name)
            .AsNoTracking()
            .ToListAsync();

        // Charlie(35, inactive), Eve(22, inactive), Hank(27, inactive), Jack(29, inactive)
        Assert.Equal(4, results.Count);
        Assert.All(results, r => Assert.False(r.IsActive));
    }

    [Fact]
    public async Task Where_MultipleConditions_SameField()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var results = await ctx.TestEntities
            .Where(e => e.Age >= 28 && e.Age <= 31)
            .OrderBy(e => e.Age)
            .AsNoTracking()
            .ToListAsync();

        // Diana(28), Jack(29), Alice(30), Ivy(31)
        Assert.Equal(4, results.Count);
        Assert.Equal(28, results[0].Age);
        Assert.Equal(31, results[3].Age);
    }

    // --- OrderBy Variants ---

    [Fact]
    public async Task OrderByDescending()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var results = await ctx.TestEntities
            .OrderByDescending(e => e.Age)
            .Take(3)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(40, results[0].Age); // Frank
        Assert.Equal(35, results[1].Age); // Charlie
        Assert.Equal(33, results[2].Age); // Grace
    }

    [Fact]
    public async Task ThenBy_SecondarySort()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var results = await ctx.TestEntities
            .OrderBy(e => e.IsActive)
            .ThenBy(e => e.Name)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(10, results.Count);
        // Inactive first (Bool 0 < 1), then by name within each group
        // Inactive: Charlie, Eve, Hank, Jack
        Assert.Equal("Charlie", results[0].Name);
        Assert.False(results[0].IsActive);
        Assert.Equal("Jack", results[3].Name);
        Assert.False(results[3].IsActive);
        // Active: Alice, Bob, Diana, Frank, Grace, Ivy
        Assert.Equal("Alice", results[4].Name);
        Assert.True(results[4].IsActive);
    }

    [Fact]
    public async Task ThenByDescending()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var results = await ctx.TestEntities
            .OrderBy(e => e.IsActive)
            .ThenByDescending(e => e.Age)
            .AsNoTracking()
            .ToListAsync();

        // Inactive group (Bool=0), ordered by age desc: Charlie(35), Jack(29), Hank(27), Eve(22)
        Assert.False(results[0].IsActive);
        Assert.Equal(35, results[0].Age);
        Assert.Equal(22, results[3].Age);
    }

    // --- Skip / Take Edge Cases ---

    [Fact]
    public async Task Skip_WithoutTake()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var results = await ctx.TestEntities
            .OrderBy(e => e.Id)
            .Skip(8)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Equal(9, results[0].Id);
        Assert.Equal(10, results[1].Id);
    }

    [Fact]
    public async Task Skip_Zero_NoOp()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var results = await ctx.TestEntities
            .OrderBy(e => e.Id)
            .Skip(0)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(10, results.Count);
    }

    [Fact]
    public async Task Take_Zero_EmptyResult()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var results = await ctx.TestEntities
            .OrderBy(e => e.Id)
            .Take(0)
            .AsNoTracking()
            .ToListAsync();

        Assert.Empty(results);
    }

    [Fact]
    public async Task Skip_PastAllRows_EmptyResult()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var results = await ctx.TestEntities
            .OrderBy(e => e.Id)
            .Skip(100)
            .AsNoTracking()
            .ToListAsync();

        Assert.Empty(results);
    }

    [Fact]
    public async Task Take_MoreThanAvailable()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var results = await ctx.TestEntities
            .OrderBy(e => e.Id)
            .Take(1000)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(10, results.Count);
    }

    // --- First / Single ---

    [Fact]
    public async Task First_ReturnsFirstRow()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var result = await ctx.TestEntities
            .OrderBy(e => e.Id)
            .AsNoTracking()
            .FirstAsync();

        Assert.Equal(1, result.Id);
    }

    [Fact]
    public async Task First_WithPredicate()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var result = await ctx.TestEntities
            .OrderBy(e => e.Id)
            .AsNoTracking()
            .FirstAsync(e => e.Age > 30);

        Assert.Equal("Charlie", result.Name);
    }

    [Fact]
    public async Task Single_WithUniquePredicate()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var result = await ctx.TestEntities
            .AsNoTracking()
            .SingleAsync(e => e.Name == "Bob");

        Assert.Equal(2, result.Id);
        Assert.Equal(25, result.Age);
    }

    // --- Any ---

    [Fact]
    public async Task Any_ReturnsTrue()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var exists = await ctx.TestEntities.AnyAsync();

        Assert.True(exists);
    }

    [Fact]
    public async Task Any_WithPredicate_True()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var exists = await ctx.TestEntities.AnyAsync(e => e.Age > 39);

        Assert.True(exists);
    }

    [Fact]
    public async Task Any_WithPredicate_False()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var exists = await ctx.TestEntities.AnyAsync(e => e.Age > 100);

        Assert.False(exists);
    }

    // --- Aggregations ---

    [Fact]
    public async Task Min_ReturnsMinimum()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var minAge = await ctx.TestEntities.MinAsync(e => e.Age);

        Assert.Equal(22, minAge); // Eve
    }

    [Fact]
    public async Task Max_ReturnsMaximum()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var maxAge = await ctx.TestEntities.MaxAsync(e => e.Age);

        Assert.Equal(40, maxAge); // Frank
    }

    [Fact]
    public async Task Sum_ReturnsSum()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var total = await ctx.TestEntities.SumAsync(e => e.Age);

        // 30+25+35+28+22+40+33+27+31+29 = 300
        Assert.Equal(300, total);
    }

    [Fact]
    public async Task Average_ReturnsAverage()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var avg = await ctx.TestEntities.AverageAsync(e => e.Age);

        Assert.Equal(30.0, avg); // 300/10
    }

    // --- Distinct ---

    [Fact]
    public async Task Distinct_OnProjectedColumn()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var distinctActiveStates = await ctx.TestEntities
            .Select(e => e.IsActive)
            .Distinct()
            .ToListAsync();

        Assert.Equal(2, distinctActiveStates.Count);
        Assert.Contains(true, distinctActiveStates);
        Assert.Contains(false, distinctActiveStates);
    }

    // --- Computed Columns in Select ---

    [Fact]
    public async Task Select_ArithmeticExpression()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var results = await ctx.TestEntities
            .OrderBy(e => e.Id)
            .Select(e => new { e.Name, DoubleAge = e.Age * 2 })
            .Take(3)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(60, results[0].DoubleAge); // Alice: 30*2
        Assert.Equal(50, results[1].DoubleAge); // Bob: 25*2
        Assert.Equal(70, results[2].DoubleAge); // Charlie: 35*2
    }

    [Fact]
    public async Task Where_ArithmeticPredicate()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var results = await ctx.TestEntities
            .Where(e => e.Age * 2 > 65)
            .OrderBy(e => e.Name)
            .AsNoTracking()
            .ToListAsync();

        // Age > 32.5: Charlie(35), Frank(40), Grace(33)
        Assert.Equal(3, results.Count);
    }

    [Fact]
    public async Task Select_Ternary_Conditional()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var results = await ctx.TestEntities
            .OrderBy(e => e.Id)
            .Select(e => new { e.Name, Status = e.IsActive ? "Active" : "Inactive" })
            .Take(3)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal("Active", results[0].Status);   // Alice
        Assert.Equal("Active", results[1].Status);   // Bob
        Assert.Equal("Inactive", results[2].Status); // Charlie
    }

    // --- Arithmetic: Addition ---

    [Fact]
    public async Task Select_Addition()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var results = await ctx.TestEntities
            .OrderBy(e => e.Id)
            .Select(e => new { e.Name, AgePlusFive = e.Age + 5 })
            .Take(3)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(35, results[0].AgePlusFive); // Alice: 30+5
        Assert.Equal(30, results[1].AgePlusFive); // Bob: 25+5
        Assert.Equal(40, results[2].AgePlusFive); // Charlie: 35+5
    }

    [Fact]
    public async Task Where_Addition()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var results = await ctx.TestEntities
            .Where(e => e.Age + 10 > 40)
            .OrderBy(e => e.Name)
            .AsNoTracking()
            .ToListAsync();

        // Age > 30: Charlie(35), Frank(40), Grace(33), Ivy(31)
        Assert.Equal(4, results.Count);
    }

    // --- Arithmetic: Subtraction ---

    [Fact]
    public async Task Select_Subtraction()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var results = await ctx.TestEntities
            .OrderBy(e => e.Id)
            .Select(e => new { e.Name, AgeMinusTen = e.Age - 10 })
            .Take(3)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(20, results[0].AgeMinusTen); // Alice: 30-10
        Assert.Equal(15, results[1].AgeMinusTen); // Bob: 25-10
        Assert.Equal(25, results[2].AgeMinusTen); // Charlie: 35-10
    }

    [Fact]
    public async Task Where_Subtraction()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var results = await ctx.TestEntities
            .Where(e => e.Age - 5 < 20)
            .OrderBy(e => e.Name)
            .AsNoTracking()
            .ToListAsync();

        // Age < 25: Eve(22)
        Assert.Single(results);
        Assert.Equal("Eve", results[0].Name);
    }

    // --- Arithmetic: Division ---

    [Fact]
    public async Task Select_Division()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var results = await ctx.TestEntities
            .OrderBy(e => e.Id)
            .Select(e => new { e.Name, HalfAge = e.Age / 2 })
            .Take(3)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(15, results[0].HalfAge); // Alice: 30/2
        Assert.Equal(12, results[1].HalfAge); // Bob: 25/2 (integer division, rounds down)
        Assert.Equal(18, results[2].HalfAge); // Charlie: 35/2 (ClickHouse intDiv rounds towards +inf for positive)
    }

    // --- Arithmetic: Modulo ---

    [Fact]
    public async Task Select_Modulo()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var results = await ctx.TestEntities
            .OrderBy(e => e.Id)
            .Select(e => new { e.Name, AgeMod10 = e.Age % 10 })
            .Take(3)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(0, results[0].AgeMod10); // Alice: 30%10
        Assert.Equal(5, results[1].AgeMod10); // Bob: 25%10
        Assert.Equal(5, results[2].AgeMod10); // Charlie: 35%10
    }

    [Fact]
    public async Task Where_Modulo()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var results = await ctx.TestEntities
            .Where(e => e.Age % 10 == 0)
            .OrderBy(e => e.Name)
            .AsNoTracking()
            .ToListAsync();

        // Ages divisible by 10: Alice(30), Frank(40)
        Assert.Equal(2, results.Count);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal("Frank", results[1].Name);
    }

    // --- Combined arithmetic ---

    [Fact]
    public async Task Select_CompoundArithmetic()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var results = await ctx.TestEntities
            .OrderBy(e => e.Id)
            .Select(e => new { e.Name, Computed = (e.Age + 10) * 2 - 5 })
            .Take(2)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(75, results[0].Computed); // Alice: (30+10)*2-5 = 75
        Assert.Equal(65, results[1].Computed); // Bob: (25+10)*2-5 = 65
    }

    [Fact]
    public async Task Select_MultipleArithmeticColumns()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var result = await ctx.TestEntities
            .Where(e => e.Id == 1) // Alice, age 30
            .Select(e => new
            {
                Sum = e.Age + 5,
                Diff = e.Age - 5,
                Prod = e.Age * 3,
                Quot = e.Age / 6,
                Rem = e.Age % 7
            })
            .SingleAsync();

        Assert.Equal(35, result.Sum);
        Assert.Equal(25, result.Diff);
        Assert.Equal(90, result.Prod);
        Assert.Equal(5, result.Quot);
        Assert.Equal(2, result.Rem);
    }

    // --- Negation ---

    [Fact]
    public async Task Select_Negation()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var result = await ctx.TestEntities
            .Where(e => e.Id == 1) // Alice, age 30
            .Select(e => -e.Age)
            .SingleAsync();

        Assert.Equal(-30, result);
    }

    // --- Count with various predicates ---

    [Fact]
    public async Task Count_WithNotPredicate()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var count = await ctx.TestEntities
            .Where(e => !e.IsActive)
            .CountAsync();

        Assert.Equal(4, count);
    }

    [Fact]
    public async Task Count_WithComplexPredicate()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var count = await ctx.TestEntities
            .Where(e => e.Age > 25 && e.IsActive)
            .CountAsync();

        // Alice(30), Diana(28), Frank(40), Grace(33), Ivy(31)
        Assert.Equal(5, count);
    }
}
