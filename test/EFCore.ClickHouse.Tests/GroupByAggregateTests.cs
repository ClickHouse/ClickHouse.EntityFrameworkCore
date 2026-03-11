using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.ClickHouse.Tests;

public class GroupByAggregateTests : IClassFixture<ClickHouseFixture>
{
    private readonly ClickHouseFixture _fixture;

    public GroupByAggregateTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task GroupBy_Count_ReturnsCorrectCounts()
    {
        await using var context = new TestDbContext(_fixture.ConnectionString);

        var results = await context.TestEntities
            .GroupBy(e => e.IsActive)
            .Select(g => new { IsActive = g.Key, Count = g.Count() })
            .OrderBy(x => x.IsActive)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(2, results.Count);
        // false group: Charlie, Eve, Hank, Jack = 4
        Assert.False(results[0].IsActive);
        Assert.Equal(4, results[0].Count);
        // true group: Alice, Bob, Diana, Frank, Grace, Ivy = 6
        Assert.True(results[1].IsActive);
        Assert.Equal(6, results[1].Count);
    }

    [Fact]
    public async Task GroupBy_Sum_ReturnsCorrectSums()
    {
        await using var context = new TestDbContext(_fixture.ConnectionString);

        var results = await context.TestEntities
            .GroupBy(e => e.IsActive)
            .Select(g => new { IsActive = g.Key, TotalAge = g.Sum(e => e.Age) })
            .OrderBy(x => x.IsActive)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(2, results.Count);
        // false: 35 + 22 + 27 + 29 = 113
        Assert.Equal(113, results[0].TotalAge);
        // true: 30 + 25 + 28 + 40 + 33 + 31 = 187
        Assert.Equal(187, results[1].TotalAge);
    }

    [Fact]
    public async Task GroupBy_Average_ReturnsCorrectAverages()
    {
        await using var context = new TestDbContext(_fixture.ConnectionString);

        var results = await context.TestEntities
            .GroupBy(e => e.IsActive)
            .Select(g => new { IsActive = g.Key, AvgAge = g.Average(e => e.Age) })
            .OrderBy(x => x.IsActive)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(2, results.Count);
        // false: 113 / 4 = 28.25
        Assert.Equal(28.25, results[0].AvgAge);
        // true: 187 / 6 ≈ 31.1667
        Assert.Equal(31.1667, results[1].AvgAge, 3);
    }

    [Fact]
    public async Task GroupBy_MinMax_ReturnsCorrectValues()
    {
        await using var context = new TestDbContext(_fixture.ConnectionString);

        var results = await context.TestEntities
            .GroupBy(e => e.IsActive)
            .Select(g => new { IsActive = g.Key, MinAge = g.Min(e => e.Age), MaxAge = g.Max(e => e.Age) })
            .OrderBy(x => x.IsActive)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(2, results.Count);
        // false: min=22(Eve), max=35(Charlie)
        Assert.Equal(22, results[0].MinAge);
        Assert.Equal(35, results[0].MaxAge);
        // true: min=25(Bob), max=40(Frank)
        Assert.Equal(25, results[1].MinAge);
        Assert.Equal(40, results[1].MaxAge);
    }

    [Fact]
    public async Task GroupBy_Having_FiltersGroups()
    {
        await using var context = new TestDbContext(_fixture.ConnectionString);

        // Only groups where count > 4
        var results = await context.TestEntities
            .GroupBy(e => e.IsActive)
            .Where(g => g.Count() > 4)
            .Select(g => new { IsActive = g.Key, Count = g.Count() })
            .AsNoTracking()
            .ToListAsync();

        // Only active group has 6, inactive has 4
        Assert.Single(results);
        Assert.True(results[0].IsActive);
        Assert.Equal(6, results[0].Count);
    }

    [Fact]
    public async Task GroupBy_MultipleAggregates_ReturnsAll()
    {
        await using var context = new TestDbContext(_fixture.ConnectionString);

        var results = await context.TestEntities
            .GroupBy(e => e.IsActive)
            .Select(g => new
            {
                IsActive = g.Key,
                Count = g.Count(),
                TotalAge = g.Sum(e => e.Age),
                MinAge = g.Min(e => e.Age),
                MaxAge = g.Max(e => e.Age),
            })
            .OrderBy(x => x.IsActive)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(2, results.Count);

        // Inactive group
        Assert.Equal(4, results[0].Count);
        Assert.Equal(113, results[0].TotalAge);
        Assert.Equal(22, results[0].MinAge);
        Assert.Equal(35, results[0].MaxAge);

        // Active group
        Assert.Equal(6, results[1].Count);
        Assert.Equal(187, results[1].TotalAge);
        Assert.Equal(25, results[1].MinAge);
        Assert.Equal(40, results[1].MaxAge);
    }

    [Fact]
    public async Task GroupBy_OrderByAggregate_Sorts()
    {
        await using var context = new TestDbContext(_fixture.ConnectionString);

        var results = await context.TestEntities
            .GroupBy(e => e.IsActive)
            .Select(g => new { IsActive = g.Key, Count = g.Count() })
            .OrderByDescending(x => x.Count)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(2, results.Count);
        // Active group (6) should come first
        Assert.True(results[0].IsActive);
        Assert.Equal(6, results[0].Count);
        // Inactive group (4) second
        Assert.False(results[1].IsActive);
        Assert.Equal(4, results[1].Count);
    }
}
