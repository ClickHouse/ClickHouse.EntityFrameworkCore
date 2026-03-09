using Microsoft.EntityFrameworkCore.TestModels.Northwind;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.EntityFrameworkCore;

namespace Microsoft.EntityFrameworkCore.Query;

public class NorthwindSelectQueryClickHouseTest : IClassFixture<NorthwindQueryClickHouseFixture<NoopModelCustomizer>>
{
    private readonly NorthwindQueryClickHouseFixture<NoopModelCustomizer> _fixture;

    public NorthwindSelectQueryClickHouseTest(NorthwindQueryClickHouseFixture<NoopModelCustomizer> fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Select_simple_projection()
    {
        await using var context = _fixture.CreateContext();

        var result = await context.Set<Customer>()
            .OrderBy(c => c.CustomerID)
            .Select(c => new { c.CustomerID, c.City })
            .Take(3)
            .ToListAsync();

        Assert.Equal(3, result.Count);
        Assert.Equal("ALFKI", result[0].CustomerID);
        Assert.Equal("Berlin", result[0].City);
    }

    [Fact]
    public async Task Select_scalar_computation()
    {
        await using var context = _fixture.CreateContext();

        var result = await context.Set<Order>()
            .OrderBy(o => o.OrderID)
            .Select(o => new { o.OrderID, FreightPlusOne = o.Freight + 1m })
            .FirstAsync();

        Assert.Equal(10248, result.OrderID);
        Assert.Equal(33.38m, result.FreightPlusOne);
    }
}
