using Microsoft.EntityFrameworkCore.TestModels.Northwind;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.EntityFrameworkCore;

namespace Microsoft.EntityFrameworkCore.Query;

public class NorthwindAggregateOperatorsQueryClickHouseTest : IClassFixture<NorthwindQueryClickHouseFixture<NoopModelCustomizer>>
{
    private readonly NorthwindQueryClickHouseFixture<NoopModelCustomizer> _fixture;

    public NorthwindAggregateOperatorsQueryClickHouseTest(NorthwindQueryClickHouseFixture<NoopModelCustomizer> fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Count_orders()
    {
        await using var context = _fixture.CreateContext();
        var count = await context.Set<Order>().CountAsync();
        Assert.Equal(830, count);
    }

    [Fact]
    public async Task Sum_freight_for_customer()
    {
        await using var context = _fixture.CreateContext();
        var sum = await context.Set<Order>()
            .Where(o => o.CustomerID == "AROUT")
            .SumAsync(o => o.Freight);

        Assert.Equal(471.95m, sum);
    }
}
