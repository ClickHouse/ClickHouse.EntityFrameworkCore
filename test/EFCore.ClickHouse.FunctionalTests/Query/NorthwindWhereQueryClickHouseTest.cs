using Microsoft.EntityFrameworkCore.TestModels.Northwind;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.EntityFrameworkCore;

namespace Microsoft.EntityFrameworkCore.Query;

public class NorthwindWhereQueryClickHouseTest : IClassFixture<NorthwindQueryClickHouseFixture<NoopModelCustomizer>>
{
    private readonly NorthwindQueryClickHouseFixture<NoopModelCustomizer> _fixture;

    public NorthwindWhereQueryClickHouseTest(NorthwindQueryClickHouseFixture<NoopModelCustomizer> fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Where_city_equals_london()
    {
        await using var context = _fixture.CreateContext();

        var customerIds = await context.Set<Customer>()
            .Where(c => c.City == "London")
            .OrderBy(c => c.CustomerID)
            .Select(c => c.CustomerID)
            .ToListAsync();

        Assert.Equal(["AROUT", "BSBEV"], customerIds);
    }

    [Fact]
    public async Task Where_order_date_after_cutoff()
    {
        await using var context = _fixture.CreateContext();
        var cutoff = new DateTime(1996, 7, 8);

        var orderIds = await context.Set<Order>()
            .Where(o => o.OrderDate >= cutoff)
            .OrderBy(o => o.OrderID)
            .Select(o => o.OrderID)
            .ToListAsync();

        Assert.Equal([10250, 10251, 10252, 10253], orderIds);
    }
}
