using Microsoft.EntityFrameworkCore.TestModels.Northwind;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.EntityFrameworkCore;

namespace Microsoft.EntityFrameworkCore.Query;

public class NorthwindFunctionsQueryClickHouseTest : IClassFixture<NorthwindQueryClickHouseFixture<NoopModelCustomizer>>
{
    private readonly NorthwindQueryClickHouseFixture<NoopModelCustomizer> _fixture;

    public NorthwindFunctionsQueryClickHouseTest(NorthwindQueryClickHouseFixture<NoopModelCustomizer> fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Contains_over_company_name()
    {
        await using var context = _fixture.CreateContext();

        var ids = await context.Set<Customer>()
            .Where(c => c.CompanyName.Contains("Beverages"))
            .Select(c => c.CustomerID)
            .ToListAsync();

        Assert.Equal(["BSBEV"], ids);
    }

    [Fact]
    public async Task StartsWith_over_city()
    {
        await using var context = _fixture.CreateContext();

        var count = await context.Set<Customer>()
            .Where(c => c.City.StartsWith("Br"))
            .CountAsync();

        Assert.Equal(1, count); // Bräcke
    }
}
