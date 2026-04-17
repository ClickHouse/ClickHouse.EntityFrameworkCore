using Microsoft.EntityFrameworkCore.TestModels.Northwind;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.EntityFrameworkCore;

namespace Microsoft.EntityFrameworkCore.Query;

public class NorthwindCompiledQueryClickHouseTest : IClassFixture<NorthwindQueryClickHouseFixture<NoopModelCustomizer>>
{
    private readonly NorthwindQueryClickHouseFixture<NoopModelCustomizer> _fixture;

    public NorthwindCompiledQueryClickHouseTest(NorthwindQueryClickHouseFixture<NoopModelCustomizer> fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public void Compiled_query_returns_customer_ids()
    {
        using var context = _fixture.CreateContext();

        var compiled = EF.CompileQuery(
            (NorthwindContext ctx, string country) => ctx.Customers
                .Where(c => c.Country == country)
                .OrderBy(c => c.CustomerID)
                .Select(c => c.CustomerID));

        var result = compiled(context, "UK").ToList();

        Assert.Equal(["AROUT", "BSBEV", "CONSH", "EASTC", "ISLAT", "NORTS", "SEVES"], result);
    }
}
