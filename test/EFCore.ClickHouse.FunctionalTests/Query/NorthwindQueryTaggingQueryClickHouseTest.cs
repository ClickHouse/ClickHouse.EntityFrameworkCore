using Microsoft.EntityFrameworkCore.TestModels.Northwind;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.EntityFrameworkCore;

namespace Microsoft.EntityFrameworkCore.Query;

public class NorthwindQueryTaggingQueryClickHouseTest : IClassFixture<NorthwindQueryClickHouseFixture<NoopModelCustomizer>>
{
    private readonly NorthwindQueryClickHouseFixture<NoopModelCustomizer> _fixture;

    public NorthwindQueryTaggingQueryClickHouseTest(NorthwindQueryClickHouseFixture<NoopModelCustomizer> fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public void TagWith_is_included_in_query_string()
    {
        using var context = _fixture.CreateContext();

        var sql = context.Set<Customer>()
            .TagWith("northwind-tag")
            .Where(c => c.City == "London")
            .ToQueryString();

        Assert.Contains("northwind-tag", sql);
    }
}
