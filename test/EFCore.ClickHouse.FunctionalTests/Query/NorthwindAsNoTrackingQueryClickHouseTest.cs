using Microsoft.EntityFrameworkCore.TestModels.Northwind;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.EntityFrameworkCore;

namespace Microsoft.EntityFrameworkCore.Query;

public class NorthwindAsNoTrackingQueryClickHouseTest : IClassFixture<NorthwindQueryClickHouseFixture<NoopModelCustomizer>>
{
    private readonly NorthwindQueryClickHouseFixture<NoopModelCustomizer> _fixture;

    public NorthwindAsNoTrackingQueryClickHouseTest(NorthwindQueryClickHouseFixture<NoopModelCustomizer> fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task AsNoTracking_does_not_track_entities()
    {
        await using var context = _fixture.CreateContext();

        var customers = await context.Set<Customer>()
            .AsNoTracking()
            .Where(c => c.Country == "UK")
            .OrderBy(c => c.CustomerID)
            .ToListAsync();

        Assert.Equal(7, customers.Count);
        Assert.Empty(context.ChangeTracker.Entries());
    }
}
