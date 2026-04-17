using Microsoft.EntityFrameworkCore.TestModels.Northwind;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.EntityFrameworkCore;

namespace Microsoft.EntityFrameworkCore.Query;

public class NorthwindAsTrackingQueryClickHouseTest : IClassFixture<NorthwindQueryClickHouseFixture<NoopModelCustomizer>>
{
    private readonly NorthwindQueryClickHouseFixture<NoopModelCustomizer> _fixture;

    public NorthwindAsTrackingQueryClickHouseTest(NorthwindQueryClickHouseFixture<NoopModelCustomizer> fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task AsTracking_tracks_entities()
    {
        await using var context = _fixture.CreateContext();

        var customers = await context.Set<Customer>()
            .AsTracking()
            .Where(c => c.Country == "UK")
            .OrderBy(c => c.CustomerID)
            .ToListAsync();

        Assert.Equal(7, customers.Count);
        Assert.Equal(7, context.ChangeTracker.Entries<Customer>().Count());
    }
}
