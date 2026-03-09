using Microsoft.EntityFrameworkCore.TestModels.Northwind;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace Microsoft.EntityFrameworkCore.Query;

public class NorthwindQueryClickHouseFixture<TModelCustomizer> : NorthwindQueryRelationalFixture<TModelCustomizer>
    where TModelCustomizer : ITestModelCustomizer, new()
{
    protected override ITestStoreFactory TestStoreFactory
        => ClickHouseNorthwindTestStoreFactory.Instance;

    protected override Type ContextType
        => typeof(NorthwindClickHouseContext);

    public new TestSqlLoggerFactory TestSqlLoggerFactory
        => (TestSqlLoggerFactory)ListLoggerFactory;

    protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
    {
        base.OnModelCreating(modelBuilder, context);

        modelBuilder.Entity<Order>().Property(e => e.Freight).HasColumnType("Decimal(12,2)");
        modelBuilder.Entity<OrderDetail>().Property(e => e.UnitPrice).HasPrecision(12, 4);
        modelBuilder.Entity<Product>().Property(e => e.UnitPrice).HasPrecision(12, 4);
    }
}
