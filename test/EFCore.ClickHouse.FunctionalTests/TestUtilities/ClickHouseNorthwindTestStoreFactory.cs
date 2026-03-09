namespace Microsoft.EntityFrameworkCore.TestUtilities;

public class ClickHouseNorthwindTestStoreFactory : ClickHouseTestStoreFactory
{
    public const string Name = "northwind";
    public static new ClickHouseNorthwindTestStoreFactory Instance { get; } = new();

    protected ClickHouseNorthwindTestStoreFactory()
    {
    }

    public override TestStore GetOrCreate(string storeName)
        => ClickHouseTestStore.GetOrCreate(Name, scriptPath: "TestData/Northwind.ClickHouse.sql");
}
