using ClickHouse.EntityFrameworkCore.Extensions;
using Microsoft.Extensions.DependencyInjection;

namespace Microsoft.EntityFrameworkCore.TestUtilities;

public class ClickHouseTestStoreFactory(
    string? scriptPath = null,
    string? additionalSql = null)
    : RelationalTestStoreFactory
{
    public static ClickHouseTestStoreFactory Instance { get; } = new();

    public override TestStore Create(string storeName)
        => new ClickHouseTestStore(storeName, scriptPath, additionalSql, shared: false);

    public override TestStore GetOrCreate(string storeName)
        => new ClickHouseTestStore(storeName, scriptPath, additionalSql, shared: true);

    public override IServiceCollection AddProviderServices(IServiceCollection serviceCollection)
        => serviceCollection.AddEntityFrameworkClickHouse();
}
