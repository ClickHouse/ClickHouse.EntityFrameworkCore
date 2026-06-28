using ClickHouse.EntityFrameworkCore.Extensions;
using ClickHouse.EntityFrameworkCore.Migrations.Design;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Migrations.Design;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

[assembly: DesignTimeProviderServices(
    "ClickHouse.EntityFrameworkCore.Design.Internal.ClickHouseDesignTimeServices")]

namespace ClickHouse.EntityFrameworkCore.Design.Internal;

public class ClickHouseDesignTimeServices : IDesignTimeServices
{
    public void ConfigureDesignTimeServices(IServiceCollection serviceCollection)
    {
        serviceCollection.AddEntityFrameworkClickHouse();

        new EntityFrameworkRelationalDesignServicesBuilder(serviceCollection)
            .TryAdd<IAnnotationCodeGenerator, ClickHouseAnnotationCodeGenerator>()
            .TryAdd<IProviderConfigurationCodeGenerator, ClickHouseCodeGenerator>()
            .TryAddCoreServices();

        // Teach the scaffolder to render this provider's custom migration operations
        // (materialized view / database create + drop) into C#.
        serviceCollection.Replace(
            ServiceDescriptor.Scoped<ICSharpMigrationOperationGenerator, ClickHouseCSharpMigrationOperationGenerator>());
    }
}
