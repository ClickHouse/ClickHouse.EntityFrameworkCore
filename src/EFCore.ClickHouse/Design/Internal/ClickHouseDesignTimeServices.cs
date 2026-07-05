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

        // Bake LINQ-defined materialized-view / projection SELECT SQL into the model snapshot so
        // subsequent `migrations add` calls don't re-diff them as changed. One replacement covers
        // both the snapshot and each migration's Designer BuildTargetModel.
        serviceCollection.Replace(
            ServiceDescriptor.Singleton<ICSharpSnapshotGenerator, ClickHouseCSharpSnapshotGenerator>());

        // Split `migrations add` into dependency-ordered, single-operation step files. The core
        // services above register the default MigrationsScaffolder, so replace it with ours.
        serviceCollection.AddScoped<ClickHouseMigrationsSplitter>();
        serviceCollection.Replace(
            ServiceDescriptor.Scoped<IMigrationsScaffolder, ClickHouseMigrationsScaffolder>());
    }
}
