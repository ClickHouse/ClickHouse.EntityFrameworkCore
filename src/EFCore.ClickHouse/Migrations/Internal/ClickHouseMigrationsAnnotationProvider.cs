using Microsoft.EntityFrameworkCore.Migrations;

namespace ClickHouse.EntityFrameworkCore.Migrations.Internal;

public class ClickHouseMigrationsAnnotationProvider : MigrationsAnnotationProvider
{
    public ClickHouseMigrationsAnnotationProvider(MigrationsAnnotationProviderDependencies dependencies)
        : base(dependencies)
    {
    }
}
