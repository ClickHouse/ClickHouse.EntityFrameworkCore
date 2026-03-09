using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;

namespace ClickHouse.EntityFrameworkCore.Metadata.Conventions;

public class ClickHouseConventionSetBuilder : RelationalConventionSetBuilder
{
    public ClickHouseConventionSetBuilder(
        ProviderConventionSetBuilderDependencies dependencies,
        RelationalConventionSetBuilderDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
    }

    public override ConventionSet CreateConventionSet()
    {
        var conventionSet = base.CreateConventionSet();
        // ClickHouse doesn't support auto-increment.
        // Remove ValueGenerationConvention if needed, or handle in model validator.
        return conventionSet;
    }
}
