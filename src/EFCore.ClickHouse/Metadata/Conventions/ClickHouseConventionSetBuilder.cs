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

        // ClickHouse doesn't support foreign keys — remove all FK-related conventions
        // to prevent EF from creating implicit indexes for FK columns.
        RemoveForeignKeyIndexConvention(conventionSet.EntityTypeBaseTypeChangedConventions);
        conventionSet.ForeignKeyAddedConventions.Clear();
        conventionSet.ForeignKeyAnnotationChangedConventions.Clear();
        conventionSet.ForeignKeyDependentRequirednessChangedConventions.Clear();
        conventionSet.ForeignKeyOwnershipChangedConventions.Clear();
        conventionSet.ForeignKeyPrincipalEndChangedConventions.Clear();
        conventionSet.ForeignKeyPropertiesChangedConventions.Clear();
        conventionSet.ForeignKeyRemovedConventions.Clear();
        conventionSet.ForeignKeyRequirednessChangedConventions.Clear();
        conventionSet.ForeignKeyUniquenessChangedConventions.Clear();
        conventionSet.SkipNavigationForeignKeyChangedConventions.Clear();

        conventionSet.ModelFinalizingConventions.Add(new ClickHouseDefaultEngineConvention());

        return conventionSet;
    }

    private static void RemoveForeignKeyIndexConvention(IList<IEntityTypeBaseTypeChangedConvention> conventions)
    {
        for (var i = conventions.Count - 1; i >= 0; i--)
        {
            if (conventions[i] is ForeignKeyIndexConvention)
                conventions.RemoveAt(i);
        }
    }
}
