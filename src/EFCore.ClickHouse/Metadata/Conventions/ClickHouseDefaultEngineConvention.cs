using ClickHouse.EntityFrameworkCore.Extensions;
using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;

namespace ClickHouse.EntityFrameworkCore.Metadata.Conventions;

/// <summary>
/// Sets MergeTree as the default engine for entity types that don't have an explicit engine configured.
/// Uses the EF primary key columns as ORDER BY when no explicit ORDER BY is set.
/// </summary>
public class ClickHouseDefaultEngineConvention : IModelFinalizingConvention
{
    public void ProcessModelFinalizing(IConventionModelBuilder modelBuilder, IConventionContext<IConventionModelBuilder> context)
    {
        foreach (var entityType in modelBuilder.Metadata.GetEntityTypes())
        {
            // Skip owned types and those without a table mapping
            if (entityType.IsOwned() || entityType.GetTableName() is null)
                continue;

            var mutableEntityType = (IMutableEntityType)entityType;

            // Set default engine to MergeTree if none configured
            if (entityType.GetEngine() is null)
            {
                mutableEntityType.SetEngine(ClickHouseAnnotationNames.MergeTree);
            }

            // Set ORDER BY from primary key if none configured and engine is MergeTree-family
            var engine = entityType.GetEngine();
            if (entityType.GetOrderBy() is null && IsMergeTreeFamily(engine))
            {
                var primaryKey = entityType.FindPrimaryKey();
                if (primaryKey is not null)
                {
                    var columns = primaryKey.Properties
                        .Select(p => p.GetColumnName() ?? p.Name)
                        .ToArray();
                    mutableEntityType.SetOrderBy(columns);
                }
                else
                {
                    mutableEntityType.SetOrderBy(["tuple()"]);
                }
            }
        }
    }

    private static bool IsMergeTreeFamily(string? engine)
        => engine is ClickHouseAnnotationNames.MergeTree
            or ClickHouseAnnotationNames.ReplacingMergeTree
            or ClickHouseAnnotationNames.SummingMergeTree
            or ClickHouseAnnotationNames.AggregatingMergeTree
            or ClickHouseAnnotationNames.CollapsingMergeTree
            or ClickHouseAnnotationNames.VersionedCollapsingMergeTree
            or ClickHouseAnnotationNames.GraphiteMergeTree;
}
