using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;

namespace ClickHouse.EntityFrameworkCore.Migrations.Internal;

public class ClickHouseMigrationsAnnotationProvider : MigrationsAnnotationProvider
{
    public ClickHouseMigrationsAnnotationProvider(MigrationsAnnotationProviderDependencies dependencies)
        : base(dependencies)
    {
    }

    /// <summary>
    /// Yields ClickHouse annotations for a table index being removed.
    /// Without this, DropIndexOperation would lack the SkippingIndexType annotation
    /// and the SQL generator would silently skip the DROP INDEX statement.
    /// </summary>
    public override IEnumerable<IAnnotation> ForRemove(ITableIndex index)
    {
        foreach (var annotation in index.GetAnnotations())
        {
            if (annotation.Name.StartsWith(ClickHouseAnnotationNames.Prefix, StringComparison.Ordinal))
                yield return annotation;
        }
    }

    /// <summary>
    /// Yields ClickHouse column annotations (codec, TTL, comment) for a column being removed or altered.
    /// Ensures AlterColumnOperation.OldColumn carries the previous annotations
    /// so the SQL generator can emit REMOVE statements when they are dropped.
    /// </summary>
    public override IEnumerable<IAnnotation> ForRemove(IColumn column)
    {
        foreach (var annotation in column.GetAnnotations())
        {
            if (annotation.Name.StartsWith(ClickHouseAnnotationNames.Prefix, StringComparison.Ordinal))
                yield return annotation;
        }
    }
}
