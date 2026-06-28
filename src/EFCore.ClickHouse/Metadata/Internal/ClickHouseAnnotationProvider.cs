using ClickHouse.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace ClickHouse.EntityFrameworkCore.Metadata.Internal;

public class ClickHouseAnnotationProvider : RelationalAnnotationProvider
{
    public ClickHouseAnnotationProvider(RelationalAnnotationProviderDependencies dependencies)
        : base(dependencies)
    {
    }

    public override IEnumerable<IAnnotation> For(ITable table, bool designTime)
    {
        if (!designTime)
            yield break;

        var mapping = table.EntityTypeMappings.FirstOrDefault();
        if (mapping is null)
            yield break;

        var entityType = (IEntityType)mapping.TypeBase;

        foreach (var annotation in entityType.GetAnnotations())
        {
            // Engine/order-by/etc. belong on the table operation, but projection annotations do not:
            // projections become their own ClickHouseAddProjectionOperation (the differ reads them
            // straight off the entity type). Copying them onto the CreateTableOperation pollutes it —
            // and the transient pending-lambda value is a delegate the C# code generator can't emit.
            if (annotation.Name.StartsWith(ClickHouseAnnotationNames.Prefix, StringComparison.Ordinal)
                && !annotation.Name.StartsWith(ClickHouseAnnotationNames.ProjectionPrefix, StringComparison.Ordinal))
                yield return annotation;
        }
    }

    public override IEnumerable<IAnnotation> For(ITableIndex index, bool designTime)
    {
        if (!designTime)
            yield break;

        var modelIndex = index.MappedIndexes.FirstOrDefault();
        if (modelIndex is null)
            yield break;

        foreach (var annotation in modelIndex.GetAnnotations())
        {
            if (annotation.Name.StartsWith(ClickHouseAnnotationNames.Prefix, StringComparison.Ordinal))
                yield return annotation;
        }
    }

    public override IEnumerable<IAnnotation> For(IColumn column, bool designTime)
    {
        if (!designTime)
            yield break;

        var mapping = column.PropertyMappings.FirstOrDefault();
        if (mapping is null)
            yield break;

        foreach (var annotation in mapping.Property.GetAnnotations())
        {
            if (annotation.Name.StartsWith(ClickHouseAnnotationNames.Prefix, StringComparison.Ordinal))
                yield return annotation;
        }
    }
}
