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

        var entityType = (IEntityType)table.EntityTypeMappings.First().TypeBase;

        foreach (var annotation in entityType.GetAnnotations())
        {
            if (annotation.Name.StartsWith(ClickHouseAnnotationNames.Prefix, StringComparison.Ordinal))
                yield return annotation;
        }
    }

    public override IEnumerable<IAnnotation> For(ITableIndex index, bool designTime)
    {
        if (!designTime)
            yield break;

        var modelIndex = index.MappedIndexes.First();

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

        var property = column.PropertyMappings.First().Property;

        foreach (var annotation in property.GetAnnotations())
        {
            if (annotation.Name.StartsWith(ClickHouseAnnotationNames.Prefix, StringComparison.Ordinal))
                yield return annotation;
        }
    }
}
