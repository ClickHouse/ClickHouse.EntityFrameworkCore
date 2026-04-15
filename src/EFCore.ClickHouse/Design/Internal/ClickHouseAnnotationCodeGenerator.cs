using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace ClickHouse.EntityFrameworkCore.Design.Internal;

public class ClickHouseAnnotationCodeGenerator : AnnotationCodeGenerator
{
    public ClickHouseAnnotationCodeGenerator(AnnotationCodeGeneratorDependencies dependencies)
        : base(dependencies)
    {
    }

    protected override bool IsHandledByConvention(IModel model, IAnnotation annotation)
    {
        if (annotation.Name.StartsWith(ClickHouseAnnotationNames.Prefix, StringComparison.Ordinal))
            return false;

        return base.IsHandledByConvention(model, annotation);
    }

    protected override bool IsHandledByConvention(IEntityType entityType, IAnnotation annotation)
    {
        if (annotation.Name.StartsWith(ClickHouseAnnotationNames.Prefix, StringComparison.Ordinal))
            return false;

        return base.IsHandledByConvention(entityType, annotation);
    }

    protected override bool IsHandledByConvention(IProperty property, IAnnotation annotation)
    {
        if (annotation.Name.StartsWith(ClickHouseAnnotationNames.Prefix, StringComparison.Ordinal))
            return false;

        return base.IsHandledByConvention(property, annotation);
    }

    protected override bool IsHandledByConvention(IIndex index, IAnnotation annotation)
    {
        if (annotation.Name.StartsWith(ClickHouseAnnotationNames.Prefix, StringComparison.Ordinal))
            return false;

        return base.IsHandledByConvention(index, annotation);
    }
}
