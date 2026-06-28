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

    // The snapshot generator (CSharpSnapshotGenerator) filters annotations ONLY through
    // FilterIgnoredAnnotations — it never calls RemoveAnnotationsHandledByConventions — so the
    // IsHandledByConvention overrides below do not reach the snapshot path. The pending-LINQ-lambda
    // annotations (materialized views on the model, projections on entity types) hold a
    // non-serializable delegate that is translated at differ time, so they must be dropped here or
    // the snapshot generator throws trying to emit the delegate via HasAnnotation. Filter them out
    // for every annotatable the snapshot walks (model, entity types, …).
    public override IEnumerable<IAnnotation> FilterIgnoredAnnotations(IEnumerable<IAnnotation> annotations)
        => base.FilterIgnoredAnnotations(annotations).Where(a => !IsTransientPendingLambda(a.Name));

    protected override bool IsHandledByConvention(IModel model, IAnnotation annotation)
    {
        if (IsTransientPendingLambda(annotation.Name))
            return true;

        if (annotation.Name.StartsWith(ClickHouseAnnotationNames.Prefix, StringComparison.Ordinal))
            return false;

        return base.IsHandledByConvention(model, annotation);
    }

    protected override bool IsHandledByConvention(IEntityType entityType, IAnnotation annotation)
    {
        if (IsTransientPendingLambda(annotation.Name))
            return true;

        if (annotation.Name.StartsWith(ClickHouseAnnotationNames.Prefix, StringComparison.Ordinal))
            return false;

        return base.IsHandledByConvention(entityType, annotation);
    }

    // The pending-LINQ-lambda annotations (materialized views on the model, projections on entity
    // types) hold a non-serializable delegate that is translated at differ time. Report them as
    // handled-by-convention so the snapshot generator drops them instead of trying to emit the
    // delegate via HasAnnotation (which would throw in CSharpHelper). Both suffixes are "PendingLambda".
    private static bool IsTransientPendingLambda(string annotationName)
        => annotationName.StartsWith(ClickHouseAnnotationNames.Prefix, StringComparison.Ordinal)
        && annotationName.EndsWith(":" + ClickHouseAnnotationNames.ProjectionPendingLambdaSuffix, StringComparison.Ordinal);

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
