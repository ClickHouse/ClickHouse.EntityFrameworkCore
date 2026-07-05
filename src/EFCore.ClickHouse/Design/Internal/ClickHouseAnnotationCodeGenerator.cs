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
    // IsHandledByConvention overrides below do not reach the snapshot path. A LINQ-defined
    // materialized view stashes a transient pending-lambda annotation holding a non-serializable
    // delegate (translated at differ time); it must be dropped here or the snapshot generator throws
    // trying to emit the delegate via HasAnnotation.
    public override IEnumerable<IAnnotation> FilterIgnoredAnnotations(IEnumerable<IAnnotation> annotations)
        => base.FilterIgnoredAnnotations(annotations)
            .Where(a => !a.Name.EndsWith(
                ":" + ClickHouseAnnotationNames.MaterializedViewPendingLambdaSuffix, StringComparison.Ordinal));

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
