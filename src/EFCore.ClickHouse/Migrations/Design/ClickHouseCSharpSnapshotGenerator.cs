using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using ClickHouse.EntityFrameworkCore.Migrations.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations.Design;

namespace ClickHouse.EntityFrameworkCore.Migrations.Design;

/// <summary>
/// Snapshot generator that bakes the translated SELECT SQL of a LINQ-defined materialized view or
/// projection into the model snapshot. The fluent <c>.Select(...)</c> builders stash only a
/// non-serializable <c>PendingLambda</c> delegate (stripped from the snapshot by
/// <c>ClickHouseAnnotationCodeGenerator</c>), so without this the snapshot would carry no
/// <c>SelectSql</c> annotation and every subsequent <c>migrations add</c> would re-diff the view /
/// projection as changed. We translate the pending lambda the same way the differ does and emit a
/// plain <c>HasAnnotation("…:SelectSql", …)</c> line, so the reconstructed snapshot model round-trips.
/// Views/projections defined with <c>.FromRaw(...)</c> already have a real <c>SelectSql</c> annotation
/// (emitted by the base generator) and are left untouched.
/// </summary>
public class ClickHouseCSharpSnapshotGenerator : CSharpSnapshotGenerator
{
    public ClickHouseCSharpSnapshotGenerator(CSharpSnapshotGeneratorDependencies dependencies)
        : base(dependencies)
    {
    }

    public override void Generate(string modelBuilderName, IModel model, IndentedStringBuilder stringBuilder)
    {
        base.Generate(modelBuilderName, model, stringBuilder);

        // Materialized views are model-scoped. Emit any LINQ view's translated SelectSql after the
        // base output (order is irrelevant to model construction).
        var pending = FindPendingSelectSqls(
            model.GetAnnotations(),
            ClickHouseAnnotationNames.MaterializedViewPrefix,
            ClickHouseAnnotationNames.MaterializedViewSelectSqlSuffix,
            a => a.Value is PendingMaterializedViewQuery);

        if (pending.Count == 0)
            return;

        using var context = ClickHousePendingQueryTranslator.CreateTranslationContext(model);
        foreach (var (name, annotationName) in pending)
        {
            var query = (PendingMaterializedViewQuery)model.FindAnnotation(annotationName)!.Value!;
            var sql = ClickHousePendingQueryTranslator.TranslateMaterializedViewSql(context, query);
            AppendSelectSqlAnnotation(
                stringBuilder, modelBuilderName,
                ClickHouseAnnotationNames.MaterializedViewPrefix + name + ":" + ClickHouseAnnotationNames.MaterializedViewSelectSqlSuffix,
                sql);
        }
    }

    protected override void GenerateEntityTypeAnnotations(
        string entityTypeBuilderName, IEntityType entityType, IndentedStringBuilder stringBuilder)
    {
        base.GenerateEntityTypeAnnotations(entityTypeBuilderName, entityType, stringBuilder);

        // Projections are entity-scoped. Emit any LINQ projection's translated SelectSql after base.
        var pending = FindPendingSelectSqls(
            entityType.GetAnnotations(),
            ClickHouseAnnotationNames.ProjectionPrefix,
            ClickHouseAnnotationNames.ProjectionSelectSqlSuffix,
            a => a.Value is PendingProjectionQuery);

        if (pending.Count == 0)
            return;

        using var context = ClickHousePendingQueryTranslator.CreateTranslationContext(entityType.Model);
        foreach (var (name, annotationName) in pending)
        {
            var query = (PendingProjectionQuery)entityType.FindAnnotation(annotationName)!.Value!;
            var sql = ClickHousePendingQueryTranslator.TranslateProjectionSql(context, query);
            AppendSelectSqlAnnotation(
                stringBuilder, entityTypeBuilderName,
                ClickHouseAnnotationNames.ProjectionPrefix + name + ":" + ClickHouseAnnotationNames.ProjectionSelectSqlSuffix,
                sql);
        }
    }

    // Finds `{prefix}{name}:PendingLambda` annotations that lack a sibling `{prefix}{name}:SelectSql`
    // (i.e. LINQ-defined, not FromRaw), returning each object's name and its pending-lambda annotation.
    private static List<(string Name, string PendingAnnotationName)> FindPendingSelectSqls(
        IEnumerable<IAnnotation> annotations,
        string prefix,
        string selectSqlSuffix,
        Func<IAnnotation, bool> isPendingLambda)
    {
        const string pendingSuffix = ":PendingLambda";
        var existingSelectSql = new HashSet<string>(StringComparer.Ordinal);
        var pending = new List<(string, string)>();

        foreach (var annotation in annotations)
        {
            if (!annotation.Name.StartsWith(prefix, StringComparison.Ordinal))
                continue;

            var rest = annotation.Name[prefix.Length..];
            var colonIdx = rest.IndexOf(':');
            if (colonIdx <= 0)
                continue;

            var name = rest[..colonIdx];
            var suffix = rest[(colonIdx + 1)..];

            if (suffix == selectSqlSuffix)
                existingSelectSql.Add(name);
            else if (annotation.Name.EndsWith(pendingSuffix, StringComparison.Ordinal) && isPendingLambda(annotation))
                pending.Add((name, annotation.Name));
        }

        pending.RemoveAll(p => existingSelectSql.Contains(p.Item1));
        return pending;
    }

    private void AppendSelectSqlAnnotation(
        IndentedStringBuilder stringBuilder, string builderName, string annotationName, string sql)
        => stringBuilder
            .AppendLine()
            .Append(builderName)
            .Append(".HasAnnotation(")
            .Append(Dependencies.CSharpHelper.Literal(annotationName))
            .Append(", ")
            .Append(Dependencies.CSharpHelper.Literal(sql))
            .AppendLine(");");
}
