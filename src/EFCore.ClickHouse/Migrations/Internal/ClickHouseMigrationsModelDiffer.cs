using ClickHouse.EntityFrameworkCore.Extensions;
using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using ClickHouse.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Internal;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update.Internal;

namespace ClickHouse.EntityFrameworkCore.Migrations.Internal;

/// <summary>
/// Extends EF Core's migration differ with materialized view diffing. Translates any pending
/// LINQ view bodies attached to the target model (model is fully finalized by the time the
/// differ runs, so building a translation DbContext is safe here — unlike at model-finalizing
/// time), then compares views between source and target and emits create/drop operations.
/// </summary>
public class ClickHouseMigrationsModelDiffer : MigrationsModelDiffer
{
    public ClickHouseMigrationsModelDiffer(
        IRelationalTypeMappingSource typeMappingSource,
        IMigrationsAnnotationProvider migrationsAnnotationProvider,
        IRelationalAnnotationProvider relationalAnnotationProvider,
        IRowIdentityMapFactory rowIdentityMapFactory,
        CommandBatchPreparerDependencies commandBatchPreparerDependencies)
        : base(typeMappingSource, migrationsAnnotationProvider, relationalAnnotationProvider, rowIdentityMapFactory, commandBatchPreparerDependencies)
    {
    }

    public override IReadOnlyList<MigrationOperation> GetDifferences(
        IRelationalModel? source,
        IRelationalModel? target)
    {
        var baseOps = base.GetDifferences(source, target);

        var sourceViews = (source?.Model.GetMaterializedViews() ?? []).ToDictionary(v => v.Name);
        var targetViews = (target?.Model.GetMaterializedViews() ?? []).ToDictionary(v => v.Name);

        if (sourceViews.Count == 0 && targetViews.Count == 0)
            return baseOps;

        // Translate any pending LINQ lambdas on the TARGET model. Model is locked at this point,
        // so we can't mutate annotations — we translate to local variables instead and pass the
        // result through to the create operations directly.
        var translatedTargets = ResolveTargetSqls(target?.Model, targetViews.Values);

        var dropOps = new List<MigrationOperation>();
        var createOps = new List<MigrationOperation>();

        foreach (var (name, sourceView) in sourceViews)
        {
            if (!targetViews.TryGetValue(name, out var targetView))
            {
                dropOps.Add(BuildDropOperation(sourceView));
            }
            else if (!ViewsEqual(sourceView, ResolvedView(targetView, translatedTargets)))
            {
                dropOps.Add(BuildDropOperation(sourceView));
                createOps.Add(BuildCreateOperation(ResolvedView(targetView, translatedTargets), target!.Model));
            }
        }

        foreach (var (name, targetView) in targetViews)
        {
            if (!sourceViews.ContainsKey(name))
                createOps.Add(BuildCreateOperation(ResolvedView(targetView, translatedTargets), target!.Model));
        }

        if (dropOps.Count == 0 && createOps.Count == 0)
            return baseOps;

        var result = new List<MigrationOperation>(dropOps.Count + baseOps.Count + createOps.Count);
        result.AddRange(dropOps);
        result.AddRange(baseOps);
        result.AddRange(createOps);
        return result;
    }

    private static MaterializedViewDefinition ResolvedView(
        MaterializedViewDefinition view,
        Dictionary<string, string> translatedSqls)
        => view.SelectSql is not null || !translatedSqls.TryGetValue(view.Name, out var sql)
            ? view
            : view with { SelectSql = sql };

    private static Dictionary<string, string> ResolveTargetSqls(
        IReadOnlyModel? model,
        IEnumerable<MaterializedViewDefinition> targetViews)
    {
        var sqls = new Dictionary<string, string>(StringComparer.Ordinal);
        if (model is null)
            return sqls;

        // Find views that need translation (no SelectSql baked in yet)
        var needTranslation = targetViews
            .Where(v => v.SelectSql is null)
            .Select(v => v.Name)
            .ToHashSet(StringComparer.Ordinal);

        if (needTranslation.Count == 0)
            return sqls;

        var pendingAnnotations = model.GetAnnotations()
            .Where(a => a.Name.StartsWith(ClickHouseAnnotationNames.MaterializedViewPrefix, StringComparison.Ordinal)
                     && a.Name.EndsWith(":" + ClickHouseAnnotationNames.MaterializedViewPendingLambdaSuffix, StringComparison.Ordinal)
                     && a.Value is PendingMaterializedViewQuery
                     && needTranslation.Contains(ExtractViewName(a.Name)))
            .ToList();

        if (pendingAnnotations.Count == 0)
            return sqls;

        using var translationContext = CreateTranslationContext(model);

        foreach (var annotation in pendingAnnotations)
        {
            var viewName = ExtractViewName(annotation.Name);
            var query = (PendingMaterializedViewQuery)annotation.Value!;
            sqls[viewName] = TranslateToSql(translationContext, query);
        }

        return sqls;
    }

    private static DbContext CreateTranslationContext(IReadOnlyModel model)
    {
        var options = new DbContextOptionsBuilder()
            .UseClickHouse("Host=localhost;Database=mv-translate")
            .UseModel((IModel)model)
            .Options;

        return new DbContext(options);
    }

    private static string TranslateToSql(DbContext context, PendingMaterializedViewQuery query)
    {
        var setMethod = typeof(DbContext).GetMethods()
            .First(m => m.Name == nameof(DbContext.Set)
                && m.IsGenericMethod
                && m.GetGenericArguments().Length == 1
                && m.GetParameters().Length == 0);

        var queryables = new object[query.SourceTypes.Length];
        for (var i = 0; i < query.SourceTypes.Length; i++)
        {
            queryables[i] = setMethod.MakeGenericMethod(query.SourceTypes[i]).Invoke(context, null)!;
        }

        var result = (IQueryable?)query.Lambda.DynamicInvoke(queryables)
            ?? throw new InvalidOperationException("Materialized view query lambda returned null.");

        return result.ToQueryString();
    }

    private static ClickHouseCreateMaterializedViewOperation BuildCreateOperation(
        MaterializedViewDefinition view,
        IReadOnlyModel model)
    {
        var targetType = Type.GetType(view.TargetTypeName)
            ?? throw new InvalidOperationException(
                $"Materialized view '{view.Name}' target type '{view.TargetTypeName}' could not be resolved.");

        var entityType = model.FindEntityType(targetType)
            ?? throw new InvalidOperationException(
                $"Materialized view '{view.Name}' target type '{targetType.Name}' is not a registered entity type.");

        var targetTable = entityType.GetTableName()
            ?? throw new InvalidOperationException(
                $"Materialized view '{view.Name}' target entity '{targetType.Name}' is not mapped to a table.");

        if (string.IsNullOrEmpty(view.SelectSql))
            throw new InvalidOperationException(
                $"Materialized view '{view.Name}' has no SELECT body. Call .Select(...) or .FromRaw(...).");

        return new ClickHouseCreateMaterializedViewOperation
        {
            ViewName = view.Name,
            TargetTable = targetTable,
            TargetDatabase = entityType.GetSchema(),
            SelectQuery = view.SelectSql,
            Populate = view.Populate,
            Cluster = view.Cluster,
            Database = view.Database,
        };
    }

    private static ClickHouseDropMaterializedViewOperation BuildDropOperation(MaterializedViewDefinition view)
        => new()
        {
            ViewName = view.Name,
            Database = view.Database,
            Cluster = view.Cluster,
        };

    private static bool ViewsEqual(MaterializedViewDefinition a, MaterializedViewDefinition b)
        => a.TargetTypeName == b.TargetTypeName
        && a.SelectSql == b.SelectSql
        && a.Populate == b.Populate
        && a.Cluster == b.Cluster
        && a.Database == b.Database;

    private static string ExtractViewName(string annotationName)
    {
        var rest = annotationName[ClickHouseAnnotationNames.MaterializedViewPrefix.Length..];
        var colonIdx = rest.IndexOf(':');
        return colonIdx < 0 ? rest : rest[..colonIdx];
    }
}
