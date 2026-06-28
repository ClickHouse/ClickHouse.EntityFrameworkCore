using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Metadata;

namespace ClickHouse.EntityFrameworkCore.Metadata.Builders;

/// <summary>
/// Builder returned by <c>ModelBuilder.HasMaterializedView&lt;TTarget&gt;(name)</c>.
/// Choose a source via <see cref="From{TSource}"/> or provide raw SQL via <see cref="FromRaw"/>.
/// </summary>
public sealed class ClickHouseMaterializedViewBuilder<TTarget> where TTarget : class
{
    private readonly IMutableModel _model;
    private readonly string _viewName;

    internal ClickHouseMaterializedViewBuilder(IMutableModel model, string viewName)
    {
        _model = model;
        _viewName = viewName;
        MaterializedViewAnnotations.SetTargetType(_model, _viewName, typeof(TTarget));
    }

    public ClickHouseMaterializedViewBuilder<TTarget, TSource> From<TSource>() where TSource : class
        => new(_model, _viewName);

    public ConfiguredClickHouseMaterializedViewBuilder FromRaw(string selectSql)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(selectSql);
        MaterializedViewAnnotations.SetSelectSql(_model, _viewName, selectSql);
        MaterializedViewAnnotations.RemovePendingLambda(_model, _viewName);
        return new(_model, _viewName);
    }
}

public sealed class ClickHouseMaterializedViewBuilder<TTarget, TSource>
    where TTarget : class where TSource : class
{
    private readonly IMutableModel _model;
    private readonly string _viewName;

    internal ClickHouseMaterializedViewBuilder(IMutableModel model, string viewName)
    {
        _model = model;
        _viewName = viewName;
    }

    public ClickHouseMaterializedViewBuilder<TTarget, TSource, TJoin> Join<TJoin>() where TJoin : class
        => new(_model, _viewName);

    public ConfiguredClickHouseMaterializedViewBuilder Select(
        Func<IQueryable<TSource>, IQueryable<TTarget>> body)
    {
        ArgumentNullException.ThrowIfNull(body);
        MaterializedViewAnnotations.SetPendingLambda(_model, _viewName, new PendingMaterializedViewQuery
        {
            TargetType = typeof(TTarget),
            SourceTypes = [typeof(TSource)],
            Lambda = body,
        });
        return new(_model, _viewName);
    }
}

public sealed class ClickHouseMaterializedViewBuilder<TTarget, TSource1, TSource2>
    where TTarget : class where TSource1 : class where TSource2 : class
{
    private readonly IMutableModel _model;
    private readonly string _viewName;

    internal ClickHouseMaterializedViewBuilder(IMutableModel model, string viewName)
    {
        _model = model;
        _viewName = viewName;
    }

    public ClickHouseMaterializedViewBuilder<TTarget, TSource1, TSource2, TJoin> Join<TJoin>() where TJoin : class
        => new(_model, _viewName);

    public ConfiguredClickHouseMaterializedViewBuilder Select(
        Func<IQueryable<TSource1>, IQueryable<TSource2>, IQueryable<TTarget>> body)
    {
        ArgumentNullException.ThrowIfNull(body);
        MaterializedViewAnnotations.SetPendingLambda(_model, _viewName, new PendingMaterializedViewQuery
        {
            TargetType = typeof(TTarget),
            SourceTypes = [typeof(TSource1), typeof(TSource2)],
            Lambda = body,
        });
        return new(_model, _viewName);
    }
}

public sealed class ClickHouseMaterializedViewBuilder<TTarget, TSource1, TSource2, TSource3>
    where TTarget : class where TSource1 : class where TSource2 : class where TSource3 : class
{
    private readonly IMutableModel _model;
    private readonly string _viewName;

    internal ClickHouseMaterializedViewBuilder(IMutableModel model, string viewName)
    {
        _model = model;
        _viewName = viewName;
    }

    public ConfiguredClickHouseMaterializedViewBuilder Select(
        Func<IQueryable<TSource1>, IQueryable<TSource2>, IQueryable<TSource3>, IQueryable<TTarget>> body)
    {
        ArgumentNullException.ThrowIfNull(body);
        MaterializedViewAnnotations.SetPendingLambda(_model, _viewName, new PendingMaterializedViewQuery
        {
            TargetType = typeof(TTarget),
            SourceTypes = [typeof(TSource1), typeof(TSource2), typeof(TSource3)],
            Lambda = body,
        });
        return new(_model, _viewName);
    }
}

/// <summary>
/// Terminal builder returned after <c>Select</c> or <c>FromRaw</c>.
/// Apply optional view configuration such as POPULATE, ON CLUSTER, or database qualifier.
/// </summary>
public sealed class ConfiguredClickHouseMaterializedViewBuilder
{
    private readonly IMutableModel _model;
    private readonly string _viewName;

    internal ConfiguredClickHouseMaterializedViewBuilder(IMutableModel model, string viewName)
    {
        _model = model;
        _viewName = viewName;
    }

    public ConfiguredClickHouseMaterializedViewBuilder OnCluster(string clusterName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterName);
        MaterializedViewAnnotations.SetCluster(_model, _viewName, clusterName);
        return this;
    }

    public ConfiguredClickHouseMaterializedViewBuilder InDatabase(string databaseName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(databaseName);
        MaterializedViewAnnotations.SetDatabase(_model, _viewName, databaseName);
        return this;
    }
}

internal static class MaterializedViewAnnotations
{
    private static string Prefix(string viewName)
        => ClickHouseAnnotationNames.MaterializedViewPrefix + viewName + ":";

    public static void SetTargetType(IMutableModel model, string viewName, Type targetType)
        => model.SetOrRemoveAnnotation(
            Prefix(viewName) + ClickHouseAnnotationNames.MaterializedViewTargetTypeSuffix,
            targetType.AssemblyQualifiedName);

    public static void SetSelectSql(IMutableModel model, string viewName, string sql)
        => model.SetOrRemoveAnnotation(
            Prefix(viewName) + ClickHouseAnnotationNames.MaterializedViewSelectSqlSuffix,
            sql);

    public static void SetPopulate(IMutableModel model, string viewName, bool populate)
        => model.SetOrRemoveAnnotation(
            Prefix(viewName) + ClickHouseAnnotationNames.MaterializedViewPopulateSuffix,
            populate ? (object)true : null);

    public static void SetCluster(IMutableModel model, string viewName, string clusterName)
        => model.SetOrRemoveAnnotation(
            Prefix(viewName) + ClickHouseAnnotationNames.MaterializedViewClusterSuffix,
            clusterName);

    public static void SetDatabase(IMutableModel model, string viewName, string databaseName)
        => model.SetOrRemoveAnnotation(
            Prefix(viewName) + ClickHouseAnnotationNames.MaterializedViewDatabaseSuffix,
            databaseName);

    public static void SetPendingLambda(IMutableModel model, string viewName, PendingMaterializedViewQuery pending)
        => model.SetOrRemoveAnnotation(
            Prefix(viewName) + ClickHouseAnnotationNames.MaterializedViewPendingLambdaSuffix,
            pending);

    public static void RemovePendingLambda(IMutableModel model, string viewName)
        => model.RemoveAnnotation(
            Prefix(viewName) + ClickHouseAnnotationNames.MaterializedViewPendingLambdaSuffix);
}
