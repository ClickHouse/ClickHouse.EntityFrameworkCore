using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Metadata;

namespace ClickHouse.EntityFrameworkCore.Metadata.Builders;

/// <summary>
/// Builder returned by <c>EntityTypeBuilder&lt;T&gt;.HasProjection(name)</c>. Define the projection
/// body via <see cref="Select"/> (LINQ over the parent table) or <see cref="FromRaw"/> (raw SQL).
/// A projection's SELECT has no FROM clause — the parent table is implicit; LINQ bodies are
/// translated and their FROM clause and table-alias qualifiers stripped at migration-differ time.
/// </summary>
public sealed class ClickHouseProjectionBuilder<T> where T : class
{
    private readonly IMutableEntityType _entityType;
    private readonly string _projectionName;

    internal ClickHouseProjectionBuilder(IMutableEntityType entityType, string projectionName)
    {
        _entityType = entityType;
        _projectionName = projectionName;
    }

    /// <summary>
    /// Define the projection body from raw ClickHouse SQL — the SELECT list with optional GROUP BY /
    /// ORDER BY and no FROM clause, e.g. <c>"SELECT EventDate, count() GROUP BY EventDate"</c>.
    /// </summary>
    public ConfiguredClickHouseProjectionBuilder<T> FromRaw(string selectSql)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(selectSql);
        ProjectionAnnotations.SetSelectSql(_entityType, _projectionName, selectSql);
        ProjectionAnnotations.RemovePendingLambda(_entityType, _projectionName);
        return new(_entityType, _projectionName);
    }

    /// <summary>
    /// Define the projection body via LINQ over the parent table. The query is translated to SQL and
    /// its <c>FROM</c> clause and table-alias qualifiers are stripped at migration time. Only the
    /// <c>SELECT [GROUP BY] [ORDER BY]</c> shape is supported; for joins or filters use <see cref="FromRaw"/>.
    /// </summary>
    public ConfiguredClickHouseProjectionBuilder<T> Select(Func<IQueryable<T>, IQueryable> body)
    {
        ArgumentNullException.ThrowIfNull(body);
        ProjectionAnnotations.SetPendingLambda(_entityType, _projectionName, new PendingProjectionQuery
        {
            ParentType = typeof(T),
            Lambda = body,
        });
        return new(_entityType, _projectionName);
    }
}

/// <summary>
/// Terminal builder returned after <c>Select</c> or <c>FromRaw</c>. Apply optional projection
/// configuration such as ON CLUSTER, or opt out of the automatic MATERIALIZE.
/// </summary>
public sealed class ConfiguredClickHouseProjectionBuilder<T> where T : class
{
    private readonly IMutableEntityType _entityType;
    private readonly string _projectionName;

    internal ConfiguredClickHouseProjectionBuilder(IMutableEntityType entityType, string projectionName)
    {
        _entityType = entityType;
        _projectionName = projectionName;
    }

    /// <summary>Run the ADD / MATERIALIZE / DROP PROJECTION DDL <c>ON CLUSTER '<paramref name="clusterName"/>'</c>.</summary>
    public ConfiguredClickHouseProjectionBuilder<T> OnCluster(string clusterName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterName);
        ProjectionAnnotations.SetCluster(_entityType, _projectionName, clusterName);
        return this;
    }

    /// <summary>
    /// Skip the automatic <c>MATERIALIZE PROJECTION</c> that follows <c>ADD PROJECTION</c>. By default the
    /// projection is materialized so existing parts are covered; opt out to project only newly-written parts.
    /// </summary>
    public ConfiguredClickHouseProjectionBuilder<T> WithoutMaterialize()
    {
        ProjectionAnnotations.SetMaterialize(_entityType, _projectionName, false);
        return this;
    }
}

internal static class ProjectionAnnotations
{
    private static string Prefix(string name)
        => ClickHouseAnnotationNames.ProjectionPrefix + name + ":";

    public static void SetSelectSql(IMutableEntityType entityType, string name, string sql)
        => entityType.SetOrRemoveAnnotation(Prefix(name) + ClickHouseAnnotationNames.ProjectionSelectSqlSuffix, sql);

    public static void SetCluster(IMutableEntityType entityType, string name, string cluster)
        => entityType.SetOrRemoveAnnotation(Prefix(name) + ClickHouseAnnotationNames.ProjectionClusterSuffix, cluster);

    public static void SetMaterialize(IMutableEntityType entityType, string name, bool materialize)
        => entityType.SetOrRemoveAnnotation(Prefix(name) + ClickHouseAnnotationNames.ProjectionMaterializeSuffix,
            materialize ? null : (object)false);

    public static void SetPendingLambda(IMutableEntityType entityType, string name, PendingProjectionQuery pending)
        => entityType.SetOrRemoveAnnotation(Prefix(name) + ClickHouseAnnotationNames.ProjectionPendingLambdaSuffix, pending);

    public static void RemovePendingLambda(IMutableEntityType entityType, string name)
        => entityType.RemoveAnnotation(Prefix(name) + ClickHouseAnnotationNames.ProjectionPendingLambdaSuffix);
}
