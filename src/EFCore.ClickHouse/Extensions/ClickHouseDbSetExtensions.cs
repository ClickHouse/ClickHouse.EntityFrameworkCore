using ClickHouse.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Query;

namespace Microsoft.EntityFrameworkCore;

/// <summary>
/// ClickHouse-specific extension methods for LINQ queries rooted in a <see cref="DbSet{TEntity}"/>.
/// </summary>
public static class ClickHouseDbSetExtensions
{
    /// <summary>
    /// Adds the ClickHouse <c>FINAL</c> modifier to the table of the entity type in the query.
    /// ClickHouse then applies the merge logic of the table engine at query time, for example
    /// one row for each sorting key of a <c>ReplacingMergeTree</c> table.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Use this method to read the current rows of a table with a merging engine, for example
    /// <c>ReplacingMergeTree</c>, <c>CollapsingMergeTree</c> or <c>AggregatingMergeTree</c>,
    /// before the background merges are complete. Queries with <c>FINAL</c> can be slower than
    /// queries without it.
    /// </para>
    /// <para>
    /// <c>FINAL</c> applies to all tables of this entity type (TPT, TPC and entity splitting), but
    /// not to other entity types in the query. Navigations, <c>Include</c> and owned types in other
    /// tables read their tables without <c>FINAL</c>. Call <c>Final()</c> on each
    /// <see cref="DbSet{TEntity}"/> that needs it, or set the ClickHouse <c>final</c> setting to
    /// apply <c>FINAL</c> to all tables that support it.
    /// </para>
    /// <para>
    /// ClickHouse rejects <c>FINAL</c> for engines that do not merge rows, for example
    /// <c>MergeTree</c>, <c>Memory</c> and <c>Log</c>, with the <c>ILLEGAL_FINAL</c> error.
    /// On a view that is not materialized, ClickHouse accepts <c>FINAL</c> but ignores it.
    /// </para>
    /// </remarks>
    /// <param name="source">The <see cref="DbSet{TEntity}"/> to read with <c>FINAL</c>.</param>
    /// <returns>A query that reads the entity type with <c>FINAL</c>.</returns>
    public static IQueryable<TEntity> Final<TEntity>(this DbSet<TEntity> source)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(source);

        var queryable = (IQueryable)source;
        var root = (EntityQueryRootExpression)queryable.Expression;

        return queryable.Provider.CreateQuery<TEntity>(
            new ClickHouseFinalQueryRootExpression(root.QueryProvider!, root.EntityType));
    }
}
