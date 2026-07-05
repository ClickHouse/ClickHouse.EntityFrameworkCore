using ClickHouse.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace ClickHouse.EntityFrameworkCore.Extensions;

public static class ClickHouseModelBuilderExtensions
{
    /// <summary>
    /// Begin defining a ClickHouse materialized view that writes into the table mapped by <typeparamref name="TTarget"/>.
    /// Chain with <c>.From&lt;TSource&gt;().Select(...)</c> for LINQ-defined views or <c>.FromRaw(sql)</c> for raw SQL.
    /// </summary>
    public static ClickHouseMaterializedViewBuilder<TTarget> HasMaterializedView<TTarget>(
        this ModelBuilder modelBuilder,
        string viewName)
        where TTarget : class
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(viewName);
        return new(((IMutableModel)modelBuilder.Model), viewName);
    }
}
