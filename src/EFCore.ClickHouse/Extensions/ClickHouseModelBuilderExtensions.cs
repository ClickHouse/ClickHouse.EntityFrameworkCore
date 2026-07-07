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

    /// <summary>
    /// Begin defining a ClickHouse dictionary named <paramref name="dictionaryName"/>. The dictionary's
    /// columns are the public properties of <typeparamref name="TDictionary"/>. Chain with
    /// <c>.FromTable&lt;TSource&gt;().HasKey(...).Layout(...).Lifetime(...)</c>.
    /// </summary>
    public static ClickHouseDictionaryBuilder<TDictionary> HasDictionary<TDictionary>(
        this ModelBuilder modelBuilder,
        string dictionaryName)
        where TDictionary : class
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(dictionaryName);

        // Make the dictionary queryable as `SELECT … FROM <name>`: map TDictionary as a keyless
        // view entity. ClickHouse exposes a dictionary as a readable table, and view-mapped entities
        // are never migrated as tables — so the CREATE DICTIONARY migration is unaffected. Skip this
        // if the type is already mapped (e.g. it doubles as its own source table, already queryable).
        if (modelBuilder.Model.FindEntityType(typeof(TDictionary)) is null)
            modelBuilder.Entity<TDictionary>().HasNoKey().ToView(dictionaryName);

        return new((IMutableModel)modelBuilder.Model, dictionaryName);
    }
}
