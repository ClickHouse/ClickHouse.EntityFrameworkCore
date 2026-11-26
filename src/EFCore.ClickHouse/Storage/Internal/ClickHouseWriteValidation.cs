using System.Collections;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal;

/// <summary>
/// Runs <see cref="IClickHouseWriteValidatingTypeMapping"/> on a value before the bulk insert path
/// writes it, including each component of an <c>Array</c>, <c>Map</c> or <c>Tuple</c> value.
/// </summary>
/// <remarks>
/// A composite mapping does not check its components itself, so a <c>DateTimeOffset</c> inside an
/// <c>Array(DateTime64(9))</c> must be found here. Most columns have no part to check, so that
/// answer is kept per mapping and such a column costs one lookup per row.
/// </remarks>
internal static class ClickHouseWriteValidation
{
    private static readonly ConcurrentDictionary<RelationalTypeMapping, bool> AppliesCache = new();

    /// <summary>
    /// Reports whether a value of <paramref name="mapping"/> has a part that must be checked before
    /// it is written.
    /// </summary>
    public static bool Applies(RelationalTypeMapping mapping)
        => AppliesCache.GetOrAdd(mapping, static m => m switch
        {
            IClickHouseWriteValidatingTypeMapping => true,
            ClickHouseNullableElementMapping nullable => Applies(nullable.Inner),
            ClickHouseArrayTypeMapping array => Applies(array.ElementMapping),
            ClickHouseMapTypeMapping map => Applies(map.KeyMapping) || Applies(map.ValueMapping),
            ClickHouseTupleTypeMapping tuple => tuple.ElementMappings.Any(Applies),
            _ => false
        });

    /// <summary>
    /// Throws when <paramref name="value"/>, or one of its components, cannot be written to the
    /// store type of <paramref name="mapping"/>.
    /// </summary>
    /// <param name="mapping">The column's type mapping.</param>
    /// <param name="value">The model value. A null value or component is not checked.</param>
    /// <param name="columnName">The column being written, for the error message.</param>
    public static void Validate(RelationalTypeMapping mapping, object? value, string? columnName)
    {
        if (value is null or DBNull)
            return;

        switch (mapping)
        {
            case IClickHouseWriteValidatingTypeMapping validating:
                validating.ValidateWriteValue(value, columnName);
                break;

            case ClickHouseNullableElementMapping nullable:
                Validate(nullable.Inner, value, columnName);
                break;

            case ClickHouseArrayTypeMapping array when value is IEnumerable elements:
                foreach (var element in elements)
                    Validate(array.ElementMapping, element, columnName);
                break;

            case ClickHouseMapTypeMapping map when value is IDictionary entries:
                foreach (DictionaryEntry entry in entries)
                {
                    Validate(map.KeyMapping, entry.Key, columnName);
                    Validate(map.ValueMapping, entry.Value, columnName);
                }
                break;

            case ClickHouseTupleTypeMapping tuple when value is ITuple components:
                for (var i = 0; i < components.Length && i < tuple.ElementMappings.Count; i++)
                    Validate(tuple.ElementMappings[i], components[i], columnName);
                break;
        }
    }
}
