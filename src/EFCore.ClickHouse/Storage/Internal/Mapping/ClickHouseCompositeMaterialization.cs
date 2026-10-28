using System.ComponentModel;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

/// <summary>
/// Helpers that composite type mappings (Array, Map, Tuple) call from materializer expressions.
/// </summary>
/// <remarks>
/// This is a provider-internal API. It is public only because code generated for precompiled
/// queries calls it directly and lives in the user's assembly. It is not intended to be used from
/// application code and may change or be removed in any release without notice.
/// </remarks>
[EditorBrowsable(EditorBrowsableState.Never)]
public static class ClickHouseCompositeMaterialization
{
    /// <summary>
    /// Rebuilds the driver's array as <c>TElement[]</c>, converting each element. Nested composites
    /// compose through this: an <c>Array(Array(DateTime64))</c> element mapping is itself a
    /// <see cref="ClickHouseArrayTypeMapping"/>, so its own conversion runs per element.
    /// </summary>
    public static TElement[] ConvertArray<TElement>(
        object value,
        Func<object, TElement> convertElement,
        bool canPassThrough)
    {
        // The driver often already produces the target type, for example Array(Int32) -> int[].
        // See ClickHouseComponentConversion.CanPassThrough for when that proves there is no work
        // left to do.
        if (canPassThrough && value is TElement[] alreadyTyped)
            return alreadyTyped;

        var source = (Array)value;
        var result = new TElement[source.Length];
        for (var i = 0; i < source.Length; i++)
        {
            var element = source.GetValue(i);
            result[i] = element is null or DBNull ? default! : convertElement(element);
        }

        return result;
    }
}