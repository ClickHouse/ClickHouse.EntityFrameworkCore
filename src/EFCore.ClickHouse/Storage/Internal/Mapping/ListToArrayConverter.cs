using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

/// <summary>
/// Converts between List&lt;T&gt; (model) and T[] (provider/driver).
/// The ClickHouse driver always returns arrays; this converter lets users
/// map properties as List&lt;T&gt; for convenience.
/// </summary>
public class ListToArrayConverter<T> : ValueConverter<List<T>, T[]>
{
    public ListToArrayConverter()
        : base(
            list => list == null ? Array.Empty<T>() : list.ToArray(),
            array => array == null ? new List<T>() : new List<T>(array))
    {
    }
}
