using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

/// <summary>
/// Bridges an <see cref="IEnumerable{T}"/>-derived collection model type (e.g. <c>IList&lt;T&gt;</c>,
/// <c>IReadOnlyList&lt;T&gt;</c>, <c>ICollection&lt;T&gt;</c>) and the provider-side <c>T[]</c>
/// storage representation. <c>T[]</c> implements all standard collection interfaces in
/// .NET, so the provider→model direction is a reference cast; the model→provider direction
/// materializes via <see cref="Enumerable.ToArray{T}"/>.
/// </summary>
public class EnumerableToArrayConverter<TCollection, T> : ValueConverter<TCollection, T[]>
    where TCollection : class, IEnumerable<T>
{
    public EnumerableToArrayConverter()
        : base(
            coll => coll == null ? Array.Empty<T>() : coll.ToArray(),
            arr => (TCollection)(object)(arr ?? Array.Empty<T>()))
    {
    }
}
