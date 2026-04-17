using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

/// <summary>
/// Bridges an <see cref="IEnumerable{T}"/>-derived collection model type (e.g. <c>IList&lt;T&gt;</c>,
/// <c>IReadOnlyList&lt;T&gt;</c>, <c>ICollection&lt;T&gt;</c>) and the provider-side <c>T[]</c>
/// storage representation.
///
/// <para>
/// Model→provider materializes via <see cref="Enumerable.ToArray{T}"/>. Provider→model
/// depends on the interface:
/// </para>
/// <list type="bullet">
///   <item><description><b>Mutable</b> (<c>IList&lt;T&gt;</c>, <c>ICollection&lt;T&gt;</c>) — wraps in a fresh <see cref="List{T}"/> so users can call <c>Add</c> / <c>Remove</c>. A <c>T[]</c> cast to <c>IList&lt;T&gt;</c> reports <c>IsReadOnly=true</c>, so mutation throws.</description></item>
///   <item><description><b>Read-only</b> (<c>IEnumerable&lt;T&gt;</c>, <c>IReadOnlyList&lt;T&gt;</c>, <c>IReadOnlyCollection&lt;T&gt;</c>) — returns the array cast to the interface. No extra allocation; the fixed-size semantics of <c>T[]</c> are compatible with the read-only contract.</description></item>
/// </list>
/// </summary>
public class EnumerableToArrayConverter<TCollection, T> : ValueConverter<TCollection, T[]>
    where TCollection : class, IEnumerable<T>
{
    public EnumerableToArrayConverter()
        : base(
            coll => coll == null ? Array.Empty<T>() : coll.ToArray(),
            arr => FromArray(arr))
    {
    }

    private static readonly bool MutableInterface =
        typeof(TCollection) == typeof(IList<T>)
     || typeof(TCollection) == typeof(ICollection<T>);

    private static TCollection FromArray(T[]? arr)
    {
        arr ??= Array.Empty<T>();
        return MutableInterface
            ? (TCollection)(object)new List<T>(arr)
            : (TCollection)(object)arr;
    }
}
