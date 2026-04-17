using System.Collections;
using System.Data.Common;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

public class ClickHouseArrayTypeMapping : RelationalTypeMapping
{
    private static readonly MethodInfo GetValueMethod =
        typeof(DbDataReader).GetRuntimeMethod(nameof(DbDataReader.GetValue), [typeof(int)])!;

    public RelationalTypeMapping ElementMapping { get; }

    /// <summary>
    /// Creates a mapping for T[] CLR types.
    /// </summary>
    public ClickHouseArrayTypeMapping(RelationalTypeMapping elementMapping)
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    elementMapping.ClrType.MakeArrayType(),
                    comparer: CreateArrayComparer(elementMapping.ClrType),
                    elementMapping: ExposableElementMapping(elementMapping)),
                $"Array({elementMapping.StoreType})",
                dbType: System.Data.DbType.Object))
    {
        ElementMapping = elementMapping;
    }

    /// <summary>
    /// Creates a mapping for List&lt;T&gt; CLR types with a ValueConverter to T[].
    /// </summary>
    public ClickHouseArrayTypeMapping(RelationalTypeMapping elementMapping, ValueConverter converter, ValueComparer comparer)
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    converter.ModelClrType,
                    converter: converter,
                    comparer: comparer,
                    elementMapping: ExposableElementMapping(elementMapping)),
                $"Array({elementMapping.StoreType})",
                dbType: System.Data.DbType.Object))
    {
        ElementMapping = elementMapping;
    }

    /// <summary>
    /// Exposes the element mapping to EF Core only when the element is scalar.
    /// EF Core's <c>RelationalModelValidator</c> rejects properties whose mapping chain
    /// reports a "primitive collection of a primitive collection" — i.e. where both the
    /// outer mapping and its element mapping have a non-null <c>ElementTypeMapping</c>.
    /// ClickHouse composite types (arrays of arrays for Polygon/MultiPolygon, Tuple, Map,
    /// Variant, Dynamic) intentionally compose via nested mappings, so hiding the element
    /// mapping in those cases keeps the validator happy without affecting SQL generation.
    /// </summary>
    private static RelationalTypeMapping? ExposableElementMapping(RelationalTypeMapping elementMapping)
        => elementMapping.ElementTypeMapping is null ? elementMapping : null;

    protected ClickHouseArrayTypeMapping(RelationalTypeMappingParameters parameters, RelationalTypeMapping elementMapping)
        : base(parameters)
    {
        ElementMapping = elementMapping;
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseArrayTypeMapping(parameters, ElementMapping);

    public override MethodInfo GetDataReaderMethod()
        => GetValueMethod;

    public override Expression CustomizeDataReaderExpression(Expression expression)
    {
        // When there's a ValueConverter (e.g. List<T> ↔ T[]), the data reader must produce
        // the provider type (T[]). EF Core applies the converter afterward.
        var targetType = Converter?.ProviderClrType ?? ClrType;
        return Expression.Convert(expression, targetType);
    }

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var sb = new StringBuilder("[");
        var first = true;
        foreach (var element in (IEnumerable)value)
        {
            if (!first) sb.Append(", ");
            sb.Append(element is null ? "NULL" : ElementMapping.GenerateSqlLiteral(element));
            first = false;
        }
        sb.Append(']');
        return sb.ToString();
    }

    private static ValueComparer CreateArrayComparer(Type elementType)
    {
        var method = typeof(ClickHouseArrayTypeMapping)
            .GetMethod(nameof(CreateTypedArrayComparer), BindingFlags.Static | BindingFlags.NonPublic)!
            .MakeGenericMethod(elementType);
        return (ValueComparer)method.Invoke(null, null)!;
    }

    internal static ValueComparer CreateListComparer(Type elementType)
    {
        var method = typeof(ClickHouseArrayTypeMapping)
            .GetMethod(nameof(CreateTypedListComparer), BindingFlags.Static | BindingFlags.NonPublic)!
            .MakeGenericMethod(elementType);
        return (ValueComparer)method.Invoke(null, null)!;
    }

    /// <summary>
    /// Creates a <see cref="ValueComparer"/> for a collection-interface model type
    /// (<c>IEnumerable&lt;T&gt;</c>, <c>IList&lt;T&gt;</c>, <c>IReadOnlyList&lt;T&gt;</c>, etc.).
    /// The snapshot direction returns a fresh <c>T[]</c> cast to the interface — safe because
    /// <c>T[]</c> implements all the standard collection interfaces in .NET.
    /// </summary>
    internal static ValueComparer CreateEnumerableComparer(Type collectionType, Type elementType)
    {
        var method = typeof(ClickHouseArrayTypeMapping)
            .GetMethod(nameof(CreateTypedEnumerableComparer), BindingFlags.Static | BindingFlags.NonPublic)!
            .MakeGenericMethod(collectionType, elementType);
        return (ValueComparer)method.Invoke(null, null)!;
    }

    private static ValueComparer<T[]?> CreateTypedArrayComparer<T>()
        => new(
            (a, b) => StructuralComparisons.StructuralEqualityComparer.Equals(a, b),
            o => o == null ? 0 : StructuralComparisons.StructuralEqualityComparer.GetHashCode(o),
            source => source == null ? null : (T[])source.Clone());

    private static ValueComparer<List<T>?> CreateTypedListComparer<T>()
        => new(
            (a, b) => (a == null && b == null) || (a != null && b != null && a.SequenceEqual(b)),
            o => o == null ? 0 : o.Aggregate(0, (hash, el) => HashCode.Combine(hash, el)),
            source => source == null ? null : new List<T>(source));

    private static ValueComparer<TCollection?> CreateTypedEnumerableComparer<TCollection, T>()
        where TCollection : class, IEnumerable<T>
        => new(
            (a, b) => (a == null && b == null) || (a != null && b != null && a.SequenceEqual(b)),
            o => o == null ? 0 : o.Aggregate(0, (hash, el) => HashCode.Combine(hash, el)),
            source => SnapshotTyped<TCollection, T>(source));

    // Snapshot mirrors EnumerableToArrayConverter.FromArray: fresh List<T> for the mutable
    // interfaces (so change-tracking comparison against a user-mutated collection doesn't
    // spuriously report equal), plain array cast for the read-only ones.
    private static TCollection? SnapshotTyped<TCollection, T>(TCollection? source)
        where TCollection : class, IEnumerable<T>
    {
        if (source is null)
            return null;

        if (typeof(TCollection) == typeof(IList<T>)
            || typeof(TCollection) == typeof(ICollection<T>))
        {
            return (TCollection)(object)new List<T>(source);
        }

        return (TCollection)(object)source.ToArray();
    }
}
