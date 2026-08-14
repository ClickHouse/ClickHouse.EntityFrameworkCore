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

    private static readonly MethodInfo ConvertArrayMethod =
        typeof(ClickHouseArrayTypeMapping).GetMethod(nameof(ConvertArray), BindingFlags.Static | BindingFlags.NonPublic)!;

    public RelationalTypeMapping ElementMapping { get; }

    /// <summary>
    /// Creates a mapping for T[] CLR types. Element nullability flows through
    /// <paramref name="elementMapping"/>'s <see cref="RelationalTypeMapping.ClrType"/>:
    /// <c>Array(Nullable(Int32))</c> arrives here with a <see cref="ClickHouseNullableElementMapping"/>
    /// whose <c>ClrType</c> is <c>int?</c>, so <c>MakeArrayType()</c> produces <c>int?[]</c>.
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

        // An element whose CLR type differs from what the driver produces (DateTimeOffset and
        // DateOnly both arrive as DateTime) needs the array rebuilt element by element. Casting
        // the whole array would throw InvalidCastException. Otherwise cast directly, which is
        // both correct and cheaper.
        if (!ClickHouseComponentConversion.NeedsConversion(ElementMapping))
            return Expression.Convert(expression, targetType);

        var elementType = ElementMapping.ClrType;
        Expression converted = Expression.Call(
            ConvertArrayMethod.MakeGenericMethod(elementType),
            expression,
            ClickHouseComponentConversion.CreateConverter(ElementMapping, elementType),
            Expression.Constant(ClickHouseComponentConversion.CanPassThrough(ElementMapping)));

        return converted.Type == targetType ? converted : Expression.Convert(converted, targetType);
    }

    /// <summary>
    /// Rebuilds the driver's array as <c>TElement[]</c>, converting each element. Nested composites
    /// compose through this: an <c>Array(Array(DateTime64))</c> element mapping is itself a
    /// <see cref="ClickHouseArrayTypeMapping"/>, so its own conversion runs per element.
    /// </summary>
    private static TElement[] ConvertArray<TElement>(
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
