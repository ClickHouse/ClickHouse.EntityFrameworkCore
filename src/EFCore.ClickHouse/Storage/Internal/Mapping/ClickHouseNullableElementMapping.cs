using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

/// <summary>
/// Wraps a value-type scalar mapping and reports <c>Nullable&lt;T&gt;</c> as its
/// <see cref="RelationalTypeMapping.ClrType"/> — used by composite resolvers (Array, Tuple,
/// Map, Variant) that build their CLR type from the component element CLR types.
/// </summary>
/// <remarks>
/// <para>
/// EF Core scalar columns carry per-property nullability on <see cref="Microsoft.EntityFrameworkCore.Metadata.IProperty.IsNullable"/>,
/// so <see cref="ClickHouseTypeMappingSource"/>'s <c>FindMapping</c> strips
/// <c>Nullable(...)</c> wrappers in <c>ParseStoreTypeName</c> and returns the unwrapped scalar
/// mapping. That convention works for scalar columns but breaks composites: an
/// <c>Array(Nullable(Int32))</c> property is <c>int?[]</c> at the CLR level, so the composite must
/// take element nullability from the element mapping's <see cref="RelationalTypeMapping.ClrType"/>.
/// <para>
/// For a primitive collection, EF Core does model this on
/// <see cref="Microsoft.EntityFrameworkCore.Metadata.IReadOnlyElementType.IsNullable"/>, and the
/// resolver should prefer that channel. It has no equivalent for a <c>Map</c> value or a single
/// <c>Tuple</c> position, so this wrapper stays necessary for those.
/// </para>
/// </para>
/// <para>
/// This wrapper exists for that single purpose: report <c>Nullable&lt;T&gt;</c> as the CLR
/// type while delegating every other concern (data-reader method, SQL literal generation,
/// type facets) to the inner scalar mapping. Reference-type elements don't need it — their
/// runtime type is identical with or without the C# nullable-reference annotation —
/// and neither does <c>LowCardinality(T)</c> alone (storage-only encoding, same CLR type).
/// </para>
/// </remarks>
public sealed class ClickHouseNullableElementMapping : RelationalTypeMapping
{
    public RelationalTypeMapping Inner { get; }

    public ClickHouseNullableElementMapping(RelationalTypeMapping inner)
        : base(BuildParameters(inner))
    {
        Inner = inner;
    }

    private ClickHouseNullableElementMapping(RelationalTypeMappingParameters parameters, RelationalTypeMapping inner)
        : base(parameters)
    {
        Inner = inner;
    }

    private static RelationalTypeMappingParameters BuildParameters(RelationalTypeMapping inner)
    {
        if (!inner.ClrType.IsValueType || Nullable.GetUnderlyingType(inner.ClrType) is not null)
            throw new ArgumentException(
                $"ClickHouseNullableElementMapping is only meaningful over non-nullable value-type mappings; got CLR type {inner.ClrType}.",
                nameof(inner));

        return new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(
                clrType: typeof(Nullable<>).MakeGenericType(inner.ClrType),
                converter: inner.Converter,
                comparer: inner.Comparer,
                keyComparer: inner.KeyComparer,
                providerValueComparer: inner.ProviderValueComparer,
                valueGeneratorFactory: null,
                elementMapping: inner.ElementTypeMapping,
                jsonValueReaderWriter: inner.JsonValueReaderWriter),
            FormatStoreType(inner.StoreType),
            inner.StoreTypePostfix,
            inner.DbType,
            inner.IsUnicode,
            inner.Size,
            inner.IsFixedLength,
            inner.Precision,
            inner.Scale);
    }

    /// <summary>
    /// Adds the <c>Nullable(...)</c> wrapper unless the inner store type already carries one.
    /// </summary>
    /// <remarks>
    /// The inner mapping is resolved from the component store type, which still holds the
    /// <c>Nullable(...)</c> text, and <c>PreserveExplicitStoreType</c> keeps that text verbatim.
    /// Wrapping it again would give <c>Nullable(Nullable(T))</c>, which ClickHouse rejects with
    /// <c>Nested type Nullable(T) cannot be inside Nullable type</c>.
    /// </remarks>
    private static string FormatStoreType(string innerStoreType)
        => DenotesNullable(innerStoreType) ? innerStoreType : $"Nullable({innerStoreType})";

    private static bool DenotesNullable(string storeType)
    {
        var s = storeType.AsSpan().Trim();

        // LowCardinality(Nullable(T)) is already nullable; the wrapper must not be added again.
        if (s.StartsWith("LowCardinality(", StringComparison.OrdinalIgnoreCase) && s.EndsWith(")"))
            s = s["LowCardinality(".Length..^1].Trim();

        return s.StartsWith("Nullable(", StringComparison.OrdinalIgnoreCase);
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseNullableElementMapping(parameters, Inner);

    public override MethodInfo GetDataReaderMethod() => Inner.GetDataReaderMethod();

    // Delegate the read conversion as well, so a composite over Nullable(DateTime64) or
    // Nullable(Date32) still converts each element. Callers handle the null case before this
    // runs, so the inner non-nullable conversion is safe here.
    public override Expression CustomizeDataReaderExpression(Expression expression)
        => Inner.CustomizeDataReaderExpression(expression);

    protected override string GenerateNonNullSqlLiteral(object value) => Inner.GenerateSqlLiteral(value);
}
