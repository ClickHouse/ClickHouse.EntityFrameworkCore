using System.Collections.Concurrent;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

/// <summary>
/// Lets a composite mapping (Array, Map, Tuple) apply its component mappings' own read conversions
/// to each component of a value that the driver returned.
/// </summary>
/// <remarks>
/// <para>
/// A composite mapping reads its whole column through <c>GetValue</c>, so the component mappings'
/// read pipeline never runs. Where a component's CLR type differs from the type the driver produces,
/// casting the whole composite throws <see cref="InvalidCastException"/>. The composite must instead
/// be rebuilt component by component, reusing each component mapping's existing conversion rather
/// than repeating it here.
/// </para>
/// <para>
/// A component read has the same two steps EF Core applies to a scalar column:
/// <list type="number">
///   <item><description>
///     <see cref="RelationalTypeMapping.CustomizeDataReaderExpression"/> turns the raw driver value
///     into the provider CLR type — this is where <see cref="DateTimeOffset"/> and
///     <see cref="DateOnly"/> are built from the <see cref="DateTime"/> the driver returns.
///   </description></item>
///   <item><description>
///     The mapping's <see cref="RelationalTypeMapping.Converter"/> turns the provider CLR type into
///     the model CLR type — this is where an <c>Enum8</c> component becomes a C# <c>enum</c> and an
///     <c>Array(T)</c> component becomes a <c>List&lt;T&gt;</c>.
///   </description></item>
/// </list>
/// Both steps are needed. Applying only the first would leave a component mapping that carries a
/// converter readable as its provider type but not as the type the property declares.
/// </para>
/// </remarks>
internal static class ClickHouseComponentConversion
{
    // Keyed by (mapping, target type). The compiled converter is embedded in the materializer as a
    // constant, so it is built once per mapping rather than once per row.
    private static readonly ConcurrentDictionary<(RelationalTypeMapping Mapping, Type Target), Delegate> ConverterCache = new();

    private static readonly ConcurrentDictionary<RelationalTypeMapping, bool> NeedsConversionCache = new();

    /// <summary>
    /// Reports whether reading <paramref name="mapping"/> changes the value the driver produced.
    /// </summary>
    /// <remarks>
    /// Cached, because a nested composite's answer depends on its own components' answers and the
    /// probe below builds a throw-away sub-tree to find out.
    /// </remarks>
    public static bool NeedsConversion(RelationalTypeMapping mapping)
        => NeedsConversionCache.GetOrAdd(mapping, static m =>
        {
            // A value converter always changes the value.
            if (m.Converter is not null)
                return true;

            // CustomizeDataReaderExpression returns its argument unchanged when a mapping needs no
            // conversion, so reference equality against a probe is a reliable test.
            var probe = Expression.Parameter(typeof(object), "component");
            return !ReferenceEquals(m.CustomizeDataReaderExpression(probe), probe);
        });

    /// <summary>
    /// Reports whether a composite that the driver already produced at the target CLR type may be
    /// returned unchanged, instead of being rebuilt component by component.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Rebuilding is only needed where reading a component changes its value. The composite
    /// mappings therefore keep a fast path for a driver value that already has the target type —
    /// which earns its keep for a nested composite such as <c>Array(Array(Int32))</c>, where the
    /// inner mapping's read is a cast and nothing more.
    /// </para>
    /// <para>
    /// That fast path is only sound where matching CLR types prove there is nothing left to do.
    /// It holds for <see cref="RelationalTypeMapping.CustomizeDataReaderExpression"/>: every
    /// component mapping that uses it either changes the CLR type — <see cref="DateTimeOffset"/>
    /// and <see cref="DateOnly"/> are both built from a <see cref="DateTime"/> — or coerces a
    /// numeric type the driver may return too wide, which is a no-op once the type already matches.
    /// It does not hold for a <see cref="RelationalTypeMapping.Converter"/>, which is free to
    /// change the value while keeping the CLR type. So a component that carries one is always
    /// rebuilt.
    /// </para>
    /// </remarks>
    /// <remarks>
    /// <para>
    /// The check recurses. A composite component carries no converter of its own — an
    /// <c>Array(T)</c> read as <c>T[]</c> does not — so looking only at the immediate mapping would
    /// report that an <c>Array(Array(T))</c> element needs nothing done, and the fast path would
    /// return the driver's value with the innermost components left unconverted.
    /// </para>
    /// </remarks>
    public static bool CanPassThrough(RelationalTypeMapping mapping)
    {
        if (mapping.Converter is not null)
            return false;

        return mapping switch
        {
            ClickHouseNullableElementMapping nullable => CanPassThrough(nullable.Inner),
            ClickHouseArrayTypeMapping array => CanPassThrough(array.ElementMapping),
            ClickHouseMapTypeMapping map => CanPassThrough(map.KeyMapping) && CanPassThrough(map.ValueMapping),
            ClickHouseTupleTypeMapping tuple => tuple.ElementMappings.All(CanPassThrough),
            _ => true
        };
    }

    /// <summary>
    /// Returns an expression of type <c>Func&lt;object, TComponent&gt;</c> that reads one component,
    /// where <c>TComponent</c> is <paramref name="componentType"/>.
    /// </summary>
    /// <remarks>
    /// The converter is compiled once per mapping and embedded as a constant. An inline
    /// <see cref="Expression.Lambda(Expression, ParameterExpression[])"/> would instead be rebuilt on
    /// every materialization, allocating a delegate per row for every composite column. The trade-off
    /// is that a constant holding a delegate cannot be quoted, so composite columns whose components
    /// convert are not usable from precompiled queries.
    /// </remarks>
    public static Expression CreateConverter(RelationalTypeMapping mapping, Type componentType)
        => Expression.Constant(
            GetConverter(mapping, componentType),
            typeof(Func<,>).MakeGenericType(typeof(object), componentType));

    /// <summary>
    /// Returns the compiled reader for one component as a <c>Func&lt;object, object?&gt;</c>, for a
    /// caller that holds the readers in an array rather than embedding each one separately.
    /// </summary>
    public static Func<object, object?> CreateComponentReader(RelationalTypeMapping mapping)
        => (Func<object, object?>)GetConverter(mapping, typeof(object));

    private static Delegate GetConverter(RelationalTypeMapping mapping, Type componentType)
        => ConverterCache.GetOrAdd(
            (mapping, componentType),
            static key => Compile(key.Mapping, key.Target));

    private static Delegate Compile(RelationalTypeMapping mapping, Type componentType)
    {
        var parameter = Expression.Parameter(typeof(object), "component");
        var body = mapping.CustomizeDataReaderExpression(parameter);

        if (mapping.Converter is { } valueConverter)
        {
            // Step 1 produces the provider CLR type, which is what the converter accepts.
            body = Coerce(body, valueConverter.ProviderClrType, mapping, componentType);
            body = Expression.Invoke(valueConverter.ConvertFromProviderExpression, body);
        }

        body = Coerce(body, componentType, mapping, componentType);

        return Expression.Lambda(
                typeof(Func<,>).MakeGenericType(typeof(object), componentType),
                body,
                parameter)
            .Compile();
    }

    private static Expression Coerce(
        Expression expression,
        Type targetType,
        RelationalTypeMapping mapping,
        Type componentType)
    {
        if (expression.Type == targetType)
            return expression;

        try
        {
            return Expression.Convert(expression, targetType);
        }
        catch (InvalidOperationException ex)
        {
            // Without this the user sees EF Core's bare "No coercion operator is defined between
            // types ...", which names two CLR types they never wrote.
            throw new NotSupportedException(
                $"The ClickHouse provider cannot read a component of store type '{mapping.StoreType}' "
                + $"as '{componentType}'. Reading it produces '{expression.Type}', and no conversion "
                + $"to '{targetType}' exists. Change the property type to match the column, or set an "
                + $"explicit column type with HasColumnType(...).",
                ex);
        }
    }
}
