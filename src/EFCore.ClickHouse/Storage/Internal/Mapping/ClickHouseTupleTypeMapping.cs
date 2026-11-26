using System.Collections.Concurrent;
using System.Data.Common;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

public class ClickHouseTupleTypeMapping : RelationalTypeMapping
{
    private static readonly MethodInfo GetValueMethod =
        typeof(DbDataReader).GetRuntimeMethod(nameof(DbDataReader.GetValue), [typeof(int)])!;

    private static readonly MethodInfo ConvertMethod =
        typeof(ClickHouseTupleTypeMapping).GetMethod(nameof(ConvertTuple), BindingFlags.Static | BindingFlags.NonPublic)!;

    // Cache the compiled constructor and its component types per tuple type, to avoid
    // Activator.CreateInstance and reflection per row.
    private static readonly ConcurrentDictionary<Type, (Delegate Factory, Type[] ComponentTypes)> ConstructorCache = new();

    public IReadOnlyList<RelationalTypeMapping> ElementMappings { get; }

    public ClickHouseTupleTypeMapping(IReadOnlyList<RelationalTypeMapping> elementMappings, bool useValueTuple = true)
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    MakeTupleType(
                        elementMappings.Select(ClickHouseNullableElementMapping.ComponentClrType).ToArray(),
                        useValueTuple)),
                FormatStoreType(elementMappings),
                dbType: System.Data.DbType.Object))
    {
        ElementMappings = elementMappings;
    }

    protected ClickHouseTupleTypeMapping(
        RelationalTypeMappingParameters parameters,
        IReadOnlyList<RelationalTypeMapping> elementMappings)
        : base(parameters)
    {
        ElementMappings = elementMappings;
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseTupleTypeMapping(parameters, ElementMappings);

    public override MethodInfo GetDataReaderMethod()
        => GetValueMethod;

    public override Expression CustomizeDataReaderExpression(Expression expression)
    {
        // Two reasons to rebuild: the driver returns System.Tuple<> even where the CLR type is a
        // ValueTuple<>, and a component whose CLR type differs from what the driver produces
        // (DateTimeOffset and DateOnly both arrive as DateTime) cannot be cast in place.
        var needsComponentConversion = ElementMappings.Any(ClickHouseComponentConversion.NeedsConversion);

        if (!ClrType.IsValueType && !needsComponentConversion)
            return Expression.Convert(expression, ClrType);

        // Build the delegate array once and embed it as a constant. Expression.NewArrayInit would
        // instead make the allocation part of the materializer, so every row read would allocate a
        // fresh array of the same delegates — about 40 bytes per row on a two-component tuple.
        var componentConverters = Expression.Constant(
            ElementMappings.Select(ClickHouseComponentConversion.CreateComponentReader).ToArray(),
            typeof(Func<object, object?>[]));

        return Expression.Call(
            ConvertMethod.MakeGenericMethod(ClrType),
            expression,
            componentConverters,
            Expression.Constant(ElementMappings.All(ClickHouseComponentConversion.CanPassThrough)));
    }

    // Rebuilds the driver's tuple as T, converting each component. Handles both ValueTuple<> and
    // System.Tuple<> targets, since both expose a constructor taking every component.
    private static T ConvertTuple<T>(
        object value,
        Func<object, object?>[] convertComponents,
        bool canPassThrough)
    {
        // A ValueTuple target never takes this path, because the driver returns System.Tuple<>.
        // See ClickHouseComponentConversion.CanPassThrough for the component condition.
        if (canPassThrough && value is T alreadyTyped)
            return alreadyTyped;

        if (value is not ITuple tuple)
            throw new InvalidCastException($"Cannot convert {value.GetType()} to {typeof(T)}");

        if (tuple.Length != convertComponents.Length)
            throw new InvalidCastException(
                $"Cannot convert {value.GetType()} to {typeof(T)}: the value has {tuple.Length} "
                + $"components but the mapping expects {convertComponents.Length}.");

        var (factory, componentTypes) = ConstructorCache.GetOrAdd(typeof(T), static type =>
        {
            var constructor = type.GetConstructors()[0];
            var ctorParams = constructor.GetParameters();
            var argsParam = Expression.Parameter(typeof(object[]), "args");
            var bodyArgs = new Expression[ctorParams.Length];

            for (var j = 0; j < ctorParams.Length; j++)
            {
                bodyArgs[j] = Expression.Convert(
                    Expression.ArrayIndex(argsParam, Expression.Constant(j)),
                    ctorParams[j].ParameterType);
            }

            return (
                (Delegate)Expression.Lambda<Func<object[], T>>(
                    Expression.New(constructor, bodyArgs), argsParam).Compile(),
                Array.ConvertAll(ctorParams, p => p.ParameterType));
        });

        var args = new object?[tuple.Length];
        for (var i = 0; i < tuple.Length; i++)
        {
            var component = tuple[i];
            if (component is null or DBNull)
            {
                // The array and map helpers substitute default(T) for a null component. A tuple slot
                // cannot: the constructor takes it positionally, so a null on a non-nullable
                // value-type slot would fail inside the compiled factory as a NullReferenceException.
                // ClickHouse only returns NULL for a Nullable(...) slot, so reaching this means the
                // column and the CLR tuple disagree — say so plainly.
                if (componentTypes[i].IsValueType && Nullable.GetUnderlyingType(componentTypes[i]) is null)
                {
                    throw new InvalidCastException(
                        $"Cannot convert {value.GetType()} to {typeof(T)}: component {i} is NULL but "
                        + $"'{componentTypes[i]}' is not nullable. Declare that tuple component as "
                        + $"'{componentTypes[i]}?', or make the column non-nullable.");
                }

                args[i] = null;
                continue;
            }

            args[i] = convertComponents[i](component);
        }

        return ((Func<object[], T>)factory)(args!);
    }

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var tuple = (ITuple)value;
        var sb = new StringBuilder("(");
        for (var i = 0; i < tuple.Length; i++)
        {
            if (i > 0) sb.Append(", ");
            var element = tuple[i];
            sb.Append(element is null ? "NULL" : ElementMappings[i].GenerateSqlLiteral(element));
        }
        sb.Append(')');
        return sb.ToString();
    }

    private static string FormatStoreType(IReadOnlyList<RelationalTypeMapping> elementMappings)
        => $"Tuple({string.Join(", ", elementMappings.Select(m => m.StoreType))})";

    private static Type MakeTupleType(Type[] elementTypes, bool useValueTuple)
    {
        if (elementTypes.Length is 0 or > 7)
            throw new NotSupportedException($"Tuples with {elementTypes.Length} elements are not supported. Supported range: 1-7.");

        var genericDef = useValueTuple
            ? elementTypes.Length switch
            {
                1 => typeof(ValueTuple<>),
                2 => typeof(ValueTuple<,>),
                3 => typeof(ValueTuple<,,>),
                4 => typeof(ValueTuple<,,,>),
                5 => typeof(ValueTuple<,,,,>),
                6 => typeof(ValueTuple<,,,,,>),
                7 => typeof(ValueTuple<,,,,,,>),
                _ => throw new NotSupportedException($"Tuples with {elementTypes.Length} elements are not supported.")
            }
            : elementTypes.Length switch
            {
                1 => typeof(Tuple<>),
                2 => typeof(Tuple<,>),
                3 => typeof(Tuple<,,>),
                4 => typeof(Tuple<,,,>),
                5 => typeof(Tuple<,,,,>),
                6 => typeof(Tuple<,,,,,>),
                7 => typeof(Tuple<,,,,,,>),
                _ => throw new NotSupportedException($"Tuples with {elementTypes.Length} elements are not supported.")
            };

        return genericDef.MakeGenericType(elementTypes);
    }
}
