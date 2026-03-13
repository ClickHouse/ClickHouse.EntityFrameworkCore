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
        typeof(ClickHouseTupleTypeMapping).GetMethod(nameof(ConvertToValueTuple), BindingFlags.Static | BindingFlags.NonPublic)!;

    // Cache compiled constructors per ValueTuple type to avoid Activator.CreateInstance per row
    private static readonly ConcurrentDictionary<Type, Delegate> ConstructorCache = new();

    public IReadOnlyList<RelationalTypeMapping> ElementMappings { get; }

    public ClickHouseTupleTypeMapping(IReadOnlyList<RelationalTypeMapping> elementMappings, bool useValueTuple = true)
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    MakeTupleType(elementMappings.Select(m => m.ClrType).ToArray(), useValueTuple)),
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
        // The driver returns System.Tuple<>, but C# value tuples are ValueTuple<>.
        // Use a conversion helper that handles both cases.
        if (ClrType.IsValueType)
            return Expression.Call(ConvertMethod.MakeGenericMethod(ClrType), expression);

        return Expression.Convert(expression, ClrType);
    }

    // Converts the driver's Tuple<> to ValueTuple<> (or passes through if already correct type)
    private static T ConvertToValueTuple<T>(object value) where T : struct
    {
        if (value is T t)
            return t;

        // Driver returns System.Tuple<>, need to create ValueTuple<> from its elements
        if (value is ITuple tuple)
        {
            var args = new object?[tuple.Length];
            for (var i = 0; i < tuple.Length; i++)
                args[i] = tuple[i];

            var factory = ConstructorCache.GetOrAdd(typeof(T), static type =>
            {
                var ctorParams = type.GetConstructors()[0].GetParameters();
                var paramExprs = new ParameterExpression[ctorParams.Length];
                var argsParam = Expression.Parameter(typeof(object[]), "args");
                var bodyArgs = new Expression[ctorParams.Length];

                for (var j = 0; j < ctorParams.Length; j++)
                {
                    bodyArgs[j] = Expression.Convert(
                        Expression.ArrayIndex(argsParam, Expression.Constant(j)),
                        ctorParams[j].ParameterType);
                }

                var body = Expression.New(type.GetConstructors()[0], bodyArgs);
                return Expression.Lambda<Func<object[], T>>(body, argsParam).Compile();
            });

            return ((Func<object[], T>)factory)(args!);
        }

        throw new InvalidCastException($"Cannot convert {value.GetType()} to {typeof(T)}");
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
