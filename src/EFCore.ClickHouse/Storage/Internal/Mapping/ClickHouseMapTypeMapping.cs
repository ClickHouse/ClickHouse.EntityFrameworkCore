using System.Collections;
using System.Data.Common;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

public class ClickHouseMapTypeMapping : RelationalTypeMapping
{
    private static readonly MethodInfo GetValueMethod =
        typeof(DbDataReader).GetRuntimeMethod(nameof(DbDataReader.GetValue), [typeof(int)])!;

    private static readonly MethodInfo ConvertMapMethod =
        typeof(ClickHouseMapTypeMapping).GetMethod(nameof(ConvertMap), BindingFlags.Static | BindingFlags.NonPublic)!;

    public RelationalTypeMapping KeyMapping { get; }
    public RelationalTypeMapping ValueMapping { get; }

    // The CLR types this Map is built from. See ClickHouseNullableElementMapping.ComponentClrType
    // for why the component mapping's own ClrType is not enough.
    private Type KeyComponentClrType => ClickHouseNullableElementMapping.ComponentClrType(KeyMapping);
    private Type ValueComponentClrType => ClickHouseNullableElementMapping.ComponentClrType(ValueMapping);

    public ClickHouseMapTypeMapping(RelationalTypeMapping keyMapping, RelationalTypeMapping valueMapping)
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(Dictionary<,>).MakeGenericType(
                        ClickHouseNullableElementMapping.ComponentClrType(keyMapping),
                        ClickHouseNullableElementMapping.ComponentClrType(valueMapping)),
                    comparer: CreateDictionaryComparer(
                        ClickHouseNullableElementMapping.ComponentClrType(keyMapping),
                        ClickHouseNullableElementMapping.ComponentClrType(valueMapping))),
                $"Map({keyMapping.StoreType}, {valueMapping.StoreType})",
                dbType: System.Data.DbType.Object))
    {
        KeyMapping = keyMapping;
        ValueMapping = valueMapping;
    }

    protected ClickHouseMapTypeMapping(
        RelationalTypeMappingParameters parameters,
        RelationalTypeMapping keyMapping,
        RelationalTypeMapping valueMapping)
        : base(parameters)
    {
        KeyMapping = keyMapping;
        ValueMapping = valueMapping;
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseMapTypeMapping(parameters, KeyMapping, ValueMapping);

    public override MethodInfo GetDataReaderMethod()
        => GetValueMethod;

    public override Expression CustomizeDataReaderExpression(Expression expression)
    {
        // A key or value whose CLR type differs from what the driver produces (DateTimeOffset and
        // DateOnly both arrive as DateTime) needs the dictionary rebuilt entry by entry. Casting
        // the whole dictionary would throw InvalidCastException.
        if (!ClickHouseComponentConversion.NeedsConversion(KeyMapping)
            && !ClickHouseComponentConversion.NeedsConversion(ValueMapping))
        {
            return Expression.Convert(expression, ClrType);
        }

        Expression converted = Expression.Call(
            ConvertMapMethod.MakeGenericMethod(KeyComponentClrType, ValueComponentClrType),
            expression,
            ClickHouseComponentConversion.CreateConverter(KeyMapping, KeyComponentClrType),
            ClickHouseComponentConversion.CreateConverter(ValueMapping, ValueComponentClrType),
            Expression.Constant(
                ClickHouseComponentConversion.CanPassThrough(KeyMapping)
                && ClickHouseComponentConversion.CanPassThrough(ValueMapping)));

        return converted.Type == ClrType ? converted : Expression.Convert(converted, ClrType);
    }

    private static Dictionary<TKey, TValue> ConvertMap<TKey, TValue>(
        object value,
        Func<object, TKey> convertKey,
        Func<object, TValue> convertValue,
        bool canPassThrough)
        where TKey : notnull
    {
        // See ClickHouseComponentConversion.CanPassThrough for when a dictionary the driver already
        // typed needs no rebuilding.
        if (canPassThrough && value is Dictionary<TKey, TValue> alreadyTyped)
            return alreadyTyped;

        var source = (IDictionary)value;
        var result = new Dictionary<TKey, TValue>(source.Count);
        foreach (DictionaryEntry entry in source)
        {
            result[convertKey(entry.Key)] =
                entry.Value is null or DBNull ? default! : convertValue(entry.Value);
        }

        return result;
    }

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var dict = (IDictionary)value;
        var sb = new StringBuilder("map(");
        var first = true;
        foreach (DictionaryEntry entry in dict)
        {
            if (!first) sb.Append(", ");
            sb.Append(KeyMapping.GenerateSqlLiteral(entry.Key));
            sb.Append(", ");
            sb.Append(entry.Value is null ? "NULL" : ValueMapping.GenerateSqlLiteral(entry.Value));
            first = false;
        }
        sb.Append(')');
        return sb.ToString();
    }

    private static ValueComparer CreateDictionaryComparer(Type keyType, Type valueType)
    {
        var method = typeof(ClickHouseMapTypeMapping)
            .GetMethod(nameof(CreateTypedDictionaryComparer), BindingFlags.Static | BindingFlags.NonPublic)!
            .MakeGenericMethod(keyType, valueType);
        return (ValueComparer)method.Invoke(null, null)!;
    }

    private static ValueComparer<Dictionary<TKey, TValue>?> CreateTypedDictionaryComparer<TKey, TValue>()
        where TKey : notnull
        => new(
            (a, b) => (a == null && b == null) || (a != null && b != null && a.Count == b.Count && !a.Except(b).Any()),
            o => o == null ? 0 : o.Aggregate(0, (hash, kvp) => hash ^ HashCode.Combine(kvp.Key, kvp.Value)),
            source => source == null ? null : new Dictionary<TKey, TValue>(source));
}
