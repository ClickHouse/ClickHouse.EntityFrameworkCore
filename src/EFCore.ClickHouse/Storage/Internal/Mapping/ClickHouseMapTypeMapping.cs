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

    public RelationalTypeMapping KeyMapping { get; }
    public RelationalTypeMapping ValueMapping { get; }

    public ClickHouseMapTypeMapping(RelationalTypeMapping keyMapping, RelationalTypeMapping valueMapping)
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(Dictionary<,>).MakeGenericType(keyMapping.ClrType, valueMapping.ClrType),
                    comparer: CreateDictionaryComparer(keyMapping.ClrType, valueMapping.ClrType)),
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
        => Expression.Convert(expression, ClrType);

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
            (a, b) => a != null && b != null && a.Count == b.Count && !a.Except(b).Any(),
            o => o == null ? 0 : o.Aggregate(0, (hash, kvp) => hash ^ HashCode.Combine(kvp.Key, kvp.Value)),
            source => source == null ? null : new Dictionary<TKey, TValue>(source));
}
