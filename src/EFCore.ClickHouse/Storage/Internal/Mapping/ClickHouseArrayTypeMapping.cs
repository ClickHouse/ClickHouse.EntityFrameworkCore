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
                    comparer: CreateArrayComparer(elementMapping.ClrType)),
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
                    comparer: comparer),
                $"Array({elementMapping.StoreType})",
                dbType: System.Data.DbType.Object))
    {
        ElementMapping = elementMapping;
    }

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
}
