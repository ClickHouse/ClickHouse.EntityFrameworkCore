using System.Data.Common;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

public class ClickHouseVariantTypeMapping : RelationalTypeMapping
{
    private static readonly MethodInfo GetValueMethod =
        typeof(DbDataReader).GetRuntimeMethod(nameof(DbDataReader.GetValue), [typeof(int)])!;

    public IReadOnlyList<RelationalTypeMapping> ElementMappings { get; }

    public ClickHouseVariantTypeMapping(IReadOnlyList<RelationalTypeMapping> elementMappings)
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(object),
                    comparer: new ValueComparer<object?>(
                        (a, b) => Equals(a, b),
                        o => o == null ? 0 : o.GetHashCode(),
                        source => source)),
                FormatStoreType(elementMappings),
                dbType: System.Data.DbType.Object))
    {
        ElementMappings = elementMappings;
    }

    protected ClickHouseVariantTypeMapping(
        RelationalTypeMappingParameters parameters,
        IReadOnlyList<RelationalTypeMapping> elementMappings)
        : base(parameters)
    {
        ElementMappings = elementMappings;
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseVariantTypeMapping(parameters, ElementMappings);

    public override MethodInfo GetDataReaderMethod()
        => GetValueMethod;

    public override Expression CustomizeDataReaderExpression(Expression expression)
        => expression;

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var valueType = value.GetType();

        // Find element mapping matching the value's CLR type
        foreach (var mapping in ElementMappings)
        {
            if (mapping.ClrType == valueType)
                return $"{mapping.GenerateSqlLiteral(value)}::{mapping.StoreType}";
        }

        // Fallback: IsAssignableFrom
        foreach (var mapping in ElementMappings)
        {
            if (mapping.ClrType.IsAssignableFrom(valueType))
                return $"{mapping.GenerateSqlLiteral(value)}::{mapping.StoreType}";
        }

        throw new InvalidOperationException(
            $"No element mapping found for CLR type '{valueType.Name}' in Variant({string.Join(", ", ElementMappings.Select(m => m.StoreType))}).");
    }

    private static string FormatStoreType(IReadOnlyList<RelationalTypeMapping> elementMappings)
        => $"Variant({string.Join(", ", elementMappings.Select(m => m.StoreType))})";
}
