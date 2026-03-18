using System.Data.Common;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

public class ClickHouseDynamicTypeMapping : RelationalTypeMapping
{
    private static readonly MethodInfo GetValueMethod =
        typeof(DbDataReader).GetRuntimeMethod(nameof(DbDataReader.GetValue), [typeof(int)])!;

    private readonly IRelationalTypeMappingSource? _typeMappingSource;

    public ClickHouseDynamicTypeMapping(IRelationalTypeMappingSource? typeMappingSource = null)
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(object),
                    comparer: new ValueComparer<object?>(
                        (a, b) => Equals(a, b),
                        o => o == null ? 0 : o.GetHashCode(),
                        source => source)),
                "Dynamic",
                dbType: System.Data.DbType.Object))
    {
        _typeMappingSource = typeMappingSource;
    }

    protected ClickHouseDynamicTypeMapping(
        RelationalTypeMappingParameters parameters,
        IRelationalTypeMappingSource? typeMappingSource)
        : base(parameters)
    {
        _typeMappingSource = typeMappingSource;
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseDynamicTypeMapping(parameters, _typeMappingSource);

    public override MethodInfo GetDataReaderMethod()
        => GetValueMethod;

    public override Expression CustomizeDataReaderExpression(Expression expression)
        => expression;

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        if (_typeMappingSource is null)
            throw new InvalidOperationException(
                "Cannot generate SQL literal for Dynamic type without a type mapping source.");

        var valueType = value.GetType();
        var mapping = _typeMappingSource.FindMapping(valueType);

        if (mapping is null)
            throw new InvalidOperationException(
                $"Cannot generate SQL literal for Dynamic column: no type mapping found for CLR type '{valueType.Name}'. " +
                "Binary INSERT via SaveChanges works correctly; SQL literal generation is a known limitation for unmapped types.");

        return mapping.GenerateSqlLiteral(value);
    }
}
