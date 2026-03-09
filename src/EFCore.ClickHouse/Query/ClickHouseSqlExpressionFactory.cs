using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Query;

public class ClickHouseSqlExpressionFactory : SqlExpressionFactory
{
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public ClickHouseSqlExpressionFactory(SqlExpressionFactoryDependencies dependencies)
        : base(dependencies)
    {
        _typeMappingSource = dependencies.TypeMappingSource;
    }

    public SqlExpression ToBool(SqlExpression expression)
        => Convert(expression, typeof(bool), _typeMappingSource.FindMapping(typeof(bool)));
}
