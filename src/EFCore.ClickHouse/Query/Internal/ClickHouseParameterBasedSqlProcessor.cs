using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;

namespace ClickHouse.EntityFrameworkCore.Query.Internal;

public class ClickHouseParameterBasedSqlProcessor : RelationalParameterBasedSqlProcessor
{
    public ClickHouseParameterBasedSqlProcessor(
        RelationalParameterBasedSqlProcessorDependencies dependencies,
        RelationalParameterBasedSqlProcessorParameters parameters)
        : base(dependencies, parameters)
    {
    }

    protected override Expression ProcessSqlNullability(
        Expression queryExpression,
        ParametersCacheDecorator decorator)
        => new ClickHouseSqlNullabilityProcessor(Dependencies, Parameters)
            .Process(queryExpression, decorator);
}
