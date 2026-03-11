using Microsoft.EntityFrameworkCore.Query;

namespace ClickHouse.EntityFrameworkCore.Query.ExpressionTranslators.Internal;

public class ClickHouseAggregateMethodCallTranslatorProvider : RelationalAggregateMethodCallTranslatorProvider
{
    public ClickHouseAggregateMethodCallTranslatorProvider(
        RelationalAggregateMethodCallTranslatorProviderDependencies dependencies)
        : base(dependencies)
    {
        var sqlExpressionFactory = dependencies.SqlExpressionFactory;

        AddTranslators(
        [
            new ClickHouseQueryableAggregateMethodTranslator(sqlExpressionFactory),
        ]);
    }
}
