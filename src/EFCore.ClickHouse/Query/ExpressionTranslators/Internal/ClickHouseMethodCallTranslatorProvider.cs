using Microsoft.EntityFrameworkCore.Query;

namespace ClickHouse.EntityFrameworkCore.Query.ExpressionTranslators.Internal;

public class ClickHouseMethodCallTranslatorProvider : RelationalMethodCallTranslatorProvider
{
    public ClickHouseMethodCallTranslatorProvider(
        RelationalMethodCallTranslatorProviderDependencies dependencies)
        : base(dependencies)
    {
        var sqlExpressionFactory = dependencies.SqlExpressionFactory;

        AddTranslators(
        [
            new ClickHouseStringMethodTranslator(sqlExpressionFactory),
            new ClickHouseLikeTranslator(sqlExpressionFactory),
        ]);
    }
}
