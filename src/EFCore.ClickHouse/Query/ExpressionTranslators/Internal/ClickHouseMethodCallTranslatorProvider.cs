using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Query.ExpressionTranslators.Internal;

public class ClickHouseMethodCallTranslatorProvider : RelationalMethodCallTranslatorProvider
{
    public ClickHouseMethodCallTranslatorProvider(
        RelationalMethodCallTranslatorProviderDependencies dependencies,
        IRelationalTypeMappingSource typeMappingSource)
        : base(dependencies)
    {
        var sqlExpressionFactory = dependencies.SqlExpressionFactory;

        AddTranslators(
        [
            new ClickHouseStringMethodTranslator(sqlExpressionFactory),
            new ClickHouseLikeTranslator(sqlExpressionFactory),
            new ClickHouseMathMethodTranslator(sqlExpressionFactory, typeMappingSource),
            new ClickHouseJsonNodeTranslator(sqlExpressionFactory, typeMappingSource),
        ]);
    }
}
