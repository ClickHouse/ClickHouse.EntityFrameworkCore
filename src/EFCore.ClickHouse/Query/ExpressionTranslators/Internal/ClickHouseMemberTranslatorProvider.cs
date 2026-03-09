using Microsoft.EntityFrameworkCore.Query;

namespace ClickHouse.EntityFrameworkCore.Query.ExpressionTranslators.Internal;

public class ClickHouseMemberTranslatorProvider : RelationalMemberTranslatorProvider
{
    public ClickHouseMemberTranslatorProvider(
        RelationalMemberTranslatorProviderDependencies dependencies)
        : base(dependencies)
    {
        var sqlExpressionFactory = dependencies.SqlExpressionFactory;

        AddTranslators(
        [
            new ClickHouseStringMethodTranslator(sqlExpressionFactory),
        ]);
    }
}
