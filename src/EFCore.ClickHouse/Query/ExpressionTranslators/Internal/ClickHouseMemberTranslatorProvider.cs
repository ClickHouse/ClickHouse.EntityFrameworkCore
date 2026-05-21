using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Query.ExpressionTranslators.Internal;

public class ClickHouseMemberTranslatorProvider : RelationalMemberTranslatorProvider
{
    public ClickHouseMemberTranslatorProvider(
        RelationalMemberTranslatorProviderDependencies dependencies,
        IRelationalTypeMappingSource typeMappingSource)
        : base(dependencies)
    {
        var sqlExpressionFactory = dependencies.SqlExpressionFactory;

        AddTranslators(
        [
            new ClickHouseArrayMethodTranslator(sqlExpressionFactory, typeMappingSource),
            new ClickHouseStringMethodTranslator(sqlExpressionFactory),
        ]);
    }
}
