using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;

namespace ClickHouse.EntityFrameworkCore.Query.Internal;

public class ClickHouseParameterBasedSqlProcessor : RelationalParameterBasedSqlProcessor
{
    private readonly ParameterTranslationMode? _explicitCollectionParameterTranslationMode;

    public ClickHouseParameterBasedSqlProcessor(
        RelationalParameterBasedSqlProcessorDependencies dependencies,
        RelationalParameterBasedSqlProcessorParameters parameters)
        : this(dependencies, parameters, explicitCollectionParameterTranslationMode: null)
    {
    }

    public ClickHouseParameterBasedSqlProcessor(
        RelationalParameterBasedSqlProcessorDependencies dependencies,
        RelationalParameterBasedSqlProcessorParameters parameters,
        ParameterTranslationMode? explicitCollectionParameterTranslationMode)
        : base(dependencies, parameters)
    {
        _explicitCollectionParameterTranslationMode = explicitCollectionParameterTranslationMode;
    }

    protected override Expression ProcessSqlNullability(
        Expression queryExpression,
        ParametersCacheDecorator decorator)
        => new ClickHouseSqlNullabilityProcessor(Dependencies, Parameters, _explicitCollectionParameterTranslationMode)
            .Process(queryExpression, decorator);
}
