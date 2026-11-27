using ClickHouse.EntityFrameworkCore.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore.Query;

namespace ClickHouse.EntityFrameworkCore.Query.Internal;

public class ClickHouseParameterBasedSqlProcessorFactory : IRelationalParameterBasedSqlProcessorFactory
{
    private readonly RelationalParameterBasedSqlProcessorDependencies _dependencies;
    private readonly IClickHouseSingletonOptions _singletonOptions;

    public ClickHouseParameterBasedSqlProcessorFactory(
        RelationalParameterBasedSqlProcessorDependencies dependencies,
        IClickHouseSingletonOptions singletonOptions)
    {
        _dependencies = dependencies;
        _singletonOptions = singletonOptions;
    }

    public RelationalParameterBasedSqlProcessor Create(RelationalParameterBasedSqlProcessorParameters parameters)
        => new ClickHouseParameterBasedSqlProcessor(
            _dependencies, parameters, _singletonOptions.ExplicitParameterizedCollectionMode);
}
