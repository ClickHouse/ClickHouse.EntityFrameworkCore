using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal;

public class ClickHouseExecutionStrategyFactory : RelationalExecutionStrategyFactory
{
    public ClickHouseExecutionStrategyFactory(ExecutionStrategyDependencies dependencies)
        : base(dependencies)
    {
    }

    protected override IExecutionStrategy CreateDefaultStrategy(ExecutionStrategyDependencies dependencies)
        => new ClickHouseExecutionStrategy(dependencies);
}
