using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal;

public class ClickHouseExecutionStrategy : IExecutionStrategy
{
    private ExecutionStrategyDependencies Dependencies { get; }

    public ClickHouseExecutionStrategy(ExecutionStrategyDependencies dependencies)
    {
        Dependencies = dependencies;
    }

    public bool RetriesOnFailure => false;

    public TResult Execute<TState, TResult>(
        TState state,
        Func<DbContext, TState, TResult> operation,
        Func<DbContext, TState, ExecutionResult<TResult>>? verifySucceeded)
        => operation(Dependencies.CurrentContext.Context, state);

    public async Task<TResult> ExecuteAsync<TState, TResult>(
        TState state,
        Func<DbContext, TState, CancellationToken, Task<TResult>> operation,
        Func<DbContext, TState, CancellationToken, Task<ExecutionResult<TResult>>>? verifySucceeded,
        CancellationToken cancellationToken = default)
        => await operation(Dependencies.CurrentContext.Context, state, cancellationToken).ConfigureAwait(false);
}
