using Microsoft.EntityFrameworkCore.Query;

namespace ClickHouse.EntityFrameworkCore.Query.Internal;

public class ClickHouseQueryCompilationContextFactory : IQueryCompilationContextFactory
{
    private readonly QueryCompilationContextDependencies _dependencies;
    private readonly RelationalQueryCompilationContextDependencies _relationalDependencies;

    public ClickHouseQueryCompilationContextFactory(
        QueryCompilationContextDependencies dependencies,
        RelationalQueryCompilationContextDependencies relationalDependencies)
    {
        _dependencies = dependencies;
        _relationalDependencies = relationalDependencies;
    }

    public QueryCompilationContext Create(bool async)
        => new ClickHouseQueryCompilationContext(_dependencies, _relationalDependencies, async);
}
