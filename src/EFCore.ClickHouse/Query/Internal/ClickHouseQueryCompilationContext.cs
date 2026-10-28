#pragma warning disable EF9100

using Microsoft.EntityFrameworkCore.Query;

namespace ClickHouse.EntityFrameworkCore.Query.Internal;

public class ClickHouseQueryCompilationContext : RelationalQueryCompilationContext
{
    public override bool SupportsPrecompiledQuery => true;

    public ClickHouseQueryCompilationContext(
        QueryCompilationContextDependencies dependencies,
        RelationalQueryCompilationContextDependencies relationalDependencies,
        bool async,
        bool precompiling)
        : base(dependencies, relationalDependencies, async, precompiling: precompiling)
    {
    }
}
