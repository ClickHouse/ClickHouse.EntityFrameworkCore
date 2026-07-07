using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;

namespace ClickHouse.EntityFrameworkCore.Metadata.Conventions;

/// <summary>
/// Placeholder convention. LINQ-to-SQL translation of materialized view bodies happens at
/// migration-differ time (see ClickHouseMigrationsModelDiffer) — by then the model is fully
/// finalized and a translation DbContext can be built around it safely. Translating eagerly
/// during model finalization hangs because creating a DbContext from inside the model-build
/// pipeline contends on shared service-provider state.
/// </summary>
public class ClickHouseMaterializedViewTranslationConvention : IModelFinalizingConvention
{
    public void ProcessModelFinalizing(
        IConventionModelBuilder modelBuilder,
        IConventionContext<IConventionModelBuilder> context)
    {
        // No-op. See class docs.
    }
}
