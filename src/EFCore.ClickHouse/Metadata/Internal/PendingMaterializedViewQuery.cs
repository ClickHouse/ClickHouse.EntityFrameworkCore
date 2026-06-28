namespace ClickHouse.EntityFrameworkCore.Metadata.Internal;

/// <summary>
/// The unresolved LINQ definition of a materialized view, stashed on the model
/// by the fluent builder and consumed by <c>ClickHouseMaterializedViewTranslationConvention</c>
/// at model-finalizing time. Removed from the model before snapshot generation.
/// </summary>
internal sealed class PendingMaterializedViewQuery
{
    public required Type TargetType { get; init; }
    public required Type[] SourceTypes { get; init; }
    public required Delegate Lambda { get; init; }
}
