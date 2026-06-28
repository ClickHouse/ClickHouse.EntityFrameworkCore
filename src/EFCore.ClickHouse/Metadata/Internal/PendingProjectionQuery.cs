namespace ClickHouse.EntityFrameworkCore.Metadata.Internal;

/// <summary>
/// The unresolved LINQ definition of a projection, stashed on the parent entity type by the
/// fluent builder and translated at migration-differ time (the model is finalized by then, so a
/// translation DbContext can be built safely — unlike at model-finalizing time). Removed from the
/// model before snapshot generation. Parallels <see cref="PendingMaterializedViewQuery"/>, but a
/// projection always has a single implicit source — its own parent table.
/// </summary>
internal sealed class PendingProjectionQuery
{
    public required Type ParentType { get; init; }
    public required Delegate Lambda { get; init; }

    /// <summary>Source types fed to the translation lambda — for a projection, just the parent.</summary>
    public Type[] SourceTypes => [ParentType];
}
