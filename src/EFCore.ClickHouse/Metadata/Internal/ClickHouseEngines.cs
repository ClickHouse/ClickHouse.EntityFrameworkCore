namespace ClickHouse.EntityFrameworkCore.Metadata.Internal;

internal static class ClickHouseEngines
{
    public const string ReplicatedPrefix = "Replicated";

    /// <summary>
    /// Returns true for the MergeTree family engines. These engines require ORDER BY and have a
    /// Replicated variant (for example ReplacingMergeTree and ReplicatedReplacingMergeTree).
    /// </summary>
    public static bool IsMergeTreeFamily(string? engine)
        => engine is ClickHouseAnnotationNames.MergeTree
            or ClickHouseAnnotationNames.ReplacingMergeTree
            or ClickHouseAnnotationNames.SummingMergeTree
            or ClickHouseAnnotationNames.AggregatingMergeTree
            or ClickHouseAnnotationNames.CollapsingMergeTree
            or ClickHouseAnnotationNames.VersionedCollapsingMergeTree
            or ClickHouseAnnotationNames.GraphiteMergeTree;
}
