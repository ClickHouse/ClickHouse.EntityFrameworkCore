namespace ClickHouse.EntityFrameworkCore.Metadata.Internal;

public static class ClickHouseAnnotationNames
{
    public const string Prefix = "ClickHouse:";

    // Table-level engine configuration
    public const string Engine = Prefix + "Engine";
    public const string OrderBy = Prefix + "OrderBy";
    public const string PartitionBy = Prefix + "PartitionBy";
    public const string PrimaryKey = Prefix + "PrimaryKey";
    public const string SampleBy = Prefix + "SampleBy";
    public const string Ttl = Prefix + "Ttl";

    // Engine-specific parameters
    public const string ReplacingMergeTreeVersion = Prefix + "ReplacingMergeTree:Version";
    public const string ReplacingMergeTreeIsDeleted = Prefix + "ReplacingMergeTree:IsDeleted";
    public const string SummingMergeTreeColumns = Prefix + "SummingMergeTree:Columns";
    public const string CollapsingMergeTreeSign = Prefix + "CollapsingMergeTree:Sign";
    public const string VersionedCollapsingMergeTreeSign = Prefix + "VersionedCollapsingMergeTree:Sign";
    public const string VersionedCollapsingMergeTreeVersion = Prefix + "VersionedCollapsingMergeTree:Version";
    public const string GraphiteMergeTreeConfigSection = Prefix + "GraphiteMergeTree:ConfigSection";

    // Settings (prefix-based key-value storage)
    public const string SettingPrefix = Prefix + "Setting:";

    // Column-level annotations
    public const string ColumnCodec = Prefix + "ColumnCodec";
    public const string ColumnTtl = Prefix + "ColumnTtl";
    public const string ColumnComment = Prefix + "ColumnComment";

    // Data-skipping index annotations
    public const string SkippingIndexType = Prefix + "SkippingIndex:Type";
    public const string SkippingIndexGranularity = Prefix + "SkippingIndex:Granularity";
    public const string SkippingIndexParams = Prefix + "SkippingIndex:Params";

    // Engine name constants
    public const string MergeTree = "MergeTree";
    public const string ReplacingMergeTree = "ReplacingMergeTree";
    public const string SummingMergeTree = "SummingMergeTree";
    public const string AggregatingMergeTree = "AggregatingMergeTree";
    public const string CollapsingMergeTree = "CollapsingMergeTree";
    public const string VersionedCollapsingMergeTree = "VersionedCollapsingMergeTree";
    public const string GraphiteMergeTree = "GraphiteMergeTree";
    public const string TinyLog = "TinyLog";
    public const string StripeLog = "StripeLog";
    public const string Log = "Log";
    public const string Memory = "Memory";
}
