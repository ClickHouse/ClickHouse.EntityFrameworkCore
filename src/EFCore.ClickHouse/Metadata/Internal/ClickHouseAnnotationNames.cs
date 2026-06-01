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

    // Materialized view annotations — stored on IModel as prefix-based per-view
    // annotations of the form "ClickHouse:MaterializedView:{name}:{suffix}".
    public const string MaterializedViewPrefix = Prefix + "MaterializedView:";
    public const string MaterializedViewTargetTypeSuffix = "TargetType";
    public const string MaterializedViewSelectSqlSuffix = "SelectSql";
    public const string MaterializedViewPopulateSuffix = "Populate";
    public const string MaterializedViewClusterSuffix = "Cluster";
    public const string MaterializedViewDatabaseSuffix = "Database";

    // Transient (non-snapshotted) annotation carrying the unresolved LINQ lambda for a view —
    // consumed by ClickHouseMaterializedViewTranslationConvention and removed before snapshot.
    public const string MaterializedViewPendingLambdaSuffix = "PendingLambda";

    // Dictionary annotations — stored on IModel as prefix-based per-dictionary annotations of the
    // form "ClickHouse:Dictionary:{name}:{suffix}" (same pattern as materialized views).
    public const string DictionaryPrefix = Prefix + "Dictionary:";
    public const string DictionaryTypeSuffix = "DictType";      // assembly-qualified CLR type defining the columns
    public const string DictionarySourceTypeSuffix = "SourceType"; // assembly-qualified CLR type of the source table entity
    public const string DictionaryColumnNamesSuffix = "ColumnNames";   // string[] — captured so column changes are diffable
    public const string DictionaryColumnTypesSuffix = "ColumnClrTypes"; // string[] — parallel to ColumnNames
    public const string DictionaryKeySuffix = "Key";            // string[] of key column names
    public const string DictionaryLayoutSuffix = "Layout";      // canonical LAYOUT name (e.g. "HASHED")
    public const string DictionaryLayoutParamsSuffix = "LayoutParams";
    public const string DictionaryLifetimeMinSuffix = "LifetimeMin";
    public const string DictionaryLifetimeMaxSuffix = "LifetimeMax";
    public const string DictionaryClusterSuffix = "Cluster";
    public const string DictionaryDatabaseSuffix = "Database";

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
