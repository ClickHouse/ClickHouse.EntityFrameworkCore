using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Metadata;

namespace ClickHouse.EntityFrameworkCore.Extensions;

public static class ClickHouseEntityTypeExtensions
{
    // Engine

    public static string? GetEngine(this IReadOnlyEntityType entityType)
        => (string?)entityType[ClickHouseAnnotationNames.Engine];

    public static void SetEngine(this IMutableEntityType entityType, string? engine)
        => entityType.SetOrRemoveAnnotation(ClickHouseAnnotationNames.Engine, engine);

    // ORDER BY

    public static string[]? GetOrderBy(this IReadOnlyEntityType entityType)
        => (string[]?)entityType[ClickHouseAnnotationNames.OrderBy];

    public static void SetOrderBy(this IMutableEntityType entityType, string[]? columns)
        => entityType.SetOrRemoveAnnotation(ClickHouseAnnotationNames.OrderBy,
            columns is { Length: > 0 } ? columns : null);

    // PARTITION BY

    public static string[]? GetPartitionBy(this IReadOnlyEntityType entityType)
        => (string[]?)entityType[ClickHouseAnnotationNames.PartitionBy];

    public static void SetPartitionBy(this IMutableEntityType entityType, string[]? columns)
        => entityType.SetOrRemoveAnnotation(ClickHouseAnnotationNames.PartitionBy,
            columns is { Length: > 0 } ? columns : null);

    // PRIMARY KEY (ClickHouse structural key, distinct from EF's HasKey)

    public static string[]? GetClickHousePrimaryKey(this IReadOnlyEntityType entityType)
        => (string[]?)entityType[ClickHouseAnnotationNames.PrimaryKey];

    public static void SetClickHousePrimaryKey(this IMutableEntityType entityType, string[]? columns)
        => entityType.SetOrRemoveAnnotation(ClickHouseAnnotationNames.PrimaryKey,
            columns is { Length: > 0 } ? columns : null);

    // SAMPLE BY

    public static string[]? GetSampleBy(this IReadOnlyEntityType entityType)
        => (string[]?)entityType[ClickHouseAnnotationNames.SampleBy];

    public static void SetSampleBy(this IMutableEntityType entityType, string[]? columns)
        => entityType.SetOrRemoveAnnotation(ClickHouseAnnotationNames.SampleBy,
            columns is { Length: > 0 } ? columns : null);

    // TTL (table-level)

    public static string? GetTtl(this IReadOnlyEntityType entityType)
        => (string?)entityType[ClickHouseAnnotationNames.Ttl];

    public static void SetTtl(this IMutableEntityType entityType, string? ttlExpression)
        => entityType.SetOrRemoveAnnotation(ClickHouseAnnotationNames.Ttl, ttlExpression);

    // ReplacingMergeTree

    public static string? GetReplacingMergeTreeVersion(this IReadOnlyEntityType entityType)
        => (string?)entityType[ClickHouseAnnotationNames.ReplacingMergeTreeVersion];

    public static void SetReplacingMergeTreeVersion(this IMutableEntityType entityType, string? version)
        => entityType.SetOrRemoveAnnotation(ClickHouseAnnotationNames.ReplacingMergeTreeVersion, version);

    public static string? GetReplacingMergeTreeIsDeleted(this IReadOnlyEntityType entityType)
        => (string?)entityType[ClickHouseAnnotationNames.ReplacingMergeTreeIsDeleted];

    public static void SetReplacingMergeTreeIsDeleted(this IMutableEntityType entityType, string? isDeleted)
        => entityType.SetOrRemoveAnnotation(ClickHouseAnnotationNames.ReplacingMergeTreeIsDeleted, isDeleted);

    // SummingMergeTree

    public static string[]? GetSummingMergeTreeColumns(this IReadOnlyEntityType entityType)
        => (string[]?)entityType[ClickHouseAnnotationNames.SummingMergeTreeColumns];

    public static void SetSummingMergeTreeColumns(this IMutableEntityType entityType, string[]? columns)
        => entityType.SetOrRemoveAnnotation(ClickHouseAnnotationNames.SummingMergeTreeColumns,
            columns is { Length: > 0 } ? columns : null);

    // CollapsingMergeTree

    public static string? GetCollapsingMergeTreeSign(this IReadOnlyEntityType entityType)
        => (string?)entityType[ClickHouseAnnotationNames.CollapsingMergeTreeSign];

    public static void SetCollapsingMergeTreeSign(this IMutableEntityType entityType, string? sign)
        => entityType.SetOrRemoveAnnotation(ClickHouseAnnotationNames.CollapsingMergeTreeSign, sign);

    // VersionedCollapsingMergeTree

    public static string? GetVersionedCollapsingMergeTreeSign(this IReadOnlyEntityType entityType)
        => (string?)entityType[ClickHouseAnnotationNames.VersionedCollapsingMergeTreeSign];

    public static void SetVersionedCollapsingMergeTreeSign(this IMutableEntityType entityType, string? sign)
        => entityType.SetOrRemoveAnnotation(ClickHouseAnnotationNames.VersionedCollapsingMergeTreeSign, sign);

    public static string? GetVersionedCollapsingMergeTreeVersion(this IReadOnlyEntityType entityType)
        => (string?)entityType[ClickHouseAnnotationNames.VersionedCollapsingMergeTreeVersion];

    public static void SetVersionedCollapsingMergeTreeVersion(this IMutableEntityType entityType, string? version)
        => entityType.SetOrRemoveAnnotation(ClickHouseAnnotationNames.VersionedCollapsingMergeTreeVersion, version);

    // GraphiteMergeTree

    public static string? GetGraphiteMergeTreeConfigSection(this IReadOnlyEntityType entityType)
        => (string?)entityType[ClickHouseAnnotationNames.GraphiteMergeTreeConfigSection];

    public static void SetGraphiteMergeTreeConfigSection(this IMutableEntityType entityType, string? configSection)
        => entityType.SetOrRemoveAnnotation(ClickHouseAnnotationNames.GraphiteMergeTreeConfigSection, configSection);

    // Settings (prefix-based key-value)

    public static Dictionary<string, string> GetSettings(this IReadOnlyEntityType entityType)
    {
        var settings = new Dictionary<string, string>();
        foreach (var annotation in entityType.GetAnnotations())
        {
            if (annotation.Name.StartsWith(ClickHouseAnnotationNames.SettingPrefix, StringComparison.Ordinal)
                && annotation.Value is string value)
            {
                var key = annotation.Name[ClickHouseAnnotationNames.SettingPrefix.Length..];
                settings[key] = value;
            }
        }
        return settings;
    }

    public static void SetSetting(this IMutableEntityType entityType, string settingName, string? value)
        => entityType.SetOrRemoveAnnotation(ClickHouseAnnotationNames.SettingPrefix + settingName, value);
}
