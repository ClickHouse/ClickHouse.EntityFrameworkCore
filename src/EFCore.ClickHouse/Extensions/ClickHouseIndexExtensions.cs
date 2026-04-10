using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Metadata;

namespace ClickHouse.EntityFrameworkCore.Extensions;

public static class ClickHouseIndexExtensions
{
    // Skipping index type (minmax, set, bloom_filter, etc.)

    public static string? GetSkippingIndexType(this IReadOnlyIndex index)
        => (string?)index[ClickHouseAnnotationNames.SkippingIndexType];

    public static void SetSkippingIndexType(this IMutableIndex index, string? type)
        => index.SetOrRemoveAnnotation(ClickHouseAnnotationNames.SkippingIndexType, type);

    // Granularity

    public static int? GetGranularity(this IReadOnlyIndex index)
        => (int?)index[ClickHouseAnnotationNames.SkippingIndexGranularity];

    public static void SetGranularity(this IMutableIndex index, int? granularity)
        => index.SetOrRemoveAnnotation(ClickHouseAnnotationNames.SkippingIndexGranularity, granularity);

    // Skipping index params (e.g., "100" for set(100), "0.01" for bloom_filter(0.01))

    public static string? GetSkippingIndexParams(this IReadOnlyIndex index)
        => (string?)index[ClickHouseAnnotationNames.SkippingIndexParams];

    public static void SetSkippingIndexParams(this IMutableIndex index, string? parameters)
        => index.SetOrRemoveAnnotation(ClickHouseAnnotationNames.SkippingIndexParams, parameters);
}
