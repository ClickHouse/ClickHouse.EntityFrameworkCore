using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Metadata;

namespace ClickHouse.EntityFrameworkCore.Extensions;

public static class ClickHousePropertyExtensions
{
    // Codec

    public static string? GetCodec(this IReadOnlyProperty property)
        => (string?)property[ClickHouseAnnotationNames.ColumnCodec];

    public static void SetCodec(this IMutableProperty property, string? codec)
        => property.SetOrRemoveAnnotation(ClickHouseAnnotationNames.ColumnCodec, codec);

    // Column TTL

    public static string? GetColumnTtl(this IReadOnlyProperty property)
        => (string?)property[ClickHouseAnnotationNames.ColumnTtl];

    public static void SetColumnTtl(this IMutableProperty property, string? ttlExpression)
        => property.SetOrRemoveAnnotation(ClickHouseAnnotationNames.ColumnTtl, ttlExpression);

    // Column Comment

    public static string? GetColumnComment(this IReadOnlyProperty property)
        => (string?)property[ClickHouseAnnotationNames.ColumnComment];

    public static void SetColumnComment(this IMutableProperty property, string? comment)
        => property.SetOrRemoveAnnotation(ClickHouseAnnotationNames.ColumnComment, comment);
}
