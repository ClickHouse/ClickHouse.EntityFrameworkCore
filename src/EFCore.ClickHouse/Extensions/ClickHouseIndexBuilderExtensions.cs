using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace ClickHouse.EntityFrameworkCore.Extensions;

public static class ClickHouseIndexBuilderExtensions
{
    public static IndexBuilder HasSkippingIndexType(this IndexBuilder indexBuilder, string type)
    {
        indexBuilder.Metadata.SetSkippingIndexType(type);
        return indexBuilder;
    }

    public static IndexBuilder HasGranularity(this IndexBuilder indexBuilder, int granularity)
    {
        indexBuilder.Metadata.SetGranularity(granularity);
        return indexBuilder;
    }

    public static IndexBuilder HasSkippingIndexParams(this IndexBuilder indexBuilder, string parameters)
    {
        indexBuilder.Metadata.SetSkippingIndexParams(parameters);
        return indexBuilder;
    }
}
