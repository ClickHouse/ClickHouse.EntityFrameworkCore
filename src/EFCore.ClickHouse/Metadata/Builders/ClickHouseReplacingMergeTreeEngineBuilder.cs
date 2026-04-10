using ClickHouse.EntityFrameworkCore.Extensions;
using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Metadata;

namespace ClickHouse.EntityFrameworkCore.Metadata.Builders;

public class ClickHouseReplacingMergeTreeEngineBuilder : ClickHouseEngineBuilder
{
    public ClickHouseReplacingMergeTreeEngineBuilder(
        IMutableEntityType entityType, string? version = null, string? isDeleted = null)
        : base(entityType, ClickHouseAnnotationNames.ReplacingMergeTree)
    {
        if (version is not null)
            entityType.SetReplacingMergeTreeVersion(version);
        if (isDeleted is not null)
            entityType.SetReplacingMergeTreeIsDeleted(isDeleted);
    }

    public new ClickHouseReplacingMergeTreeEngineBuilder WithOrderBy(params string[] columns)
    {
        base.WithOrderBy(columns);
        return this;
    }

    public new ClickHouseReplacingMergeTreeEngineBuilder WithPartitionBy(params string[] columns)
    {
        base.WithPartitionBy(columns);
        return this;
    }

    public new ClickHouseReplacingMergeTreeEngineBuilder WithPrimaryKey(params string[] columns)
    {
        base.WithPrimaryKey(columns);
        return this;
    }

    public new ClickHouseReplacingMergeTreeEngineBuilder WithSampleBy(params string[] columns)
    {
        base.WithSampleBy(columns);
        return this;
    }

    public new ClickHouseReplacingMergeTreeEngineBuilder WithTtl(string ttlExpression)
    {
        base.WithTtl(ttlExpression);
        return this;
    }

    public new ClickHouseReplacingMergeTreeEngineBuilder WithSetting(string key, string value)
    {
        base.WithSetting(key, value);
        return this;
    }
}
