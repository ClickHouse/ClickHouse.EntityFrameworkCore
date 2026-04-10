using ClickHouse.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore.Metadata;

namespace ClickHouse.EntityFrameworkCore.Metadata.Builders;

public abstract class ClickHouseEngineBuilder
{
    protected IMutableEntityType EntityType { get; }

    protected ClickHouseEngineBuilder(IMutableEntityType entityType, string engineName)
    {
        ArgumentNullException.ThrowIfNull(entityType);
        ArgumentException.ThrowIfNullOrWhiteSpace(engineName);

        EntityType = entityType;
        entityType.SetEngine(engineName);
    }

    public ClickHouseEngineBuilder WithOrderBy(params string[] columns)
    {
        ArgumentNullException.ThrowIfNull(columns);
        EntityType.SetOrderBy(columns);
        return this;
    }

    public ClickHouseEngineBuilder WithPartitionBy(params string[] columns)
    {
        ArgumentNullException.ThrowIfNull(columns);
        EntityType.SetPartitionBy(columns);
        return this;
    }

    public ClickHouseEngineBuilder WithPrimaryKey(params string[] columns)
    {
        ArgumentNullException.ThrowIfNull(columns);
        EntityType.SetClickHousePrimaryKey(columns);
        return this;
    }

    public ClickHouseEngineBuilder WithSampleBy(params string[] columns)
    {
        ArgumentNullException.ThrowIfNull(columns);
        EntityType.SetSampleBy(columns);
        return this;
    }

    public ClickHouseEngineBuilder WithTtl(string ttlExpression)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(ttlExpression);
        EntityType.SetTtl(ttlExpression);
        return this;
    }

    public ClickHouseEngineBuilder WithSetting(string key, string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentNullException.ThrowIfNull(value);
        EntityType.SetSetting(key, value);
        return this;
    }
}
