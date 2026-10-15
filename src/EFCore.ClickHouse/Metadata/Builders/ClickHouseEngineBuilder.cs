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

    /// <summary>
    /// Sets the table's sorting key (<c>ORDER BY</c>). In ClickHouse the sorting key also serves as the
    /// primary key unless an explicit one is set via <see cref="WithPrimaryKey"/>.
    /// </summary>
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

    /// <summary>
    /// Sets an explicit primary key (<c>PRIMARY KEY</c>) distinct from the sorting key. Only needed when the
    /// primary index should differ from <c>ORDER BY</c>; otherwise the sorting key is used as the primary key.
    /// ClickHouse requires these columns to be a prefix of the <see cref="WithOrderBy"/> columns.
    /// </summary>
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
