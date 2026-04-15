using ClickHouse.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore.Metadata;

namespace ClickHouse.EntityFrameworkCore.Metadata.Builders;

/// <summary>
/// Engine builder for simple engines (TinyLog, StripeLog, Log, Memory) that do not support
/// ORDER BY, PARTITION BY, PRIMARY KEY, SAMPLE BY, TTL, or SETTINGS.
/// </summary>
public class ClickHouseSimpleEngineBuilder : ClickHouseEngineBuilder
{
    public ClickHouseSimpleEngineBuilder(IMutableEntityType entityType, string engineName)
        : base(entityType, engineName)
    {
    }

    public new ClickHouseSimpleEngineBuilder WithOrderBy(params string[] columns)
        => throw new InvalidOperationException(
            $"The '{EntityType.GetEngine()}' engine does not support ORDER BY.");

    public new ClickHouseSimpleEngineBuilder WithPartitionBy(params string[] columns)
        => throw new InvalidOperationException(
            $"The '{EntityType.GetEngine()}' engine does not support PARTITION BY.");

    public new ClickHouseSimpleEngineBuilder WithPrimaryKey(params string[] columns)
        => throw new InvalidOperationException(
            $"The '{EntityType.GetEngine()}' engine does not support PRIMARY KEY.");

    public new ClickHouseSimpleEngineBuilder WithSampleBy(params string[] columns)
        => throw new InvalidOperationException(
            $"The '{EntityType.GetEngine()}' engine does not support SAMPLE BY.");

    public new ClickHouseSimpleEngineBuilder WithTtl(string ttlExpression)
        => throw new InvalidOperationException(
            $"The '{EntityType.GetEngine()}' engine does not support TTL.");

    public new ClickHouseSimpleEngineBuilder WithSetting(string key, string value)
        => throw new InvalidOperationException(
            $"The '{EntityType.GetEngine()}' engine does not support SETTINGS.");
}
