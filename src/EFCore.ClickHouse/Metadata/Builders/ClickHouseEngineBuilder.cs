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

    /// <summary>
    /// Marks the table as replicated. With no arguments, ClickHouse uses the server settings
    /// <c>default_replica_path</c> and <c>default_replica_name</c>.
    /// </summary>
    protected void SetReplication()
    {
        EntityType.SetReplicated(true);
        EntityType.SetReplicatedZooKeeperPath(null);
        EntityType.SetReplicatedReplicaName(null);
    }

    /// <summary>
    /// Marks the table as replicated with an explicit ZooKeeper (ClickHouse Keeper) path and replica name.
    /// Both values can contain macros such as <c>{shard}</c>, <c>{replica}</c>, <c>{database}</c>,
    /// <c>{table}</c> and <c>{uuid}</c>.
    /// </summary>
    protected void SetReplication(string zooKeeperPath, string replicaName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(zooKeeperPath);
        ArgumentException.ThrowIfNullOrWhiteSpace(replicaName);
        EntityType.SetReplicated(true);
        EntityType.SetReplicatedZooKeeperPath(zooKeeperPath);
        EntityType.SetReplicatedReplicaName(replicaName);
    }
}
