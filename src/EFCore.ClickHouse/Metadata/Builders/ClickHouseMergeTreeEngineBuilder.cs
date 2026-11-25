using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Metadata;

namespace ClickHouse.EntityFrameworkCore.Metadata.Builders;

public class ClickHouseMergeTreeEngineBuilder : ClickHouseEngineBuilder
{
    public ClickHouseMergeTreeEngineBuilder(IMutableEntityType entityType)
        : base(entityType, ClickHouseAnnotationNames.MergeTree)
    {
    }

    public new ClickHouseMergeTreeEngineBuilder WithOrderBy(params string[] columns)
    {
        base.WithOrderBy(columns);
        return this;
    }

    public new ClickHouseMergeTreeEngineBuilder WithPartitionBy(params string[] columns)
    {
        base.WithPartitionBy(columns);
        return this;
    }

    public new ClickHouseMergeTreeEngineBuilder WithPrimaryKey(params string[] columns)
    {
        base.WithPrimaryKey(columns);
        return this;
    }

    public new ClickHouseMergeTreeEngineBuilder WithSampleBy(params string[] columns)
    {
        base.WithSampleBy(columns);
        return this;
    }

    public new ClickHouseMergeTreeEngineBuilder WithTtl(string ttlExpression)
    {
        base.WithTtl(ttlExpression);
        return this;
    }

    public new ClickHouseMergeTreeEngineBuilder WithSetting(string key, string value)
    {
        base.WithSetting(key, value);
        return this;
    }

    /// <summary>
    /// Uses the <c>ReplicatedMergeTree</c> engine with the ZooKeeper path and replica name from the server
    /// settings <c>default_replica_path</c> and <c>default_replica_name</c>.
    /// </summary>
    public ClickHouseMergeTreeEngineBuilder WithReplication()
    {
        SetReplication();
        return this;
    }

    /// <summary>
    /// Uses the <c>ReplicatedMergeTree</c> engine with an explicit ZooKeeper (ClickHouse Keeper) path and replica name,
    /// for example <c>"/clickhouse/tables/{shard}/{database}/{table}"</c> and <c>"{replica}"</c>.
    /// </summary>
    public ClickHouseMergeTreeEngineBuilder WithReplication(string zooKeeperPath, string replicaName)
    {
        SetReplication(zooKeeperPath, replicaName);
        return this;
    }
}
