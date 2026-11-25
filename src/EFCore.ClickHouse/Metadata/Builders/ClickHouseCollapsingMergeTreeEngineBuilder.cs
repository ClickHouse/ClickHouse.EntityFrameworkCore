using ClickHouse.EntityFrameworkCore.Extensions;
using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Metadata;

namespace ClickHouse.EntityFrameworkCore.Metadata.Builders;

public class ClickHouseCollapsingMergeTreeEngineBuilder : ClickHouseEngineBuilder
{
    public ClickHouseCollapsingMergeTreeEngineBuilder(IMutableEntityType entityType, string sign)
        : base(entityType, ClickHouseAnnotationNames.CollapsingMergeTree)
    {
        entityType.SetCollapsingMergeTreeSign(sign);
    }

    public new ClickHouseCollapsingMergeTreeEngineBuilder WithOrderBy(params string[] columns)
    {
        base.WithOrderBy(columns);
        return this;
    }

    public new ClickHouseCollapsingMergeTreeEngineBuilder WithPartitionBy(params string[] columns)
    {
        base.WithPartitionBy(columns);
        return this;
    }

    public new ClickHouseCollapsingMergeTreeEngineBuilder WithPrimaryKey(params string[] columns)
    {
        base.WithPrimaryKey(columns);
        return this;
    }

    public new ClickHouseCollapsingMergeTreeEngineBuilder WithSampleBy(params string[] columns)
    {
        base.WithSampleBy(columns);
        return this;
    }

    public new ClickHouseCollapsingMergeTreeEngineBuilder WithTtl(string ttlExpression)
    {
        base.WithTtl(ttlExpression);
        return this;
    }

    public new ClickHouseCollapsingMergeTreeEngineBuilder WithSetting(string key, string value)
    {
        base.WithSetting(key, value);
        return this;
    }

    /// <summary>
    /// Uses the <c>ReplicatedCollapsingMergeTree</c> engine with the ZooKeeper path and replica name from the server
    /// settings <c>default_replica_path</c> and <c>default_replica_name</c>.
    /// </summary>
    public ClickHouseCollapsingMergeTreeEngineBuilder WithReplication()
    {
        SetReplication();
        return this;
    }

    /// <summary>
    /// Uses the <c>ReplicatedCollapsingMergeTree</c> engine with an explicit ZooKeeper (ClickHouse Keeper) path and replica name,
    /// for example <c>"/clickhouse/tables/{shard}/{database}/{table}"</c> and <c>"{replica}"</c>.
    /// </summary>
    public ClickHouseCollapsingMergeTreeEngineBuilder WithReplication(string zooKeeperPath, string replicaName)
    {
        SetReplication(zooKeeperPath, replicaName);
        return this;
    }
}
