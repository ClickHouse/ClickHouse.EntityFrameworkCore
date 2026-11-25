using ClickHouse.EntityFrameworkCore.Extensions;
using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Metadata;

namespace ClickHouse.EntityFrameworkCore.Metadata.Builders;

public class ClickHouseVersionedCollapsingMergeTreeEngineBuilder : ClickHouseEngineBuilder
{
    public ClickHouseVersionedCollapsingMergeTreeEngineBuilder(
        IMutableEntityType entityType, string sign, string version)
        : base(entityType, ClickHouseAnnotationNames.VersionedCollapsingMergeTree)
    {
        entityType.SetVersionedCollapsingMergeTreeSign(sign);
        entityType.SetVersionedCollapsingMergeTreeVersion(version);
    }

    public new ClickHouseVersionedCollapsingMergeTreeEngineBuilder WithOrderBy(params string[] columns)
    {
        base.WithOrderBy(columns);
        return this;
    }

    public new ClickHouseVersionedCollapsingMergeTreeEngineBuilder WithPartitionBy(params string[] columns)
    {
        base.WithPartitionBy(columns);
        return this;
    }

    public new ClickHouseVersionedCollapsingMergeTreeEngineBuilder WithPrimaryKey(params string[] columns)
    {
        base.WithPrimaryKey(columns);
        return this;
    }

    public new ClickHouseVersionedCollapsingMergeTreeEngineBuilder WithSampleBy(params string[] columns)
    {
        base.WithSampleBy(columns);
        return this;
    }

    public new ClickHouseVersionedCollapsingMergeTreeEngineBuilder WithTtl(string ttlExpression)
    {
        base.WithTtl(ttlExpression);
        return this;
    }

    public new ClickHouseVersionedCollapsingMergeTreeEngineBuilder WithSetting(string key, string value)
    {
        base.WithSetting(key, value);
        return this;
    }

    /// <summary>
    /// Uses the <c>ReplicatedVersionedCollapsingMergeTree</c> engine with the ZooKeeper path and replica name from the server
    /// settings <c>default_replica_path</c> and <c>default_replica_name</c>.
    /// </summary>
    public ClickHouseVersionedCollapsingMergeTreeEngineBuilder WithReplication()
    {
        SetReplication();
        return this;
    }

    /// <summary>
    /// Uses the <c>ReplicatedVersionedCollapsingMergeTree</c> engine with an explicit ZooKeeper (ClickHouse Keeper) path and replica name,
    /// for example <c>"/clickhouse/tables/{shard}/{database}/{table}"</c> and <c>"{replica}"</c>.
    /// </summary>
    public ClickHouseVersionedCollapsingMergeTreeEngineBuilder WithReplication(string zooKeeperPath, string replicaName)
    {
        SetReplication(zooKeeperPath, replicaName);
        return this;
    }
}
