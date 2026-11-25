using ClickHouse.EntityFrameworkCore.Extensions;
using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Metadata;

namespace ClickHouse.EntityFrameworkCore.Metadata.Builders;

public class ClickHouseSummingMergeTreeEngineBuilder : ClickHouseEngineBuilder
{
    public ClickHouseSummingMergeTreeEngineBuilder(IMutableEntityType entityType, params string[] columns)
        : base(entityType, ClickHouseAnnotationNames.SummingMergeTree)
    {
        if (columns.Length > 0)
            entityType.SetSummingMergeTreeColumns(columns);
    }

    public new ClickHouseSummingMergeTreeEngineBuilder WithOrderBy(params string[] columns)
    {
        base.WithOrderBy(columns);
        return this;
    }

    public new ClickHouseSummingMergeTreeEngineBuilder WithPartitionBy(params string[] columns)
    {
        base.WithPartitionBy(columns);
        return this;
    }

    public new ClickHouseSummingMergeTreeEngineBuilder WithPrimaryKey(params string[] columns)
    {
        base.WithPrimaryKey(columns);
        return this;
    }

    public new ClickHouseSummingMergeTreeEngineBuilder WithSampleBy(params string[] columns)
    {
        base.WithSampleBy(columns);
        return this;
    }

    public new ClickHouseSummingMergeTreeEngineBuilder WithTtl(string ttlExpression)
    {
        base.WithTtl(ttlExpression);
        return this;
    }

    public new ClickHouseSummingMergeTreeEngineBuilder WithSetting(string key, string value)
    {
        base.WithSetting(key, value);
        return this;
    }

    /// <summary>
    /// Uses the <c>ReplicatedSummingMergeTree</c> engine with the ZooKeeper path and replica name from the server
    /// settings <c>default_replica_path</c> and <c>default_replica_name</c>.
    /// </summary>
    /// <remarks>
    /// The default path contains <c>{uuid}</c>. ClickHouse accepts it only in an <c>ON CLUSTER</c> query
    /// (see <c>UseCluster</c>) or in a database with the <c>Replicated</c> engine.
    /// </remarks>
    public ClickHouseSummingMergeTreeEngineBuilder WithReplication()
    {
        SetReplication();
        return this;
    }

    /// <summary>
    /// Uses the <c>ReplicatedSummingMergeTree</c> engine with an explicit ZooKeeper (ClickHouse Keeper) path and replica name,
    /// for example <c>"/clickhouse/tables/{shard}/{database}/{table}"</c> and <c>"{replica}"</c>.
    /// </summary>
    public ClickHouseSummingMergeTreeEngineBuilder WithReplication(string zooKeeperPath, string replicaName)
    {
        SetReplication(zooKeeperPath, replicaName);
        return this;
    }
}
