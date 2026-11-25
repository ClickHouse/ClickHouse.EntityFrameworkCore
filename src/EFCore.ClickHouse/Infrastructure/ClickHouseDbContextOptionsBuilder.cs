using ClickHouse.EntityFrameworkCore.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace ClickHouse.EntityFrameworkCore.Infrastructure;

public class ClickHouseDbContextOptionsBuilder
    : RelationalDbContextOptionsBuilder<ClickHouseDbContextOptionsBuilder, ClickHouseOptionsExtension>
{
    public ClickHouseDbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder)
        : base(optionsBuilder)
    {
    }

    /// <summary>
    /// Disables automatic injection of <c>set_join_use_nulls=1</c> into connection strings.
    /// Use this when the ClickHouse server or user profile forbids changing that setting
    /// (e.g. <c>readonly=1</c> profiles). With auto-injection disabled, LEFT JOINs return
    /// column defaults (0, "") for non-matching rows rather than NULL, and EF Core's
    /// null-based navigation detection will not work as expected.
    /// </summary>
    public virtual ClickHouseDbContextOptionsBuilder DisableJoinNullSemantics()
        => WithOption(e => e.WithJoinNullSemanticsDisabled(true));

    /// <summary>
    /// Adds <c>ON CLUSTER '<paramref name="clusterName"/>'</c> to the DDL that migrations,
    /// <c>EnsureCreated()</c> and <c>EnsureDeleted()</c> generate, so that ClickHouse runs each statement
    /// on all hosts of the cluster. The migrations history table is also created with
    /// <c>ReplicatedMergeTree</c>, so that all replicas see the same applied migrations.
    /// </summary>
    /// <remarks>
    /// <c>ON CLUSTER</c> only creates a table on each host. To replicate the rows of a table between the
    /// replicas of a shard, configure a Replicated engine with <c>WithReplication()</c> on the table.
    /// Do not use this option in a database with the <c>Replicated</c> engine: ClickHouse replicates
    /// DDL in such a database and refuses <c>ON CLUSTER</c>.
    /// Use <see cref="UseReplicatedMigrationsHistoryTable"/> there.
    /// </remarks>
    /// <param name="clusterName">A cluster from the <c>remote_servers</c> server setting. Macros such as <c>{cluster}</c> are allowed.</param>
    public virtual ClickHouseDbContextOptionsBuilder UseCluster(string clusterName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterName);
        return WithOption(e => e.WithClusterName(clusterName));
    }

    /// <summary>
    /// Creates the migrations history table with <c>ReplicatedMergeTree</c> and the server settings
    /// <c>default_replica_path</c> and <c>default_replica_name</c>, so that all replicas see the same applied
    /// migrations. Use this in a database with the <c>Replicated</c> engine. <see cref="UseCluster"/> does this too.
    /// </summary>
    public virtual ClickHouseDbContextOptionsBuilder UseReplicatedMigrationsHistoryTable()
        => WithOption(e => e.WithReplicatedMigrationsHistoryTable(true));
}
