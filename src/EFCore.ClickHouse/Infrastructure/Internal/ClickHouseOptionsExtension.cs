using System.Data.Common;
using ClickHouse.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace ClickHouse.EntityFrameworkCore.Infrastructure.Internal;

public class ClickHouseOptionsExtension : RelationalOptionsExtension
{
    private DbContextOptionsExtensionInfo? _info;

    public DbDataSource? DataSource { get; private set; }

    /// <summary>
    /// When true, the provider will NOT inject <c>set_join_use_nulls=1</c> into connection
    /// strings. Use this when the ClickHouse server/profile forbids changing that setting
    /// (e.g. <c>readonly=1</c> profiles that reject any SET). Disabling means LEFT JOIN
    /// returns column defaults (0, "") for non-matching rows instead of NULL; EF Core's
    /// null-based navigation detection will not work correctly in that mode.
    /// </summary>
    public bool JoinNullSemanticsDisabled { get; private set; }

    /// <summary>
    /// The cluster for <c>ON CLUSTER</c> in the DDL that the provider generates, or <see langword="null"/>
    /// to run DDL only on the server of the connection.
    /// </summary>
    public string? ClusterName { get; private set; }

    /// <summary>
    /// When true, the migrations history table uses <c>ReplicatedMergeTree</c>. The history table is also
    /// replicated when <see cref="ClusterName"/> is set.
    /// </summary>
    public bool ReplicatedMigrationsHistoryTable { get; private set; }

    public ClickHouseOptionsExtension()
    {
    }

    public ClickHouseOptionsExtension(ClickHouseOptionsExtension copyFrom)
        : base(copyFrom)
    {
        DataSource = copyFrom.DataSource;
        JoinNullSemanticsDisabled = copyFrom.JoinNullSemanticsDisabled;
        ClusterName = copyFrom.ClusterName;
        ReplicatedMigrationsHistoryTable = copyFrom.ReplicatedMigrationsHistoryTable;
    }

    protected override RelationalOptionsExtension Clone()
        => new ClickHouseOptionsExtension(this);

    public ClickHouseOptionsExtension WithDataSource(DbDataSource dataSource)
    {
        var clone = (ClickHouseOptionsExtension)Clone();
        clone.DataSource = dataSource;
        return clone;
    }

    public ClickHouseOptionsExtension WithJoinNullSemanticsDisabled(bool disabled)
    {
        var clone = (ClickHouseOptionsExtension)Clone();
        clone.JoinNullSemanticsDisabled = disabled;
        return clone;
    }

    public ClickHouseOptionsExtension WithClusterName(string? clusterName)
    {
        var clone = (ClickHouseOptionsExtension)Clone();
        clone.ClusterName = clusterName;
        return clone;
    }

    public ClickHouseOptionsExtension WithReplicatedMigrationsHistoryTable(bool replicated)
    {
        var clone = (ClickHouseOptionsExtension)Clone();
        clone.ReplicatedMigrationsHistoryTable = replicated;
        return clone;
    }

    public override RelationalOptionsExtension WithConnectionString(string? connectionString)
    {
        var clone = (ClickHouseOptionsExtension)base.WithConnectionString(connectionString);
        clone.DataSource = null;
        return clone;
    }

    public override RelationalOptionsExtension WithConnection(DbConnection? connection, bool contextOwnsConnection = false)
    {
        var clone = (ClickHouseOptionsExtension)base.WithConnection(connection, contextOwnsConnection);
        clone.DataSource = null;
        return clone;
    }

    public override DbContextOptionsExtensionInfo Info
        => _info ??= new ExtensionInfo(this);

    public override void ApplyServices(IServiceCollection services)
        => services.AddEntityFrameworkClickHouse();

    private sealed class ExtensionInfo : RelationalExtensionInfo
    {
        public ExtensionInfo(IDbContextOptionsExtension extension)
            : base(extension)
        {
        }

        public override bool IsDatabaseProvider => true;

        public override string LogFragment => string.Empty;

        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
        {
        }
    }
}
