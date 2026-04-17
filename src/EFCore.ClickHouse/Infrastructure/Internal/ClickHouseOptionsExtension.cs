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

    public ClickHouseOptionsExtension()
    {
    }

    public ClickHouseOptionsExtension(ClickHouseOptionsExtension copyFrom)
        : base(copyFrom)
    {
        DataSource = copyFrom.DataSource;
        JoinNullSemanticsDisabled = copyFrom.JoinNullSemanticsDisabled;
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
