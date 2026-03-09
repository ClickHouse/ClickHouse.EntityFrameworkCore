using System.Data.Common;
using ClickHouse.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace ClickHouse.EntityFrameworkCore.Infrastructure.Internal;

public class ClickHouseOptionsExtension : RelationalOptionsExtension
{
    private DbContextOptionsExtensionInfo? _info;

    public DbDataSource? DataSource { get; private set; }

    public ClickHouseOptionsExtension()
    {
    }

    public ClickHouseOptionsExtension(ClickHouseOptionsExtension copyFrom)
        : base(copyFrom)
    {
        DataSource = copyFrom.DataSource;
    }

    protected override RelationalOptionsExtension Clone()
        => new ClickHouseOptionsExtension(this);

    public ClickHouseOptionsExtension WithDataSource(DbDataSource dataSource)
    {
        var clone = (ClickHouseOptionsExtension)Clone();
        clone.DataSource = dataSource;
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
