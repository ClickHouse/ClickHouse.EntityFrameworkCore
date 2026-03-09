using System.Data.Common;
using ClickHouse.EntityFrameworkCore.Infrastructure;
using ClickHouse.EntityFrameworkCore.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace Microsoft.EntityFrameworkCore;

public static class ClickHouseDbContextOptionsBuilderExtensions
{
    public static DbContextOptionsBuilder UseClickHouse(
        this DbContextOptionsBuilder optionsBuilder,
        string? connectionString,
        Action<ClickHouseDbContextOptionsBuilder>? clickHouseOptionsAction = null)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);

        var extension = (ClickHouseOptionsExtension)GetOrCreateExtension(optionsBuilder)
            .WithConnectionString(connectionString);
        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);

        ConfigureWarnings(optionsBuilder);

        clickHouseOptionsAction?.Invoke(new ClickHouseDbContextOptionsBuilder(optionsBuilder));

        return optionsBuilder;
    }

    public static DbContextOptionsBuilder UseClickHouse(
        this DbContextOptionsBuilder optionsBuilder,
        DbConnection connection,
        Action<ClickHouseDbContextOptionsBuilder>? clickHouseOptionsAction = null)
        => UseClickHouse(optionsBuilder, connection, contextOwnsConnection: false, clickHouseOptionsAction);

    public static DbContextOptionsBuilder UseClickHouse(
        this DbContextOptionsBuilder optionsBuilder,
        DbConnection connection,
        bool contextOwnsConnection,
        Action<ClickHouseDbContextOptionsBuilder>? clickHouseOptionsAction = null)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        ArgumentNullException.ThrowIfNull(connection);

        var extension = (ClickHouseOptionsExtension)GetOrCreateExtension(optionsBuilder)
            .WithConnection(connection, contextOwnsConnection);
        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);

        ConfigureWarnings(optionsBuilder);

        clickHouseOptionsAction?.Invoke(new ClickHouseDbContextOptionsBuilder(optionsBuilder));

        return optionsBuilder;
    }

    public static DbContextOptionsBuilder UseClickHouse(
        this DbContextOptionsBuilder optionsBuilder,
        DbDataSource dataSource,
        Action<ClickHouseDbContextOptionsBuilder>? clickHouseOptionsAction = null)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        ArgumentNullException.ThrowIfNull(dataSource);

        var extension = GetOrCreateExtension(optionsBuilder).WithDataSource(dataSource);
        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);

        ConfigureWarnings(optionsBuilder);

        clickHouseOptionsAction?.Invoke(new ClickHouseDbContextOptionsBuilder(optionsBuilder));

        return optionsBuilder;
    }

    public static DbContextOptionsBuilder<TContext> UseClickHouse<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder,
        string? connectionString,
        Action<ClickHouseDbContextOptionsBuilder>? clickHouseOptionsAction = null)
        where TContext : DbContext
        => (DbContextOptionsBuilder<TContext>)UseClickHouse(
            (DbContextOptionsBuilder)optionsBuilder, connectionString, clickHouseOptionsAction);

    private static ClickHouseOptionsExtension GetOrCreateExtension(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.Options.FindExtension<ClickHouseOptionsExtension>() is { } existing
            ? new ClickHouseOptionsExtension(existing)
            : new ClickHouseOptionsExtension();

    private static void ConfigureWarnings(DbContextOptionsBuilder optionsBuilder)
    {
        var coreOptionsExtension = optionsBuilder.Options.FindExtension<CoreOptionsExtension>()
            ?? new CoreOptionsExtension();

        coreOptionsExtension = RelationalOptionsExtension
            .WithDefaultWarningConfiguration(coreOptionsExtension);

        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder)
            .AddOrUpdateExtension(coreOptionsExtension);
    }
}
