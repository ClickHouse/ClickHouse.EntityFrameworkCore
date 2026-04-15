using ClickHouse.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Migrations;

namespace ClickHouse.EntityFrameworkCore.Migrations.Internal;

public class ClickHouseHistoryRepository : HistoryRepository
{
    public ClickHouseHistoryRepository(HistoryRepositoryDependencies dependencies)
        : base(dependencies)
    {
    }

    protected override bool InterpretExistsResult(object? value)
        => value is not null and not DBNull && Convert.ToBoolean(value);

    public override string GetCreateIfNotExistsScript()
    {
        var script = GetCreateScript();
        return script.Insert(
            script.IndexOf("CREATE TABLE", StringComparison.Ordinal) + 12,
            " IF NOT EXISTS");
    }

    public override string GetBeginIfNotExistsScript(string migrationId)
        => throw new NotSupportedException(
            "ClickHouse does not support conditional SQL blocks. " +
            "Idempotent migration scripts (--idempotent) are not supported by this provider.");

    public override string GetBeginIfExistsScript(string migrationId)
        => throw new NotSupportedException(
            "ClickHouse does not support conditional SQL blocks. " +
            "Idempotent migration scripts (--idempotent) are not supported by this provider.");

    public override string GetEndIfScript()
        => throw new NotSupportedException(
            "ClickHouse does not support conditional SQL blocks. " +
            "Idempotent migration scripts (--idempotent) are not supported by this provider.");

    public override IMigrationsDatabaseLock AcquireDatabaseLock()
        => new ClickHouseMigrationDatabaseLock(this);

    public override Task<IMigrationsDatabaseLock> AcquireDatabaseLockAsync(CancellationToken cancellationToken = default)
        => Task.FromResult<IMigrationsDatabaseLock>(new ClickHouseMigrationDatabaseLock(this));

    protected override void ConfigureTable(EntityTypeBuilder<HistoryRow> history)
    {
        history.Property<string>(h => h.MigrationId).HasMaxLength(150);
        history.Property<string>(h => h.ProductVersion).HasMaxLength(32).IsRequired();
        history.ToTable(TableName, table => table
            .HasMergeTreeEngine()
            .WithOrderBy("MigrationId"));
    }

    public override LockReleaseBehavior LockReleaseBehavior => LockReleaseBehavior.Connection;

    protected override string ExistsSql
        => $"EXISTS {SqlGenerationHelper.DelimitIdentifier(TableName, TableSchema)}{SqlGenerationHelper.StatementTerminator}";
}
