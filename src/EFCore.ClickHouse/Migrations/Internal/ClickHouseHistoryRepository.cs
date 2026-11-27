using ClickHouse.EntityFrameworkCore.Extensions;
using ClickHouse.EntityFrameworkCore.Infrastructure.Internal;
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
        history.ToTable(TableName, table =>
        {
            var engine = table
                .HasMergeTreeEngine()
                .WithOrderBy("MigrationId");

            // A history table that is not replicated has different rows on each host. Then a migration
            // that ran through one host looks unapplied on the other hosts.
            var options = Dependencies.Options.FindExtension<ClickHouseOptionsExtension>();
            if (options?.ClusterName is not null)
            {
                // ON CLUSTER applies the migrations to all shards, so all hosts share one history table.
                // The default path contains {shard}, which gives each shard its own history.
                // {shard}-{replica} keeps the replica names unique when shards reuse the same replica names.
                engine.WithReplication("/clickhouse/tables/{uuid}", "{shard}-{replica}");
            }
            else if (options?.ReplicatedMigrationsHistoryTable == true)
            {
                // A Replicated database refuses an explicit path and replica name.
                engine.WithReplication();
            }
            else
            {
                return;
            }

            // A replicated table drops an insert that is equal to a recent insert. When a migration is
            // reverted and then applied again, its second history row is equal to the first one.
            engine
                .WithSetting("replicated_deduplication_window", "0")
                .WithSetting("replicated_deduplication_window_for_async_inserts", "0");
        });
    }

    public override LockReleaseBehavior LockReleaseBehavior => LockReleaseBehavior.Connection;

    protected override string ExistsSql
        => $"EXISTS {SqlGenerationHelper.DelimitIdentifier(TableName, TableSchema)}{SqlGenerationHelper.StatementTerminator}";
}
