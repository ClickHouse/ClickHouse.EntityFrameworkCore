using ClickHouse.EntityFrameworkCore.Extensions;
using ClickHouse.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace EFCore.ClickHouse.DesignSmoke;

public class SmokeDbContext : DbContext
{
    public SmokeDbContext(DbContextOptions<SmokeDbContext> options) : base(options) { }

    public DbSet<SensorReading> SensorReadings => Set<SensorReading>();
    public DbSet<AuditLog> AuditLogs => Set<AuditLog>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<SensorReading>(b =>
        {
            b.HasKey(e => e.Id);

            // Model "V2" adds one extra column, so the CLI tests can scaffold a second,
            // incremental migration whose delta is a single operation (the AddColumn) — the
            // scaffolder path that diffs against an existing model snapshot.
            if (Environment.GetEnvironmentVariable("SMOKE_MODEL_V2") == "1")
                b.Property<string>("Notes");

            // Model "rename" maps the SensorId property to a differently-named column, so the differ
            // infers a column rename — used by the CLI tests that drive the interactive rename prompt.
            b.Property(e => e.SensorId)
                .HasColumnName(Environment.GetEnvironmentVariable("SMOKE_MODEL_RENAME") == "1" ? "DeviceId" : "SensorId");

            b.Property(e => e.Temperature).HasCodec("Delta, ZSTD");
            b.Property(e => e.Timestamp)
                .HasColumnComment("Reading timestamp");
            b.HasIndex(e => e.Timestamp)
                .HasSkippingIndexType("minmax")
                .HasGranularity(4);
            b.ToTable("sensor_readings", t => t
                .HasReplacingMergeTreeEngine("Version")
                .WithOrderBy("Id", "Timestamp")
                .WithPartitionBy("toYYYYMM(Timestamp)")
                .WithPrimaryKey("Id")
                .WithTtl("Timestamp + INTERVAL 1 YEAR")
                .WithSetting("index_granularity", "4096"));
        });

        modelBuilder.Entity<AuditLog>(b =>
        {
            b.HasKey(e => e.Id);
            b.ToTable("audit_logs", t => t.HasMemoryEngine());
        });

        // A materialized view over a source/target pair, so scaffolding must order the table
        // creates ahead of the view create when splitting into step migrations.
        modelBuilder.Entity<HitsSource>(b =>
        {
            b.HasKey(e => e.Id);
            b.ToTable("hits_source", t => t.HasMergeTreeEngine().WithOrderBy("Id"));

            // A table-attached projection on a plain MergeTree table: scaffolding must order the
            // ADD PROJECTION after the table create, and the FROM-less SELECT must round-trip through
            // the snapshot unchanged. (ADD PROJECTION is rejected by ReplacingMergeTree/SummingMergeTree
            // and other dedup/merge engines unless deduplicate_merge_projection_mode is configured.)
            b.HasProjection("proj_by_value")
                .FromRaw("SELECT Value, count() GROUP BY Value");
        });

        modelBuilder.Entity<HitsByHour>(b =>
        {
            b.HasKey(e => e.Bucket);
            b.ToTable("hits_by_hour", t => t.HasSummingMergeTreeEngine("Hits").WithOrderBy("Bucket"));
        });

        modelBuilder.HasMaterializedView<HitsByHour>("hits_mv")
            .FromRaw("SELECT Id AS Bucket, Value AS Hits FROM hits_source");

        // A dictionary over the hits_source table — scaffolding must order it after the table create.
        modelBuilder.HasDictionary<HitsSource>("hits_dict")
            .FromTable<HitsSource>()
            .HasKey(h => h.Id)
            .Layout(ClickHouseDictionaryLayout.Hashed)
            .Lifetime(60);
    }
}

public class SensorReading
{
    public long Id { get; set; }
    public string SensorId { get; set; } = string.Empty;
    public DateTime Timestamp { get; set; }
    public short Temperature { get; set; }
    public ulong Version { get; set; }
}

public class AuditLog
{
    public long Id { get; set; }
    public string Message { get; set; } = string.Empty;
}

public class HitsSource
{
    public long Id { get; set; }
    public long Value { get; set; }
}

public class HitsByHour
{
    public long Bucket { get; set; }
    public long Hits { get; set; }
}

public class SmokeDbContextFactory : IDesignTimeDbContextFactory<SmokeDbContext>
{
    public SmokeDbContext CreateDbContext(string[] args)
    {
        var connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION_STRING")
            ?? "Host=localhost;Database=smoke_test";
        var optionsBuilder = new DbContextOptionsBuilder<SmokeDbContext>();
        optionsBuilder.UseClickHouse(connectionString);
        return new SmokeDbContext(optionsBuilder.Options);
    }
}
