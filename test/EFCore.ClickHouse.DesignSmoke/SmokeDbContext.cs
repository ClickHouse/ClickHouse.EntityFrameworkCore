using ClickHouse.EntityFrameworkCore.Extensions;
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
        });

        modelBuilder.Entity<HitsByHour>(b =>
        {
            b.HasKey(e => e.Bucket);
            b.ToTable("hits_by_hour", t => t.HasSummingMergeTreeEngine("Hits").WithOrderBy("Bucket"));
        });

        modelBuilder.HasMaterializedView<HitsByHour>("hits_mv")
            .FromRaw("SELECT Id AS Bucket, Value AS Hits FROM hits_source");
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
