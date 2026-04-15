# ClickHouse Entity Framework Core Provider

An Entity Framework Core provider for [ClickHouse](https://clickhouse.com/), built on [ClickHouse.Driver](https://www.nuget.org/packages/ClickHouse.Driver).

## Getting Started

```csharp
await using var ctx = new AnalyticsContext();

var topPages = await ctx.PageViews
    .Where(v => v.Date >= new DateOnly(2024, 1, 1))
    .GroupBy(v => v.Path)
    .Select(g => new { Path = g.Key, Views = g.Count() })
    .OrderByDescending(x => x.Views)
    .Take(10)
    .ToListAsync();

public class AnalyticsContext : DbContext
{
    public DbSet<PageView> PageViews { get; set; }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseClickHouse("Host=localhost;Port=9000;Database=analytics");

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<PageView>(e =>
        {
            e.HasKey(x => x.Id);
            e.ToTable("page_views", t => t
                .HasMergeTreeEngine()
                .WithOrderBy("Date", "Path")
                .WithPartitionBy("toYYYYMM(Date)"));
            e.Property(x => x.UserAgent).HasCodec("ZSTD");
        });
    }
}

public class PageView
{
    public long Id { get; set; }
    public string Path { get; set; }
    public DateOnly Date { get; set; }
    public string UserAgent { get; set; }
}
```

## Supported Types

`String`, `Bool`, `Int8`–`Int64`, `UInt8`–`UInt64`, `Float32`/`Float64`, `Decimal(P,S)` (32/64/128/256), `Date`/`Date32`, `DateTime`, `DateTime64`, `FixedString(N)`, `UUID`, `BFloat16`, `Enum8`/`Enum16`, `IPv4`/`IPv6`, `Int128`/`Int256`/`UInt128`/`UInt256`, `Array(T)`, `Map(K,V)`, `Tuple(T1,...)`, `Time`/`Time64`, `Variant(T1,...,TN)`, `Dynamic`, `Json`, geographic types (Point, Ring, Polygon, MultiPolygon, Geometry).

## Supported LINQ Operations

`Where`, `OrderBy`, `Take`, `Skip`, `Select`, `First`, `Single`, `Any`, `Count`, `LongCount`, `Sum`, `Min`, `Max`, `Average`, `Distinct`, `GroupBy` (with DISTINCT and predicate overloads).

String methods: `Contains`, `StartsWith`, `EndsWith`, `IndexOf`, `Replace`, `Substring`, `Trim`, `ToLower`, `ToUpper`, `Length`, and string concatenation.

60+ Math/MathF translations: Abs, Floor, Ceiling, Round, Truncate, Pow, Sqrt, Exp, Log, trig functions, etc.

## Table Engine Configuration

All MergeTree-family engines (MergeTree, ReplacingMergeTree, SummingMergeTree, AggregatingMergeTree, CollapsingMergeTree, VersionedCollapsingMergeTree, GraphiteMergeTree) and simple engines (Log, TinyLog, StripeLog, Memory) are supported via fluent API with ORDER BY, PARTITION BY, PRIMARY KEY, SAMPLE BY, TTL, and SETTINGS.

Column-level features: CODEC, TTL, COMMENT, DEFAULT values. Data-skipping indexes with configurable type and granularity.

## Migrations

Full `dotnet ef migrations` support: CREATE TABLE with ENGINE clauses, ALTER TABLE (ADD/DROP/MODIFY/RENAME COLUMN), RENAME TABLE, CREATE/DROP DATABASE, data-skipping index management.

## Documentation

For full documentation, see the [GitHub repository](https://github.com/ClickHouse/ClickHouse.EntityFrameworkCore).
