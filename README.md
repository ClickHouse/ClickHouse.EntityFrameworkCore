<p align="center">
<img src=".static/logo.svg" width="200px" align="center">
<h1 align="center">ClickHouse Entity Framework Core Provider</h1>
</p>
<br/>
<p align="center">
<a href="https://www.nuget.org/packages/ClickHouse.EntityFrameworkCore">
<img alt="NuGet Version" src="https://img.shields.io/nuget/v/ClickHouse.EntityFrameworkCore">
</a>

<a href="https://www.nuget.org/packages/ClickHouse.EntityFrameworkCore">
<img alt="NuGet Downloads" src="https://img.shields.io/nuget/dt/ClickHouse.EntityFrameworkCore">
</a>

<a href="https://github.com/ClickHouse/ClickHouse.EntityFrameworkCore/actions/workflows/tests.yml">
<img src="https://github.com/ClickHouse/ClickHouse.EntityFrameworkCore/actions/workflows/tests.yml/badge.svg?branch=main">
</a>

<a href="https://codecov.io/gh/ClickHouse/ClickHouse.EntityFrameworkCore">
<img src="https://codecov.io/gh/ClickHouse/ClickHouse.EntityFrameworkCore/graph/badge.svg">
</a>

</p>

The official Entity Framework Core provider for [ClickHouse](https://clickhouse.com/), built on top of [ClickHouse.Driver](https://github.com/ClickHouse/clickhouse-cs).

Detailed documentation is available on the [ClickHouse website](https://clickhouse.com/docs/integrations/csharp#orm-support-ef-core).

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

| Category | ClickHouse Types | CLR Types |
|---|---|---|
| **Integers** | `Int8`/`Int16`/`Int32`/`Int64`, `UInt8`/`UInt16`/`UInt32`/`UInt64` | `sbyte`, `short`, `int`, `long`, `byte`, `ushort`, `uint`, `ulong` |
| **Big integers** | `Int128`, `Int256`, `UInt128`, `UInt256` | `BigInteger` |
| **Floats** | `Float32`, `Float64`, `BFloat16` | `float`, `double` |
| **Decimals** | `Decimal(P,S)`, `Decimal32(S)`, `Decimal64(S)`, `Decimal128(S)`, `Decimal256(S)` | `decimal` or `ClickHouseDecimal` (use `ClickHouseDecimal` for Decimal128/256 to avoid .NET decimal overflow) |
| **Bool** | `Bool` | `bool` |
| **Strings** | `String`, `FixedString(N)` | `string` |
| **Enums** | `Enum8(...)`, `Enum16(...)` | `string` or C# `enum` |
| **Date/time** | `Date`, `Date32`, `DateTime`, `DateTime64(P, 'TZ')` | `DateOnly`, `DateTime` |
| **Time** | `Time`, `Time64(N)` | `TimeSpan` |
| **UUID** | `UUID` | `Guid` |
| **Network** | `IPv4`, `IPv6` | `IPAddress` |
| **Arrays** | `Array(T)` | `T[]` or `List<T>` |
| **Maps** | `Map(K, V)` | `Dictionary<K,V>` |
| **Tuples** | `Tuple(T1, ...)` | `Tuple<...>` or `ValueTuple<...>` |
| **Variant** | `Variant(T1, T2, ...)` | `object` |
| **Dynamic** | `Dynamic` | `object` |
| **JSON** | `Json` | `JsonNode` or `string` |
| **Geographic** | `Point`, `Ring`, `LineString`, `Polygon`, `MultiLineString`, `MultiPolygon`, `Geometry` | `Tuple<double,double>` and arrays thereof; `object` for Geometry |
| **Wrappers** | `Nullable(T)`, `LowCardinality(T)` | Unwrapped automatically |

## Current Status

This provider is in active development. It supports **LINQ queries**, **inserts**, **table engine configuration**, and **migrations** — you can define ClickHouse tables with engine-specific settings, create them via `dotnet ef migrations` or `EnsureCreated`, query with LINQ, and write data via `SaveChanges`.

### LINQ Queries

`Where`, `OrderBy`, `Take`, `Skip`, `Select`, `First`, `Single`, `Any`, `Count`, `Distinct`, `AsNoTracking`

### GROUP BY & Aggregates

`GroupBy` with `Count`, `LongCount`, `Sum`, `Average`, `Min`, `Max` — including `HAVING` (`.Where()` after `.GroupBy()`), multiple aggregates in a single projection, and `OrderBy` on aggregate results.

### String Methods

`Contains`, `StartsWith`, `EndsWith`, `IndexOf`, `Replace`, `Substring`, `Trim`/`TrimStart`/`TrimEnd`, `ToLower`, `ToUpper`, `Length`, `IsNullOrEmpty`, `Concat` (and `+` operator)

### Math Functions

`Math.Abs`, `Floor`, `Ceiling`, `Round`, `Truncate`, `Pow`, `Sqrt`, `Cbrt`, `Exp`, `Log`, `Log2`, `Log10`, `Sign`, `Sin`, `Cos`, `Tan`, `Asin`, `Acos`, `Atan`, `Atan2`, `RadiansToDegrees`, `DegreesToRadians`, `IsNaN`, `IsInfinity`, `IsFinite`, `IsPositiveInfinity`, `IsNegativeInfinity` — with both `Math` and `MathF` overloads.

### INSERT via SaveChanges

`SaveChanges` supports INSERT operations using the driver's native `InsertBinaryAsync` API — RowBinary encoding with GZip compression, far more efficient than parameterized SQL.

```csharp
await using var ctx = new AnalyticsContext();

ctx.PageViews.Add(new PageView
{
    Id = 1,
    Path = "/home",
    Date = new DateOnly(2024, 6, 15),
    UserAgent = "Mozilla/5.0"
});

await ctx.SaveChangesAsync();
```

Entities transition from `Added` to `Unchanged` after save, just like any other EF Core provider.

**Batch size** is configurable (default 1000) — controls how many entities are accumulated before flushing to ClickHouse:

```csharp
optionsBuilder.UseClickHouse("Host=localhost", o => o.MaxBatchSize(5000));
```

### Bulk Insert

For high-throughput loads that don't need change tracking, use `BulkInsertAsync`:

```csharp
var events = Enumerable.Range(0, 100_000)
    .Select(i => new PageView { Id = i, Path = $"/page/{i}", Date = DateOnly.FromDateTime(DateTime.Today) });

long rowsInserted = await ctx.BulkInsertAsync(events);
```

This calls `InsertBinaryAsync` directly, bypassing EF Core's change tracker entirely. Entities are **not** tracked after insert.

### JSON Columns

The provider supports ClickHouse's `Json` column type, mapping to `System.Text.Json.Nodes.JsonNode` or `string`.

```csharp
using System.Text.Json.Nodes;

public class Event
{
    public long Id { get; set; }
    public JsonNode? Payload { get; set; }
}

// In OnModelCreating:
entity.Property(e => e.Payload).HasColumnType("Json");
```

Reading and writing JSON works through both `SaveChanges` and `BulkInsertAsync`:

```csharp
ctx.Events.Add(new Event
{
    Id = 1,
    Payload = JsonNode.Parse("""{"action": "click", "x": 100, "y": 200}""")
});
await ctx.SaveChangesAsync();

var ev = await ctx.Events.Where(e => e.Id == 1).SingleAsync();
string action = ev.Payload!["action"]!.GetValue<string>(); // "click"
```

If you prefer working with raw JSON strings, map the property as `string` with a `Json` column type — the provider will store and retrieve the raw JSON string as-is:

```csharp
public class Event
{
    public long Id { get; set; }
    public string? Payload { get; set; }  // raw JSON string
}

entity.Property(e => e.Payload).HasColumnType("Json");
```

**Limitations:**

- **No JSON path translation** — `entity.Payload["name"]` in LINQ does not translate to ClickHouse's `data.name` SQL syntax. Filter on non-JSON columns or load entities and inspect JSON in memory.
- **No owned entity mapping** — `.ToJson()` / `StructuralJsonTypeMapping` is not supported. JSON columns are opaque `JsonNode` or `string` values.
- **`JsonElement` / `JsonDocument` not supported** — only `JsonNode` and `string` CLR types are mapped.
- **NULL semantics** — ClickHouse's JSON type returns `{}` (empty object) for NULL values rather than SQL NULL. A row inserted with `Data = null` will read back as an empty `JsonNode`, not `null`.
- **Integer precision** — ClickHouse JSON stores all integers as `Int64` unless the path is typed otherwise. When reading via `JsonNode`, use `GetValue<long>()` rather than `GetValue<int>()`.

### Table Engine Configuration

Configure ClickHouse table engines, ordering, partitioning, and more via EF Core's fluent API:

```csharp
modelBuilder.Entity<SensorReading>(b =>
{
    b.HasKey(e => e.Id);
    b.Property(e => e.Temperature).HasCodec("Delta, ZSTD");
    b.Property(e => e.Location).HasColumnComment("Installation site");
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
```

**Supported engines:** `MergeTree`, `ReplacingMergeTree`, `SummingMergeTree`, `AggregatingMergeTree`, `CollapsingMergeTree`, `VersionedCollapsingMergeTree`, `GraphiteMergeTree`, `Log`, `TinyLog`, `StripeLog`, `Memory`

**Column-level DDL:** `.HasCodec("Delta, ZSTD")`, `.HasColumnTtl("expr")`, `.HasColumnComment("text")`

**Data-skipping indices:** `.HasSkippingIndexType("minmax")`, `.HasGranularity(4)`, `.HasSkippingIndexParams("100")`

**Engine settings:** `.WithSetting("index_granularity", "4096")` — any ClickHouse setting as a key-value pair

**Default behavior:** If no engine is configured, the provider defaults to `MergeTree` with the EF primary key as `ORDER BY`.

### Migrations

The provider supports `dotnet ef migrations` for creating and applying migrations:

```bash
dotnet ef migrations add InitialCreate
dotnet ef database update
```

`EnsureCreated()` / `EnsureDeleted()` also work for quick setup without migrations.

**Supported migration operations:**
- CREATE TABLE with full ENGINE clause (all engine types, ORDER BY, PARTITION BY, PRIMARY KEY, SAMPLE BY, TTL, SETTINGS, codecs, comments, data-skipping indices)
- ADD COLUMN, DROP COLUMN, MODIFY COLUMN, RENAME COLUMN, RENAME TABLE
- DROP TABLE, CREATE/DROP INDEX (data-skipping)
- Custom `ClickHouseCreateDatabaseOperation` / `ClickHouseDropDatabaseOperation`

**ClickHouse limitations reflected in migrations:**
- ALTER TABLE cannot change engine, ORDER BY, PARTITION BY, or other structural metadata — the provider throws `NotSupportedException` with a clear message
- Foreign keys, unique constraints, and sequences throw `NotSupportedException`
- Primary key add/drop is a no-op (ClickHouse PK is structural, not a constraint)
- Idempotent scripts (`--idempotent`) are not supported (ClickHouse has no conditional SQL blocks)
- Transactions are suppressed (ClickHouse does not support them)

### Not Yet Implemented

- UPDATE / DELETE (ClickHouse mutations are async, not OLTP-compatible)
- JOINs, subqueries, set operations
- Reverse engineering / scaffolding (`dotnet ef dbcontext scaffold`)
- JSON path query translation

## Building

```bash
dotnet build
dotnet test    # requires Docker (uses Testcontainers)
```

Targets .NET 10.0, EF Core 10.
