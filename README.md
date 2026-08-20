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
| **Date/time** | `Date`, `Date32`, `DateTime`, `DateTime64(P, 'TZ')` | `DateOnly`, `DateTime`, `DateTimeOffset` (see [below](#datetimeoffset)) |
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

### DateTimeOffset

A `DateTimeOffset` property maps to `DateTime64(7, 'UTC')` by default:

```csharp
public class Reading
{
    public long Id { get; set; }
    public DateTimeOffset RecordedAt { get; set; }   // DateTime64(7, 'UTC')
}
```

Three things to know:

**The offset is not kept.** ClickHouse has no type that stores a UTC offset. `DateTime64` holds an
instant, and a declared timezone only decides how that instant is rendered. A value written with
any offset is stored as the correct instant, and a value read back carries the offset of the
column's timezone — `+00:00` for the default store type. With that default store type, comparisons
and ordering are instant-correct, so these two values match the same row:

```csharp
// The same instant, written two ways.
var a = new DateTimeOffset(2026, 1, 15, 10, 0, 0, TimeSpan.FromHours(5));
var b = new DateTimeOffset(2026, 1, 15,  5, 0, 0, TimeSpan.Zero);
```

If you must keep the offset, store it yourself in a second column alongside a `DateTime`. To keep
the whole value as text, ask for the conversion explicitly with `HasConversion<string>()` — note
that `HasColumnType("String")` on its own is not enough, because it adds no converter. Such a
column is read-only for now: `SaveChanges` cannot write any property that has a value converter
([#54](https://github.com/ClickHouse/ClickHouse.EntityFrameworkCore/issues/54)).

**Precision 7 makes the round trip exact.** One .NET tick is 100 ns, which is precision 7, so a
stored value never comes back truncated. Precision 7 also covers the full `DateTimeOffset` range,
which lets you use `DateTimeOffset.MinValue` and `MaxValue` as open-ended range limits on the
default store type. Keep those two sentinels to a column whose timezone offset is zero, such as the
default `'UTC'`. Both sit at the edge of the `DateTime` range, and the driver has to build a wall
clock in the column's timezone to return a value, so any non-zero offset pushes one end outside
`DateTime`: reading `MaxValue` from a `DateTime64(7, 'Asia/Tokyo')` column throws. Choose a smaller
precision if you prefer, but be aware that it discards the digits below it:

```csharp
b.Property(e => e.RecordedAt).HasColumnType("DateTime64(3, 'UTC')");   // milliseconds
b.Property(e => e.RecordedAt).HasPrecision(3);                         // the same thing
```

Precision 8 and 9 are accepted, but they hold a narrower range of dates. ClickHouse stores a
`DateTime64(P)` as an `Int64` count of 10^-P seconds, so precision 9 reaches only 1678 to 2262 and
precision 8 about 1970 ± 2900 years. A value outside the range wraps rather than reporting, so the
provider checks it and throws on write instead. Precision 7 has no such limit — it spans roughly
29 000 years, which is why it is the default. Note also that .NET cannot represent more than 7
fractional digits, so the extra precision stores no extra detail.

**Keep `'UTC'` in the store type** unless you have a reason to change it. For a timezone-less type
such as `DateTime64(7)`, the server reads the query parameter in its `session_timezone`, which moves
the instant when that setting is not UTC.

A column that declares a different timezone, for example `DateTime64(6, 'Asia/Tokyo')`, is read
correctly and returns that zone's offset. Two limits apply to such a column:

- The host operating system must know the timezone, or the read throws. Minimal Linux images may
  need the `tzdata` package.
- In a zone with daylight saving, the repeated hour when clocks go back is ambiguous, because the
  driver gives a wall clock and drops the offset. The provider recovers the instant where the zone's
  standard offset is zero, such as `Europe/London`. Where both candidate offsets are non-zero, such
  as `Europe/Paris`, the instant cannot be recovered and the read throws — reporting one of the two
  would move the instant and give the same result for two different ones.
- A value before 1900 in a named zone throws. Before standard time a zone's offset is Local Mean
  Time, which IANA records to the second (`+09:18:59` for `Asia/Tokyo`) while `TimeZoneInfo` may
  round it to the minute, so the instant cannot be reproduced exactly. Store such a value in a
  `'UTC'` column, which has no such offset.
- Dates at the far ends of the `DateTimeOffset` range do not survive, as described above.

A column can also declare a fixed UTC offset instead of a named zone. ClickHouse spells this
`Fixed/UTC±HH:MM:SS`, with two digits in every field — the server rejects `Fixed/UTC+5:30:00` and
`Fixed/UTC+05:30`:

```csharp
b.Property(e => e.RecordedAt).HasColumnType("DateTime64(7, 'Fixed/UTC+05:30:00')");
```

None of the limits above applies here: the host needs no timezone data, a fixed offset is never
ambiguous, and it does not change before 1900. Two points of its own do:

- `DateTimeOffset` holds an offset only within plus or minus 14 hours, and only in whole minutes,
  while ClickHouse accepts more. Such a column still reads correctly — the instant is exact, because
  the offset is known — but the value comes back at offset `+00:00` rather than the column's offset.
  This mapping does not keep the offset in any case.
- ClickHouse does not hold the minutes and seconds fields to 59 — it carries the excess, so
  `Fixed/UTC+05:60:00` is a legal name for `+06:00`. The driver does not read such a name, so the
  read throws rather than depend on that. Declare the offset as `Fixed/UTC+06:00:00` instead.

`DateTimeOffset` also composes into the collection types, so `DateTimeOffset[]`,
`List<DateTimeOffset>`, `Dictionary<string, DateTimeOffset>` and `Tuple<DateTimeOffset, …>` all
round trip.

The standard members and methods — `.Year`, `.DayOfWeek`, `.AddDays(n)` and the rest — translate to
SQL; see [Date/Time Functions](#datetime-functions). `.UtcDateTime`, `.LocalDateTime` and `.Offset`
do not.

## Current Status

This provider is in active development. It supports **LINQ queries**, **inserts**, **table engine configuration**, and **migrations** — you can define ClickHouse tables with engine-specific settings, create them via `dotnet ef migrations` or `EnsureCreated`, query with LINQ, and write data via `SaveChanges`.

### LINQ Queries

`Where`, `OrderBy`, `Take`, `Skip`, `Select`, `First`, `Single`, `Any`, `Count`, `Distinct`, `AsNoTracking`

### Joins

`Join` (INNER JOIN), `GroupJoin` + `SelectMany` + `DefaultIfEmpty` (LEFT JOIN), `SelectMany` (CROSS JOIN), and self-joins. Joining a local sequence (`int[]`, `string[]`, `byte[]`) also works, as does `Contains` against a local collection (`T[]`, `List<T>`, and the `ICollection`/`IList`/`IReadOnlyList` interfaces), which becomes an `IN` predicate.

ClickHouse returns column defaults (`0`, `""`) instead of `NULL` for unmatched LEFT JOIN rows unless `join_use_nulls=1` is set. The provider adds this setting to the connection automatically so LEFT JOIN gives the .NET semantics you expect. To opt out, use `DisableJoinNullSemantics()`:

```csharp
optionsBuilder.UseClickHouse("Host=localhost", o => o.DisableJoinNullSemantics());
```

### Subqueries

`Contains` over an `IQueryable` (`IN (SELECT …)`), `Any` (`EXISTS`), `All`, correlated scalar subqueries in a projection, and subqueries in `FROM`.

ClickHouse returns `NULL` from a scalar subquery that matches no rows, where standard SQL `COUNT` returns `0`. The provider wraps `COUNT` and `SUM` scalar subqueries in `ifNull(…, 0)` when the target CLR type is a non-nullable value type, so a customer with no orders projects `0` rather than throwing.

### Set Operations

`Concat` (`UNION ALL`), `Union` (`UNION DISTINCT`), `Intersect`, and `Except`, including chained and nested combinations. ClickHouse leaves `union_default_mode` empty and rejects a bare `UNION`, so the provider always emits an explicit `ALL` or `DISTINCT` modifier.

### GROUP BY & Aggregates

`GroupBy` with `Count`, `LongCount`, `Sum`, `Average`, `Min`, `Max` — including `HAVING` (`.Where()` after `.GroupBy()`), multiple aggregates in a single projection, and `OrderBy` on aggregate results.

### String Methods

`Contains`, `StartsWith`, `EndsWith`, `IndexOf`, `Replace`, `Substring`, `Trim`/`TrimStart`/`TrimEnd`, `ToLower`, `ToUpper`, `Length`, `IsNullOrEmpty`, `Concat` (and `+` operator)

### Math Functions

`Math.Abs`, `Floor`, `Ceiling`, `Round`, `Truncate`, `Pow`, `Sqrt`, `Cbrt`, `Exp`, `Log`, `Log2`, `Log10`, `Sign`, `Sin`, `Cos`, `Tan`, `Asin`, `Acos`, `Atan`, `Atan2`, `RadiansToDegrees`, `DegreesToRadians`, `IsNaN`, `IsInfinity`, `IsFinite`, `IsPositiveInfinity`, `IsNegativeInfinity` — with both `Math` and `MathF` overloads.

### Date/Time Functions

#### Standard members and methods

The standard .NET date/time members translate to ClickHouse functions, for `DateTime`, `DateTimeOffset` and `DateOnly` alike:

| .NET | ClickHouse |
| --- | --- |
| `.Year` `.Month` `.Day` | `toYear` `toMonth` `toDayOfMonth` |
| `.Hour` `.Minute` `.Second` `.Millisecond` | `toHour` `toMinute` `toSecond` `toMillisecond` |
| `.DayOfYear` | `toDayOfYear` |
| `.DayOfWeek` | `toDayOfWeek(x, 2)` |
| `.Date` | `toStartOfDay` |
| `.TimeOfDay` | `toTime64(x, 7)` |
| `.AddYears(n)` `.AddMonths(n)` | `addYears` `addMonths` |
| `.AddDays(n)` `.AddHours(n)` `.AddMinutes(n)` `.AddSeconds(n)` `.AddMilliseconds(n)` | `addDays` `addHours` … (see below) |
| `DateTime.UtcNow` | `now64(7, 'UTC')` |
| `DateTime.Now` | `now64(7)` |
| `DateTime.Today` | `toStartOfDay(now())` |
| `DateTimeOffset.UtcNow` | `now64(7, 'UTC')` |

```csharp
// Runs entirely on the server
var busyHours = await ctx.Events
    .Where(e => e.Timestamp.Year == 2026 && e.Timestamp.DayOfWeek == DayOfWeek.Sunday)
    .GroupBy(e => e.Timestamp.Hour)
    .Select(g => new { Hour = g.Key, Count = g.Count() })
    .ToListAsync();

var recent = await ctx.Events
    .Where(e => e.Timestamp > DateTime.UtcNow.AddDays(-7))
    .ToListAsync();
```

`DateOnly` gets the date components only, which are the members it declares. For a `DateTimeOffset` property the result is in the timezone the column declares. The default mapping pins that timezone to UTC, while an explicit store type can select a named or fixed-offset timezone; the value read by .NET carries that same declared-zone offset.

Points worth knowing:

**`.DayOfWeek` needs no correction.** ClickHouse week mode 2 agrees with `System.DayOfWeek` exactly — Sunday is 0 through to Saturday 6 — so the value is used as it comes back. The mode argument is always sent, because the default mode starts the week on Monday.

**`DateTime.Now` and `DateTime.Today` read the server clock**, so they follow the *server's* timezone, not the client's, and they come back with `DateTimeKind.Unspecified`. Use `DateTime.UtcNow` when you need an instant that does not depend on server configuration. `DateTimeOffset.UtcNow` also reads a UTC-pinned server clock. `DateTimeOffset.Now` remains client-evaluated in a projection, because its observable local offset cannot be reconstructed from a UTC-pinned server value.

**`.Date` narrows outside 1970–2106.** `toStartOfDay` returns a `DateTime`, and ClickHouse *wraps* a value outside that window instead of reporting it — so `.Date` on a `DateTime64` column holding a pre-1970 date reads back wrong. Enable [`enable_extended_results_for_datetime_functions`](https://clickhouse.com/docs/operations/settings/settings#enable_extended_results_for_datetime_functions) — for example `set_enable_extended_results_for_datetime_functions=1` in the connection string — to get a range-preserving `DateTime64` result.

**A fractional `Add*` argument is exact or is not translated.** `AddDays` and the other time-based methods take a `double`. .NET splits the integral and fractional parts, scales each to *ticks* (100 ns), and truncates any fractional tick toward zero, so `AddSeconds(0.1234567)` adds exactly 1 234 567 ticks. The ClickHouse `addDays` function takes a whole number of days and discards the rest, so it cannot be used directly. A constant argument is folded with the .NET algorithm and then expressed in the coarsest unit that holds it exactly:

```csharp
e.Timestamp.AddDays(1)                // addDays(ts, 1)
e.Timestamp.AddDays(1.5)              // addMilliseconds(ts, 129600000)
e.Timestamp.AddMilliseconds(0.5)      // not translated — 5 000 ticks is below millisecond resolution
e.Timestamp.AddDays(offsetVariable)    // not translated — cannot be checked for exactness
```

The natural function keeps the column's store type, and it is the only form that works on a `Date`/`Date32` column — ClickHouse rejects `addMilliseconds` on those. Anything the provider cannot express exactly is left untranslated rather than rounded to fit, so a projection still gives the correct .NET value through client evaluation, while a predicate reports why. `DateOnly.AddDays` takes an `int`, so it always emits `addDays`.

**`DateTimeOffset.Add*` requires a UTC or fixed-offset column.** .NET preserves the instance's offset during addition. On a named timezone with daylight saving, ClickHouse applies calendar rules instead, so `addDays` across a clock change can advance the instant by 23 or 25 hours and return a different offset. The provider therefore translates these methods for the default `'UTC'` mapping and `Fixed/UTC±HH:MM:SS` mappings only. A named-zone or timezone-less source stays on the client in a projection and reports this limitation in a predicate.

**Arithmetic on two date/time values is not translated.** `dt1 - dt2` and `time1 - time2` give a `TimeSpan`, and `date + timeSpan` mixes types ClickHouse rejects; `dateDiff` returns a count of whole units, and `Time64` subtraction returns a decimal number of seconds. In a projection EF Core reads the columns and does the arithmetic on the client, which gives the correct result. In a predicate there is no client fallback, so the query fails with an explanation.

Not yet translated: `.Ticks`, `.AddTicks`, the `.Microsecond`/`.Nanosecond` members, and `DateTimeOffset.Now`.

#### `toStartOf*` bucketing

The ClickHouse `toStartOf*` family is exposed through `EF.Functions`, so you can bucket and truncate timestamps directly in queries, including in `GROUP BY`:

`ToStartOfYear`, `ToStartOfQuarter`, `ToStartOfMonth`, `ToStartOfWeek` (with an optional ClickHouse week `mode`), `ToStartOfDay`, `ToStartOfHour`, `ToStartOfMinute`, `ToStartOfSecond`, `ToStartOfFiveMinutes`, `ToStartOfTenMinutes`, `ToStartOfFifteenMinutes`, and the general `ToStartOfInterval(source, value, unit)`.

```csharp
// Truncate to the start of the month
var monthly = await ctx.Events
    .Select(e => EF.Functions.ToStartOfMonth(e.Timestamp))
    .ToListAsync();

// Bucket into 15-minute intervals and count per bucket
var buckets = await ctx.Events
    .GroupBy(e => EF.Functions.ToStartOfInterval(e.Timestamp, 15, ClickHouseInterval.Minute))
    .Select(g => new { Bucket = g.Key, Count = g.Count() })
    .ToListAsync();
```

`ToStartOfInterval` takes a `ClickHouseInterval` unit (`Second`, `Minute`, `Hour`, `Day`, `Week`, `Month`, `Quarter`, `Year`) — from the `ClickHouse.EntityFrameworkCore.Metadata` namespace — and emits `toStartOfInterval(source, toInterval<unit>(value))`. The unit must be an inline enum constant. The interval size and optional `ToStartOfWeek` mode may be literals or captured query parameters, but cannot depend on values from the current row.

Input and return types follow ClickHouse. The calendar buckets (`ToStartOfYear`/`Quarter`/`Month`/`Week`) return `Date`; `ToStartOfDay` and the hour/minute buckets return `DateTime`; `ToStartOfSecond` returns `DateTime64`. They all accept `DateTime` and `DateTime64` columns, and the plain truncation functions also accept `DateOnly` (Date/Date32). `ToStartOfInterval` is the exception: older ClickHouse rejects a `DateOnly` (Date/Date32) source with `Illegal type Date32 of 1st argument` while recent versions accept it. Prefer a `DateTime`/`DateTime64` column for interval bucketing.

> **Date range:** those default result types (`Date`, `DateTime`) only span 1970–2149/2106, so ClickHouse **narrows values outside that window** — a pre-1970 date is clamped to the epoch (calendar buckets) or wraps around (sub-day/interval buckets). To preserve the full range, enable [`enable_extended_results_for_datetime_functions`](https://clickhouse.com/docs/operations/settings/settings#enable_extended_results_for_datetime_functions) for your session — e.g. add `set_enable_extended_results_for_datetime_functions=1` to the connection string — which makes ClickHouse return `Date32`/`DateTime64` instead.

`ToStartOfWeek` defaults to ClickHouse week mode `0` (Sunday-based); pass a `mode` to change it.

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

#### Querying JSON paths

Indexing a `JsonNode` property translates to ClickHouse's native dot and subscript syntax, so filters and projections run on the server:

```csharp
// WHERE CAST(`e`.`payload`.`age` AS Int32) > 20
//   AND CAST(`e`.`payload`.`username` AS String) = 'alice_dev'
var users = await ctx.Events
    .Where(e => (int)e.Payload!["age"]! > 20
             && e.Payload!["username"]!.GetValue<string>() == "alice_dev")
    .ToListAsync();
```

Paths nest to any depth (`e.Payload!["meta"]!["runtime"]!["cpu_limit"]!`), and array subscripts work too — the provider converts the 0-based .NET index to ClickHouse's 1-based one, so `["orders"]![0]` emits `.orders[1]`.

Both `.GetValue<T>()` and an explicit cast emit a `CAST(… AS <storeType>)`, with the store type taken from the target CLR type. Keys and array indices must be compile-time constants. A variable key does not translate: in a `Where` it throws, and it can only be client-evaluated in a final `Select`.

#### `simpleJSON*` functions

For JSON held in a **`String`** column (not the native `Json` type), use the `EF.Functions` helpers, which map to ClickHouse's `simpleJSON*` family:

```csharp
var clicks = await ctx.Logs
    .Where(l => EF.Functions.SimpleJsonExtractString(l.RawPayload, "action") == "click")
    .ToListAsync();
```

`SimpleJsonExtractBool`, `SimpleJsonExtractFloat`, `SimpleJsonExtractInt`, `SimpleJsonExtractRaw`, `SimpleJsonExtractString`, `SimpleJsonExtractUInt`, `SimpleJsonHas`.

**Limitations:**

- **No owned entity mapping** — `.ToJson()` / `StructuralJsonTypeMapping` is not supported. JSON columns are opaque `JsonNode` or `string` values.
- **`JsonElement` / `JsonDocument` not supported** — only `JsonNode` and `string` CLR types are mapped.
- **NULL semantics** — ClickHouse's JSON type returns `{}` (empty object) for NULL values rather than SQL NULL. A row inserted with `Data = null` will read back as an empty `JsonNode`, not `null`.
- **Integer precision** — ClickHouse JSON stores all integers as `Int64` unless the path is typed otherwise. When inspecting a materialized `JsonNode` in memory, use `GetValue<long>()` rather than `GetValue<int>()`. This does not apply to a translated path query, where `GetValue<int>()` emits an explicit `CAST(… AS Int32)` that the server applies.

### Table Engine Configuration

Configure ClickHouse table engines, ordering, partitioning, and more via EF Core's fluent API:

```csharp
modelBuilder.Entity<SensorReading>(b =>
{
    b.HasKey(e => e.Id); // becomes ORDER BY (the ClickHouse primary key) when no explicit ORDER BY is set
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

**Primary key vs sorting key:** In ClickHouse the `ORDER BY` (sorting key) *is* the primary key, so `HasKey` alone is sufficient — it becomes `ORDER BY`. Only use `.WithPrimaryKey(...)` when you need the primary index to differ from the sort order (e.g. a `SummingMergeTree`/`AggregatingMergeTree` rollup with a long `ORDER BY` but a narrow index). ClickHouse requires the primary key to be a prefix of the `ORDER BY` columns.

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
- Server-generated values — identity columns, `RETURNING`, computed defaults read back after insert
- Reverse engineering / scaffolding (`dotnet ef dbcontext scaffold`)
- Owned entities mapped to JSON (`.ToJson()`)
- Queries that EF Core lowers to `CROSS APPLY` / `OUTER APPLY`, which ClickHouse has no equivalent for. This covers correlated `SelectMany` that selects the outer element or entity, `Take` inside a collection projection, and correlated collections over a `UNION` source.
- Set operations after a client projection (for example `Union(...).FirstOrDefault()` on a client-evaluated shape)

## Building

```bash
dotnet build
dotnet test    # requires Docker (uses Testcontainers)
```

Targets .NET 10.0, EF Core 10.
