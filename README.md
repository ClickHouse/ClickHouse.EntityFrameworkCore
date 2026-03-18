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
| **Wrappers** | `Nullable(T)`, `LowCardinality(T)` | Unwrapped automatically |

## Current Status

This provider is in early development. It supports **read-only queries** and **inserts** — you can map entities to existing ClickHouse tables, query them with LINQ, and write data via `SaveChanges`.

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

### Not Yet Implemented

- UPDATE / DELETE (ClickHouse mutations are async, not OLTP-compatible)
- Migrations
- JOINs, subqueries, set operations
- Nested type, JSON, Geo types

## Building

```bash
dotnet build
dotnet test    # requires Docker (uses Testcontainers)
```

Targets .NET 10.0, EF Core 10.
