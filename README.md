<p align="center">
<img src=".static/logo.svg" width="200px" align="center">
<h1 align="center">ClickHouse C# client</h1>
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

# ClickHouse Entity Framework Core Provider

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

`String`, `Bool`, `Int8`/`Int16`/`Int32`/`Int64`, `UInt8`/`UInt16`/`UInt32`/`UInt64`, `Float32`/`Float64`, `Decimal(P, S)`, `Date`/`Date32`, `DateTime`, `DateTime64(P, 'TZ')`, `FixedString(N)`, `UUID`

## Current Status

This provider is in early development. It supports **read-only queries** — you can map entities to existing ClickHouse tables and query them with LINQ.

### LINQ Queries

`Where`, `OrderBy`, `Take`, `Skip`, `Select`, `First`, `Single`, `Any`, `Count`, `Distinct`, `AsNoTracking`

### GROUP BY & Aggregates

`GroupBy` with `Count`, `LongCount`, `Sum`, `Average`, `Min`, `Max` — including `HAVING` (`.Where()` after `.GroupBy()`), multiple aggregates in a single projection, and `OrderBy` on aggregate results.

### String Methods

`Contains`, `StartsWith`, `EndsWith`, `IndexOf`, `Replace`, `Substring`, `Trim`/`TrimStart`/`TrimEnd`, `ToLower`, `ToUpper`, `Length`, `IsNullOrEmpty`, `Concat` (and `+` operator)

### Math Functions

`Math.Abs`, `Floor`, `Ceiling`, `Round`, `Truncate`, `Pow`, `Sqrt`, `Cbrt`, `Exp`, `Log`, `Log2`, `Log10`, `Sign`, `Sin`, `Cos`, `Tan`, `Asin`, `Acos`, `Atan`, `Atan2`, `RadiansToDegrees`, `DegreesToRadians`, `IsNaN`, `IsInfinity`, `IsFinite`, `IsPositiveInfinity`, `IsNegativeInfinity` — with both `Math` and `MathF` overloads.

### Not Yet Implemented

- INSERT / UPDATE / DELETE (modification commands are stubbed)
- Migrations
- JOINs, subqueries, set operations
- Advanced types: Array, Tuple, Nullable(T), LowCardinality, Nested, TimeSpan/TimeOnly
- Batched inserts

## Building

```bash
dotnet build
dotnet test    # requires Docker (uses Testcontainers)
```

Targets .NET 10.0, EF Core 10.
