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

## Supported LINQ Operations

`Where`, `OrderBy`, `Take`, `Skip`, `Select`, `First`, `Single`, `Any`, `Count`, `Sum`, `Min`, `Max`, `Average`, `Distinct`, `GroupBy`

String methods: `Contains`, `StartsWith`, `EndsWith`, `IndexOf`, `Replace`, `Substring`, `Trim`, `ToLower`, `ToUpper`, `Length`, and string concatenation.

## Documentation

For full documentation, see the [GitHub repository](https://github.com/ClickHouse/ClickHouse.EntityFrameworkCore).
