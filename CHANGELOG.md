v0.3.0 (Unreleased)
---
### Advanced queries
* **SimpleJSON functions**: support for `simpleJSONExtract*` and `simpleJSONHas` via `EF.Functions`.

v0.2.0
---
### Table engine and DDL
* **Table engine configuration** via fluent API: MergeTree, ReplacingMergeTree, SummingMergeTree, AggregatingMergeTree, CollapsingMergeTree, VersionedCollapsingMergeTree, GraphiteMergeTree, plus simple engines (Log, TinyLog, StripeLog, Memory).
* **Engine clauses**: ORDER BY, PARTITION BY, PRIMARY KEY, SAMPLE BY, TTL, SETTINGS — all configurable per-entity.
* **Column-level DDL features**: CODEC, TTL, COMMENT, DEFAULT values.
* **Data-skipping indexes**: configurable type, granularity, and parameters.
* **Migrations support**: `dotnet ef migrations add` / `database update` with full DDL generation (CREATE TABLE, ALTER TABLE ADD/DROP/MODIFY/RENAME COLUMN, RENAME TABLE, CREATE/DROP DATABASE).
* **Model validation**: engine parameter columns checked for existence and correct store types (Int8 for sign, UInt8 for isDeleted). Foreign key warnings.
* **Default engine convention**: MergeTree with ORDER BY derived from primary key when no explicit engine is configured.
* Lambda-based overloads for engine configuration (e.g. `HasReplacingMergeTreeEngine<T>(e => e.Version)`).
* `ListToArrayConverter` handles null → empty array for ClickHouse `Array(T)` columns.
* Nullable wrapping correctly skips container types (Array, Map, Tuple, Variant, Dynamic, Json).

### Advanced queries
* **JOINs**: INNER JOIN, LEFT JOIN, and CROSS JOIN (via `SelectMany`). LEFT JOIN now returns real `null` for non-matching rows — the provider injects `set_join_use_nulls=1` automatically on every connection path (`UseClickHouse(string)`, `UseClickHouse(DbConnection)`, `UseClickHouse(DbDataSource)`).
* **Correlated subqueries**: `Contains` / `In`, `Any` / `EXISTS`, `All`, and scalar subqueries in projections.
* **Set operations**: `Concat` (→ `UNION ALL`), `Union` (→ `UNION DISTINCT`), `Intersect`, `Except`, and chained combinations. Bare `UNION` is rejected by ClickHouse, so the provider always emits explicit `ALL` / `DISTINCT`.
* **Inline local collections in queries**: LINQ joins and `Contains` against in-memory collections (`int[]`, `List<T>`, etc.) now translate through a ClickHouse-compatible `SELECT … UNION ALL …` rewrite of EF Core's `ValuesExpression`.
* **Expanded collection property support**: `IEnumerable<T>`, `IList<T>`, `ICollection<T>`, `IReadOnlyList<T>`, and `IReadOnlyCollection<T>` now round-trip end-to-end as entity properties (via the new `EnumerableToArrayConverter<TCollection, T>`), joining the already-supported `T[]` and `List<T>`.
* **`DisableJoinNullSemantics()` option** on `ClickHouseDbContextOptionsBuilder` — opts out of the automatic `join_use_nulls=1` injection for environments where the server forbids changing the setting (e.g. `readonly=1` profiles). LEFT JOIN then returns ClickHouse defaults (0, "") instead of NULL.
* `COUNT(...)` results are cast to `Int32`/`Int64` in SQL so that set operations involving counts find a common supertype (ClickHouse `COUNT` returns `UInt64`).
* Scalar subqueries that project `COUNT` or `SUM` (or EF Core's `COALESCE(COUNT|SUM, 0)` wrap) are automatically wrapped with `ifNull(..., 0)` — ClickHouse scalar subqueries return NULL for empty input even for aggregates that standard SQL guarantees non-null.
* `ORDER BY` on nullable columns emits explicit `NULLS FIRST`/`NULLS LAST` to match .NET/standard-SQL null ordering. Non-nullable columns are left alone so `NaN` ordering on floats isn't disturbed.

v0.1.0
---
Initial preview release.

* **LINQ query translation**: Where, OrderBy, Take, Skip, Select, First, Single, Any, Count, Sum, Min, Max, Average, Distinct, GroupBy (with DISTINCT and predicate overloads), LongCount.
* **60+ Math/MathF method translations**: Abs, Floor, Ceiling, Round, Truncate, Pow, Sqrt, Exp, Log, trig functions, etc.
* **String method translations**: Contains, StartsWith, EndsWith, IndexOf, Replace, Substring, Trim, ToLower, ToUpper, Length.
* **INSERT support**: `SaveChanges()` / `SaveChangesAsync()` via the driver's native `InsertBinaryAsync` (RowBinary with GZip compression). `BulkInsertAsync<T>()` for high-throughput bulk loads. UPDATE/DELETE throw `NotSupportedException`.
* **Type support**: `String`, `Bool`, `Int8`–`Int64`, `UInt8`–`UInt64`, `Float32`/`Float64`, `Decimal(P,S)` (32/64/128/256), `Date`/`Date32`, `DateTime`, `DateTime64`, `FixedString(N)`, `UUID`, `BFloat16`, Nullable(T)/LowCardinality(T) unwrapping, Enum8/Enum16, IPv4/IPv6, BigInteger (Int128/Int256/UInt128/UInt256), Array(T), Map(K,V), Tuple(T1,...), Time/Time64, Variant(T1,...,TN), Dynamic, Json (JsonNode + string), geographic types (Point, Ring, Polygon, MultiPolygon, Geometry).
