v0.4.0 (Unreleased)
---
### Advanced queries
* **Parameterized collection `Contains` binds a single native `Array(T)` parameter.** A captured collection used with `Contains` (`ids.Contains(x.Id)`) now translates to `has({ids:Array(T)}, column)` — one bound array parameter — instead of EF Core's default one-scalar-parameter-per-element `IN (…)` expansion. This removes the parameter-count / query-size ceilings that large `IN` lists hit and keeps the query text (and plan-cache key) independent of the collection size. The array's element store type is aligned to the tested column (`Array(Int64)`, `Array(String)`, `Array(UUID)`, …). Per-query overrides are honored via the standard markers: `EF.MultipleParameters(…)` keeps the one-parameter-per-element expansion and `EF.Constant(…)` inlines the values as literals. The array-parameter path is deliberately scoped; these cases fall back to the standard per-element expansion (`IN (…)`) instead: a **nullable** tested column (so `NOT`-Contains null semantics match `x NOT IN (…)`), element types that need a **value conversion** (e.g. a CLR enum → `Enum8`), element CLR types the driver doesn't serialize natively inside an array (**temporal types** — `DateTime`/`DateTimeOffset`/`DateOnly`/`TimeOnly`/`TimeSpan`), and collections used as a **queryable source** (joins, `Where(…).Contains(…)`, which keep the `SELECT … UNION ALL …` rewrite). The natively-serialized element types are the integers, floating point, `decimal`, `bool`, `string`, and `Guid`. The collection is bound as a single parameter value, so very large lists can hit the server's `http_max_field_value_size` (default 128 KiB); raise that setting for extreme cases. ([#39](https://github.com/ClickHouse/ClickHouse.EntityFrameworkCore/issues/39))

### Bug fixes
* `Sum`/`SumAsync` over a `double` or `float` column no longer throws `InvalidCastException`. EF Core wraps a top-level aggregate so the empty case returns `0`, supplying that fallback as a boxed `Int32` carrying the `Float64`/`Float32` mapping; the literal generators now convert rather than unbox. The `Float32` read path also converts, since ClickHouse widens `sum(Float32)` to `Float64` (which the driver's `GetFloat()` refuses to downcast). ([#46](https://github.com/ClickHouse/ClickHouse.EntityFrameworkCore/issues/46))

v0.3.0
---
### Advanced queries
* **Native JSON navigation**: support for `JsonNode` indexing (`Data["key"]`, `Data[index]`) and member access.
  The provider handles ClickHouse 1-based indexing for arrays automatically and supports deep nesting and explicit casting/`.GetValue<T>()`.
* **SimpleJSON functions**: support for `simpleJSONExtract*` and `simpleJSONHas` via `EF.Functions`.
* **Array-column helpers on mapped `Array(T)` columns** translate to native ClickHouse array functions via the new `ClickHouseArrayMethodTranslator`. Search items are aligned to the column's element type mapping (Int64 vs Int32, FixedString(N) vs String, Enum8 vs String, …) so parameters serialize with the correct ClickHouse store type. Only mapped array columns are routed through these translations; LINQ over local in-memory collections continues to flow through the inline-`SELECT … UNION ALL …` path.
  * `Enumerable.Contains` / `Queryable.Contains` / instance `List<T>.Contains` (plus the `ICollection<>`/`IList<>`/`IReadOnlyList<>`/`IReadOnlyCollection<>` interfaces) → `has(array, value)`.
  * `Array.Length` / `List<T>.Count` member access / `Enumerable.Count()` / `Queryable.Count()` → `length(array)` (`int`); `LongCount()` → `length(array)` (`long`).
  * `Enumerable.Any()` / `Queryable.Any()` → `notEmpty(array)`. Note that EF Core normalizes `arr.Count > 0` / `arr.Count() > 0` (and similar) into `Any()` *before* the provider sees them, so those expressions emit `notEmpty(...)` rather than `length(...) > 0`. Use `Count == N` (with `N > 0`) when you want `length(...)` for a row-shape assertion.
  * `Any(x => f(x))` / `Count(x => f(x))` / `LongCount(x => f(x))` predicate overloads → `arrayExists(x -> f(x), array)` / `arrayCount(x -> f(x), array)`. The predicate body is translated through the existing scalar translator chain, just like `Select(...).Contains(...)`.
  * `arr.Select(x => f(x)).Contains(value)` → `has(arrayMap(x -> f(x), array), value)`. The inner lambda body is translated through the existing scalar translator chain, so e.g. `x => x.ToLower()` becomes `lowerUTF8(x)` (the shape Gridify's `CaseInsensitiveFiltering` produces for string-array properties).
  * **Element access** — `First()` / `FirstOrDefault()` → `arrayElement(array, 1)`; `Last()` / `LastOrDefault()` → `arrayElement(array, -1)` (ClickHouse negative-index from-end addressing); `ElementAt(i)` / `ElementAtOrDefault(i)` → `arrayElement(array, i + 1)`. The +1 offset is widened to Int64 server-side so `int.MaxValue` doesn't wrap. Divergences from .NET LINQ: empty / out-of-range positive indices return the element type's default (`0`, `""`, …) rather than throwing; LINQ index `-1` becomes ClickHouse index `0` which is also OOB → default; LINQ indices `≤ -2` map to ClickHouse from-end addressing (`ElementAt(-2)` on `[1,2,3]` returns `3`), again rather than yielding default.
  * **Slicing** — `Skip(n)` → `arraySlice(array, n + 1)`; `Take(m)` → `arraySlice(array, 1, m)`. Both produce mapped `Array(T)` results, so chained downstream helpers (`arr.Skip(1).Contains(x)`, `arr.Take(2).Count()`, `arr.Skip(n).Take(m).FirstOrDefault()`) compose without further configuration.
  * **Ordering / reversal / deduplication** — `Reverse()` → `arrayReverse(array)`; `Distinct()` → `arrayDistinct(array)`; `OrderBy(x => x)` / `OrderByDescending(x => x)` → `arraySort(array)` / `arrayReverseSort(array)` (identity-lambda elision). Non-identity key selectors emit the higher-order form `arraySort(x -> f(x), array)`.
  * **Scope** — Tier 3 helpers are supported when their result is consumed by a scalar follow-up (`First()`, `Count()`, `Contains(x)`, …) or another array-producing helper. Projecting a slice/reverse/sort result directly as a collection (`Select(e => e.Arr.Skip(1).ToArray())`) goes through EF Core's `QueryableMethodTranslatingExpressionVisitor` and is not yet supported; use a scalar-follow-up or `EF.Functions` indirection in the meantime.
* **`Nullable(T)` element threading through composite types**: `Array(Nullable(T))`, `Tuple(Nullable(T), …)`, `Map(K, Nullable(V))`, and `Variant(Nullable(T), …)` properties now round-trip correctly when the CLR-side type carries `Nullable<T>` for value-type elements (e.g. `int?[]`, `(int?, string)`, `Dictionary<string, int?>`). EF Core's scalar-column convention is to strip `Nullable(...)` in `FindMapping` and carry nullability on `IProperty.IsNullable`; that has no equivalent annotation channel for composite elements, so previously the composite's CLR type was built from the unwrapped `T` element type and materialization failed (`Int32[]` → `Nullable<Int32>[]` coercion error). A new `ClickHouseNullableElementMapping` wrapper and `FindComponentMapping` resolver thread `Nullable<T>` through to the composite mapping's CLR type for value-type elements; reference types and bare `LowCardinality(T)` pass through unchanged. Element-access translations (`First`, `ElementAt`, …) declare `arrayElement` as nullable when the element store type is `Nullable(...)`, so outer `IS NULL` predicates aren't folded away.

### Bug fixes
* Preserve `LowCardinality(...)` and `Nullable(...)` wrappers from `HasColumnType(...)` in generated migration DDL. Previously the wrapper was stripped during type-mapping resolution, so the migration emitted the inner type. ([#18](https://github.com/ClickHouse/ClickHouse.EntityFrameworkCore/issues/18))
* Preserve explicit `HasColumnType(...)` text whenever the resolved mapping's canonical store type differs from the user's input — fixes `Enum8(...)` and `AggregateFunction(...)` columns silently emitting `String` in generated DDL. Also covers `Enum16`, `SimpleAggregateFunction`, `Nested`, and the parameter-bearing forms (`Decimal128(S)`, `Json(...)` with type hints, etc.). ([#24](https://github.com/ClickHouse/ClickHouse.EntityFrameworkCore/issues/24))

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
