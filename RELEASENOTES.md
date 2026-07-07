v0.3.1 (Unreleased)
---
### Bug fixes
* Summing a `double` or `float` column (`.SumAsync(x => x.Value)`) no longer throws `InvalidCastException`. Two ClickHouse-specific mismatches were biting: EF Core hands the float literal generator a boxed `Int32` `0` as the empty-result fallback, and ClickHouse widens `sum(Float32)` to `Float64` so the driver couldn't read it back as a `float`. Both the literal generation and the `Float32` read path now convert instead of hard-casting. ([#46](https://github.com/ClickHouse/ClickHouse.EntityFrameworkCore/issues/46)) (Thanks to @HotTotem!)

v0.3.0
---
### Advanced queries
* **Native JSON navigation**: support for `JsonNode` indexing (`Data["key"]`, `Data[index]`) and member access.
  The provider handles ClickHouse 1-based indexing for arrays automatically and supports deep nesting and explicit casting/`.GetValue<T>()`. (Thanks to @HappyEntity!)
* **SimpleJSON functions**: support for `simpleJSONExtract*` and `simpleJSONHas` via `EF.Functions`. (Thanks to @HappyEntity!)
* **Array-column helpers on mapped `Array(T)` columns** translate to native ClickHouse array functions via the new `ClickHouseArrayMethodTranslator`. Search items are aligned to the column's element type mapping (Int64 vs Int32, FixedString(N) vs String, Enum8 vs String, …) so parameters serialize with the correct ClickHouse store type. Only mapped array columns are routed through these translations; LINQ over local in-memory collections continues to flow through the inline-`SELECT … UNION ALL …` path.
  * `Enumerable.Contains` / `Queryable.Contains` / instance `List<T>.Contains` (plus the `ICollection<>`/`IList<>`/`IReadOnlyList<>`/`IReadOnlyCollection<>` interfaces) → `has(array, value)`. (Thanks to @moxplod!)
  * `Array.Length` / `List<T>.Count` member access / `Enumerable.Count()` / `Queryable.Count()` → `length(array)` (`int`); `LongCount()` → `length(array)` (`long`).
  * `Enumerable.Any()` / `Queryable.Any()` → `notEmpty(array)`. Note that EF Core normalizes `arr.Count > 0` / `arr.Count() > 0` (and similar) into `Any()` *before* the provider sees them, so those expressions emit `notEmpty(...)` rather than `length(...) > 0`. Use `Count == N` (with `N > 0`) when you want `length(...)` for a row-shape assertion.
  * `Any(x => f(x))` / `Count(x => f(x))` / `LongCount(x => f(x))` predicate overloads → `arrayExists(x -> f(x), array)` / `arrayCount(x -> f(x), array)`. The predicate body is translated through the existing scalar translator chain, just like `Select(...).Contains(...)`.
  * `arr.Select(x => f(x)).Contains(value)` → `has(arrayMap(x -> f(x), array), value)`. The inner lambda body is translated through the existing scalar translator chain, so e.g. `x => x.ToLower()` becomes `lowerUTF8(x)` (the shape Gridify's `CaseInsensitiveFiltering` produces for string-array properties).
  * **Element access** — `First()` / `FirstOrDefault()` → `arrayElement(array, 1)`; `Last()` / `LastOrDefault()` → `arrayElement(array, -1)`; `ElementAt(i)` / `ElementAtOrDefault(i)` → `arrayElement(array, i + 1)` (the `+ 1` is computed in Int64 so `int.MaxValue` doesn't wrap). ClickHouse returns the element type's default on empty / out-of-bounds rather than throwing; LINQ-side negative indices map to ClickHouse from-end addressing once `+ 1` is applied (so `ElementAt(-1)` is OOB → default, but `ElementAt(-2)` returns the last element) — divergences from .NET LINQ that preserve ClickHouse semantics.
  * **Slicing** — `Skip(n)` → `arraySlice(array, n + 1)`; `Take(m)` → `arraySlice(array, 1, m)`. Both produce mapped `Array(T)` results, so chained downstream helpers (`arr.Skip(1).Contains(x)`, `arr.Skip(n).Take(m).FirstOrDefault()`) compose without further configuration.
  * **Ordering / reversal / deduplication** — `Reverse()` → `arrayReverse(array)`; `Distinct()` → `arrayDistinct(array)`; `OrderBy(x => x)` / `OrderByDescending(x => x)` → `arraySort(array)` / `arrayReverseSort(array)`. Non-identity key selectors emit the higher-order form `arraySort(x -> f(x), array)`.
  * **Scope** — Tier 3 helpers fire when their result is consumed by a scalar follow-up (`First()`, `Count()`, `Contains(x)`, …) or another array-producing helper. Projecting a slice/reverse/sort result directly as a collection (`Select(e => e.Arr.Skip(1).ToArray())`) is not yet supported.
* **`Nullable(T)` elements in composite types** now round-trip end-to-end for `Array(Nullable(T))`, `Tuple(Nullable(T), …)`, `Map(K, Nullable(V))`, and `Variant(Nullable(T), …)`. Previously the resolver stripped the `Nullable(...)` wrapper and built composites whose CLR type used `T` instead of `Nullable<T>` for value-type elements, breaking `int?[]` / `(int?, string)` / `Dictionary<string, int?>` materialization. Element nullability also flows through `arrayElement` so `Where(e => e.NullableInts.First() == null)` filters correctly.

### Bug fixes
* `HasColumnType("Enum8(...)")`, `HasColumnType("AggregateFunction(...)")`, and similar parameterized or aliased store types are now preserved verbatim in generated migration DDL. Previously these silently emitted `String` because the resolver canonicalized to a generic fallback mapping. ([#24](https://github.com/ClickHouse/ClickHouse.EntityFrameworkCore/issues/24)) (Thanks to @Felixzed!)

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
