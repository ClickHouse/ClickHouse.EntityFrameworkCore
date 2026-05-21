# Agent Contribution Guide

## Project Overview

This repository contains the official Entity Framework Core provider for ClickHouse. It is built on top of `ClickHouse.Driver` and implements EF Core relational provider services with ClickHouse-specific SQL generation, type mapping, migrations, and write-path behavior.

Primary target versions:

- .NET: `net10.0`
- EF Core: `Microsoft.EntityFrameworkCore.Relational` 10.x
- ClickHouse ADO.NET driver: `ClickHouse.Driver`
- Tests: xUnit v3 with Testcontainers for integration coverage

## Project Structure

```text
src/EFCore.ClickHouse/
  Extensions/                                  Public entry points such as UseClickHouse()
  Infrastructure/                              Options, model validation, singleton options
  Diagnostics/Internal/                        Provider logging definitions
  Storage/Internal/                            Connection, SQL helper, database creator, type mapping source
  Storage/Internal/Mapping/                    Individual ClickHouse type mappings
  Query/                                       SQL expression factory
  Query/Internal/                              Query pipeline visitors, processors, SQL generator
  Query/Expressions/Internal/                  Custom SQL AST nodes
  Query/ExpressionTranslators/Internal/        LINQ member/method/aggregate translators
  Metadata/                                    Annotations, fluent API builders, conventions
  Migrations/                                  Migration operations and SQL generation
  Update/Internal/                             SaveChanges insert batching and unsupported mutation paths

test/EFCore.ClickHouse.Tests/                  Focused unit and integration tests
test/EFCore.ClickHouse.FunctionalTests/        EF relational-harness/Northwind-style query tests
test/EFCore.ClickHouse.DesignSmoke/            dotnet-ef/design-time smoke project
```

## Build And Test

Use the solution-level commands unless you have a reason to narrow the scope:

```bash
dotnet build
dotnet test
```

Integration and functional tests require Docker because they use `Testcontainers.ClickHouse` to start a real ClickHouse server.

For targeted runs:

```bash
dotnet test test/EFCore.ClickHouse.Tests/EFCore.ClickHouse.Tests.csproj
dotnet test test/EFCore.ClickHouse.FunctionalTests/EFCore.ClickHouse.FunctionalTests.csproj
dotnet test --filter FullyQualifiedName~TypeMapping
```

For coverage, prefer the collector output and parse the Cobertura XML directly:

```bash
dotnet test --collect:"XPlat Code Coverage"
```

Do not generate HTML coverage reports for routine agent work; they are slower and harder to inspect programmatically.

### Coverage Helpers

Both test projects include `coverlet.collector` and `coverlet.msbuild`. After running coverage, use the helper scripts in `scripts/` to inspect the generated Cobertura XML:

```bash
python3 scripts/coverage-summary.py "test/**/coverage.cobertura.xml" "test/**/TestResults/**/coverage.cobertura.xml"
python3 scripts/coverage-uncovered.py "test/**/coverage.cobertura.xml" "test/**/TestResults/**/coverage.cobertura.xml" ClickHouseTypeMappingSource.cs
```

`scripts/coverage-summary.py` prints per-file coverage sorted worst-first. `scripts/coverage-uncovered.py` prints uncovered line numbers for a specific source file. Both scripts accept multiple coverage XML paths or glob patterns and use the most recent matching file.

## Development Workflow

- Make focused changes that match the existing provider patterns.
- Keep public docs current when behavior changes. Update `README.md`, `CHANGELOG.md`, or `RELEASENOTES.md` when appropriate.
- Do not edit local-only files such as `AGENTS.local.md` if present.
- If there is any doubt at all about ClickHouse db behavior, test it empirically.
- For PR or diff reviews, use the project-specific review guidance in `skills/review/SKILL.md`.
- Avoid unrelated refactors, formatting churn, and broad rewrites.
- Avoid ad-hoc solutions; prefer clean abstractions and logical groupings that are extensible and reusable.
- When adding provider services, register them in `ClickHouseServiceCollectionExtensions.AddEntityFrameworkClickHouse()`.

## Where To Make Common Changes

- New ClickHouse type mapping: add or update a class under `Storage/Internal/Mapping/`, then register it in `ClickHouseTypeMappingSource`.
- New LINQ method translation: add a translator under `Query/ExpressionTranslators/Internal/` and register it in the relevant translator provider.
- SQL syntax changes: update `ClickHouseQuerySqlGenerator` or `ClickHouseSqlGenerationHelper`.
- Custom SQL expression node: add it under `Query/Expressions/Internal/`, then handle it in SQL generation and nullability processing.
- Migrations or DDL behavior: update `Migrations/Internal/` and cover the generated SQL.
- SaveChanges write-path behavior: update `Update/Internal/ClickHouseModificationCommandBatch` and related factory/connection code.

## Testing Guidelines

- Use unit tests for type mapping resolution, SQL literal generation, nullability processing, and SQL generator edge cases.
- Use integration tests in `EFCore.ClickHouse.Tests` for provider behavior that must run against real ClickHouse.
- Use functional tests in `EFCore.ClickHouse.FunctionalTests` for EF relational query-suite parity and Northwind-style query behavior.
- Use `IClassFixture<T>` and shared fixtures so ClickHouse containers are not started per test.
- xUnit v3 `IAsyncLifetime` methods return `Task`.
- Give each integration fixture an isolated database or table setup. Prefer deterministic seed data.
- Assert both result semantics and SQL shape when a bug is specifically about translation.
- Cover runtime paths such as `GenerateNonNullSqlLiteral()`, data-reader materialization, conversion helpers, type resolution branches, query translators, and SQL generator overrides.
- It is acceptable to leave trivial `Clone()` overrides, pass-through constructors, and no-op transaction plumbing lightly covered.

In general, prefer integration tests that actually talk with the database over unit tests.

When writing tests that use the driver directly from the test project, prefer `global::ClickHouse.Driver.ADO.ClickHouseConnection` to avoid namespace collisions with this provider's `ClickHouse.EntityFrameworkCore` namespace.

## Design Considerations

ClickHouse is not a general OLTP database, and the provider should preserve ClickHouse semantics rather than forcing a standard relational shape where it does not fit.

Provider-specific design rules:

- Use ClickHouse-native SQL functions when translating LINQ.
- Preserve .NET observable semantics in translations, especially around nulls, indexing, and default values.
- Do not assume ClickHouse supports relational constraints, row-level transactions, `RETURNING`, identity values, or OLTP-style updates.
- ClickHouse does not support transactions, foreign keys, unique primary keys, or returned auto-increment ids.
- Prefer efficient write paths. Inserts should use the driver's native bulk APIs where possible.
- Be explicit about ClickHouse settings that affect semantics. For example, left join null semantics depend on `join_use_nulls`.
- Be careful with composite type mappings. `Array`, `Map`, `Tuple`, `Variant`, `Dynamic`, `Json`, and geo types often require store-type-driven resolution.

## Current Feature Areas

The provider supports connection setup, read-oriented LINQ queries, grouping and aggregates, string and math translations, joins, subqueries, set operations, insert-only `SaveChanges`, bulk insert, table engine configuration, migrations for supported DDL operations, and a broad ClickHouse type system.

Known unsupported or limited areas include:

- UPDATE and DELETE mutation support.
- Server-generated values such as identity columns or `RETURNING`.
- Reverse engineering/scaffolding.
- Collection method translation.
- Full EF Core specification-test coverage.
- Advanced JSON features.

## Pre-PR Checklist

Before finishing a change:

- Build the solution or the affected projects.
- Run the relevant test project or a targeted filter.
- Add or update tests for changed behavior.
- Check code coverage using the provided scripts.
- Update public docs for user-visible behavior.
- Launch a sub-agent to do a review. Evaluate the result and implement any necessary changes.
- If the changes have an implications for the long-term design of the library, make sure to mention them.
