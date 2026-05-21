---
name: review
description: Review a pull request, branch, or diff for the ClickHouse Entity Framework Core provider. Use when the user asks to review a PR, branch, commit range, or working tree diff for correctness, safety, performance, tests, documentation, and project compliance.
argument-hint: "[PR-number or branch-name or diff-spec]"
disable-model-invocation: false
allowed-tools: Task, Bash, Read, Glob, Grep, WebFetch, AskUserQuestion
---

# ClickHouse EF Core Provider Review Skill

## Arguments

- `$0` (optional): PR number, branch name, or diff spec, such as `123`, `my-feature-branch`, `HEAD~3..HEAD`, or `working-tree`.

## Ground Rules

- Review only. Do not change PR titles, descriptions, commits, or code unless the user explicitly asks for fixes.
- Ignore `/.github/workflows/*` files during review unless the user specifically asks to review CI.
- Prefer high precision. Do not report style preferences, speculative concerns, or "nice to have" refactors.
- Focus on defects that can break EF Core semantics, ClickHouse semantics, user-facing behavior, data correctness, reliability, or maintainability.
- If the diff is too large to review directly, split it by area. Use subagents only when the runtime supports them and the user/session permits delegation.
- If Docker is unavailable, do not run integration or functional tests. Use build/static inspection and note the validation gap.

## Obtaining The Diff

If a PR number is given:

- Fetch PR metadata: title, description, base/head refs, changed files, linked issues.
- Fetch the full PR diff.
- Check CI status if available, especially tests and coverage.

If a branch name is given:

- Compare it against `main` unless PR metadata indicates another base.
- Read commit messages for context.

If a diff spec is given:

- Use that range directly, for example `git diff HEAD~3..HEAD`.
- Read commit messages for the same range when applicable.

If no argument is given:

- Review the working tree: staged and unstaged changes.
- Include untracked files that appear relevant.

For each modified non-workflow file, read enough surrounding code to understand the behavior, not just the changed hunk.

## Project-Specific Review Priorities

### Correctness And EF Core Semantics

- LINQ translations must preserve .NET observable semantics for nulls, booleans, string/array indexing, aggregates, subqueries, joins, set operations, and default values.
- Query translation changes must not silently fall back to client evaluation or produce SQL that ClickHouse accepts with different semantics.
- SQL generation must use ClickHouse syntax: backtick identifiers, `{name:StoreType}` parameters, `1`/`0` boolean literals, `concat()` for string concatenation, explicit `UNION ALL` or `UNION DISTINCT`, and ClickHouse-compatible `LIMIT/OFFSET`.
- LEFT JOIN behavior must preserve EF null semantics. Be careful around `join_use_nulls` and connection-string settings.
- Scalar subquery compensation must stay narrowly targeted. Do not wrap arbitrary non-nullable subqueries with `ifNull(..., 0)`.

### Type Mapping And Materialization

- Type mapping changes must cover store-type parsing, CLR inference, SQL literal generation, ADO.NET parameter binding, binary insert, and data-reader materialization where applicable.
- Integer materialization must handle ClickHouse return types that differ from EF expectations, especially `COUNT()` returning `UInt64`.
- Composite mappings (`Array`, `Map`, `Tuple`, `Variant`, `Dynamic`, `Json`, geo types) often require store-type-driven resolution. Watch for ambiguous CLR-only inference.
- Decimal, BigInteger, JSON, Variant/Dynamic, and geo changes need edge-case tests because they are easy to get subtly wrong.
- Collection mappings must preserve declared CLR property types such as arrays, `List<T>`, and supported collection interfaces.

### Writes, Migrations, And DDL

- INSERT paths should preserve efficient `InsertBinaryAsync` behavior and correct batching.
- `BulkInsertAsync<T>()` should not accidentally track entities or use slow row-by-row SQL.
- UPDATE/DELETE remain unsupported unless the PR explicitly implements a complete ClickHouse mutation strategy.
- Server-generated values, identity, `RETURNING`, foreign keys, and transactional assumptions are not ClickHouse-compatible. Flag accidental reliance on them.
- Migrations must emit ClickHouse-valid DDL and reject unsupported operations with clear exceptions.
- Table engine, `ORDER BY`, `PARTITION BY`, `PRIMARY KEY`, TTL, codecs, comments, and skipping-index behavior must be tested through generated SQL and, when practical, real ClickHouse execution.

### Performance And Resource Safety

- Hot paths include SQL generation, type mapping resolution, data-reader conversion, `SaveChanges` insert batching, and bulk insert.
- Avoid unnecessary allocations, boxing, reflection in per-row materialization paths, sync-over-async, and buffering large result or insert sets.
- Preserve cancellation token flow through async operations.
- Ensure connections, commands, readers, and driver clients are disposed or scoped correctly.
- Do not introduce shared mutable state without thread-safety analysis.

### Public API And Developer Experience

- Public APIs should match EF Core provider conventions and existing naming/style in this repository.
- Exceptions and validation messages should be actionable and mention the unsupported ClickHouse/EF behavior.
- User-visible behavior changes require README updates and, where appropriate, CHANGELOG/RELEASENOTES updates.
- Avoid breaking existing connection string behavior or fluent API usage without clearly calling it out.

## Testing And Coverage Expectations

- Unit tests are appropriate for SQL literal generation, type mapping source resolution, nullability processing, SQL generator shape, and translator output.
- Integration tests in `test/EFCore.ClickHouse.Tests` are appropriate for real ClickHouse behavior, type round-trips, inserts, migrations, and query execution.
- Functional tests in `test/EFCore.ClickHouse.FunctionalTests` are appropriate for EF relational-harness/Northwind-style query coverage.
- Use shared Testcontainers fixtures; do not start a new ClickHouse container per test.
- Do not delete, weaken, or broadly skip existing tests.
- Type system, materialization, SQL literal, query translator, and migration changes should include coverage for nulls, nullable store types, unsupported shapes, and at least one negative/error case when relevant.
- Use `scripts/coverage-summary.py` and `scripts/coverage-uncovered.py` when coverage matters. Focus on changed runtime code, not trivial `Clone()` overrides or no-op plumbing.

Useful commands:

```bash
dotnet build
dotnet test test/EFCore.ClickHouse.Tests/EFCore.ClickHouse.Tests.csproj
dotnet test test/EFCore.ClickHouse.FunctionalTests/EFCore.ClickHouse.FunctionalTests.csproj
dotnet test --filter FullyQualifiedName~TypeMapping
dotnet test --collect:"XPlat Code Coverage"
python3 scripts/coverage-summary.py "test/**/coverage.cobertura.xml" "test/**/TestResults/**/coverage.cobertura.xml"
python3 scripts/coverage-uncovered.py "test/**/coverage.cobertura.xml" "test/**/TestResults/**/coverage.cobertura.xml" ClickHouseTypeMappingSource.cs
```

## What To Ignore

- Pure formatting, whitespace, brace style, and naming preferences unless they create confusion or an API inconsistency.
- Broad refactor suggestions not needed for correctness.
- Micro-optimizations without a realistic hot-path impact.
- Commented debugging code in draft work unless it creates a real risk.
- Workflow files under `/.github/workflows/*`.

## Severity Model

Blockers:

- Data loss, corruption, wrong query results, or incorrect materialization.
- Resource leaks, deadlocks, races, or broken async/cancellation behavior.
- Significant hot-path performance regression.
- Public API or migration behavior that would break realistic users without a migration path.
- Security-sensitive issues.

Majors:

- Missing tests for important edge cases or runtime paths.
- Fragile code likely to fail under normal EF Core or ClickHouse usage.
- Unsupported ClickHouse behavior exposed as if it worked.
- Incomplete user-facing docs for new public behavior.
- Confusing diagnostics in complex or user-facing failure paths.

Nits:

- Only report nits that reduce bug risk or user confusion. Do not use review space for style commentary.

## Output Format

Use the repository's normal review style: findings first, ordered by severity, with file and line references. Be terse and evidence-based. Omit optional sections that have nothing notable.

If there are blockers, majors, or risk-reducing nits, start with findings:

```markdown
## Findings

- [Blocker] `path/to/File.cs:123` - Issue and impact.
  Suggested fix.

- [Major] `path/to/File.cs:456` - Issue and impact.
  Suggested fix.
```

Then include these sections as relevant. `Summary`, `Review Checklist`, and `Final Verdict` are mandatory:

```markdown
## Summary

One paragraph describing what the change does and the high-level verdict.

## Missing Context

- Critical context that was unavailable.

## Tests And Evidence

- What was run or inspected.
- Concrete missing tests, if any.

## Performance And Safety

- Hot-path, concurrency, resource, or failure-mode concerns.

## User-Lens Review

- Surprising behavior, docs gaps, or likely future breaking-change risk.

## Code Coverage

- Whether changed runtime paths are covered.
- Concrete coverage gaps and exact tests to add.

## Extras

- README, CHANGELOG, RELEASENOTES, examples, or migration notes that are required or missing.

## Review Checklist

| Check | Status | Notes |
|---|---|---|
| EF Core query/materialization semantics preserved? | Yes / No / N/A | |
| ClickHouse SQL dialect respected? | Yes / No / N/A | |
| Type mapping paths covered: parse, literal, parameter, read, binary insert? | Yes / No / N/A | |
| Insert/bulk insert behavior preserved? | Yes / No / N/A | |
| Migrations/DDL behavior valid for ClickHouse? | Yes / No / N/A | |
| Async/cancellation/resource disposal correct? | Yes / No / N/A | |
| Existing tests preserved? | Yes / No | |
| New tests cover important edge cases? | Yes / No / N/A | |
| Coverage reviewed for changed runtime paths? | Yes / No / N/A | |
| Docs/release notes updated for user-visible behavior? | Yes / No / N/A | |

## Final Verdict

Status: Approve / Request changes / Block
Minimum required actions:
- ...
```

If there are no blockers or majors, say so clearly and mention any residual validation gaps.
