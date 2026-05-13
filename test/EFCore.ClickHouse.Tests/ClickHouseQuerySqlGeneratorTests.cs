using System.Reflection;
using ClickHouse.EntityFrameworkCore.Query.Expressions.Internal;
using ClickHouse.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

namespace EFCore.ClickHouse.Tests;

/// <summary>
/// Unit + integration tests covering ClickHouse-specific overrides in
/// <see cref="ClickHouseQuerySqlGenerator"/> that aren't reachable from the
/// rest of the test suite — primarily <c>VisitRowValue</c>, <c>GenerateValues</c>,
/// and the descending-nullable ordering branch.
/// </summary>
public class ClickHouseQuerySqlGeneratorTests : IClassFixture<ClickHouseFixture>
{
    private readonly ClickHouseFixture _fixture;

    public ClickHouseQuerySqlGeneratorTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    // ─── Inline collection Contains (integration) ────────────
    // Sanity check that primitive-collection Contains() round-trips through the
    // ClickHouse parameter-format path (`{ids:Array(Int64)}`). EF Core 10's default
    // MultipleParameters mode emits IN(...), not a ValuesExpression — the actual
    // GenerateValues body is exercised by the unit tests further down.

    [Fact]
    public async Task InlineCollection_Contains_RoundTrips()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var ids = new long[] { 2, 4, 6 };

        var results = await ctx.TestEntities
            .Where(e => ids.Contains(e.Id))
            .OrderBy(e => e.Id)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(3, results.Count);
        Assert.Equal(2, results[0].Id);
        Assert.Equal(4, results[1].Id);
        Assert.Equal(6, results[2].Id);
    }

    [Fact]
    public void InlineCollection_Contains_DoesNotTranslateToHas()
    {
        using var ctx = new TestDbContext(_fixture.ConnectionString);

        var ids = new long[] { 2, 4, 6 };

        var sql = ctx.TestEntities
            .Where(e => ids.Contains(e.Id))
            .ToQueryString();

        Assert.DoesNotContain("has(", sql);
    }

    [Fact]
    public async Task InlineCollection_Contains_SingleElement_RoundTrips()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var ids = new long[] { 7 };

        var results = await ctx.TestEntities
            .Where(e => ids.Contains(e.Id))
            .AsNoTracking()
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(7, results[0].Id);
    }

    // ─── NULLS LAST branch (integration) ─────────────────────
    // VisitOrdering emits NULLS LAST only for descending nullable expressions;
    // the existing tests only hit the ascending (NULLS FIRST) branch.

    [Fact]
    public async Task OrderByDescending_OnString_EmitsNullsLast()
    {
        await using var ctx = new TestDbContext(_fixture.ConnectionString);

        var results = await ctx.TestEntities
            .OrderByDescending(e => e.Name)
            .Take(3)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal("Jack", results[0].Name);
        Assert.Equal("Ivy", results[1].Name);
        Assert.Equal("Hank", results[2].Name);
    }

    // ─── VisitRowValue (unit) ────────────────────────────────
    // Nothing in the LINQ pipeline currently produces ClickHouseRowValueExpression
    // (the nullability processor only forwards them), so we drive the visitor
    // directly with a constructed instance.

    [Fact]
    public void VisitRowValue_RendersParenthesizedCommaSeparatedValues()
    {
        using var ctx = new GeneratorTestDbContext();
        var generator = CreateGenerator(ctx);

        var typeMappingSource = ctx.GetService<IRelationalTypeMappingSource>();
        var intMapping = typeMappingSource.FindMapping(typeof(int))!;

        var rowValue = new ClickHouseRowValueExpression(
            [
                new SqlConstantExpression(1, intMapping),
                new SqlConstantExpression(2, intMapping),
                new SqlConstantExpression(3, intMapping)
            ],
            typeof(ValueTuple<int, int, int>));

        var sql = generator.RenderExpression(rowValue);

        Assert.Equal("(1, 2, 3)", sql);
    }

    [Fact]
    public void VisitRowValue_TwoValues_RendersAsTuple()
    {
        using var ctx = new GeneratorTestDbContext();
        var generator = CreateGenerator(ctx);

        var typeMappingSource = ctx.GetService<IRelationalTypeMappingSource>();
        var stringMapping = typeMappingSource.FindMapping(typeof(string))!;
        var intMapping = typeMappingSource.FindMapping(typeof(int))!;

        var rowValue = new ClickHouseRowValueExpression(
            [
                new SqlConstantExpression("hello", stringMapping),
                new SqlConstantExpression(42, intMapping)
            ],
            typeof(ValueTuple<string, int>));

        var sql = generator.RenderExpression(rowValue);

        Assert.Equal("('hello', 42)", sql);
    }

    // ─── GenerateValues error paths (unit) ───────────────────
    // RowValues becomes null when EF Core hands a parameter-based ValuesExpression
    // to the SQL generator — that should have been expanded earlier in the
    // pipeline, so generation must fail loudly. An explicit empty rowValues list
    // surfaces the EF Core "empty collection as inline query root" message.

    [Fact]
    public void GenerateValues_NullRowValues_Throws()
    {
        using var ctx = new GeneratorTestDbContext();
        var generator = CreateGenerator(ctx);

        var typeMappingSource = ctx.GetService<IRelationalTypeMappingSource>();
        var intArrayMapping = typeMappingSource.FindMapping(typeof(int[]))!;

        var parameter = new SqlParameterExpression(
            "ids",
            "ids",
            typeof(IReadOnlyList<int>),
            nullable: false,
            translationMode: null,
            intArrayMapping);

        var values = new ValuesExpression("v", parameter, ["Value"]);

        var ex = Assert.Throws<InvalidOperationException>(() => generator.RunGenerateValues(values));
        Assert.Contains("RowValues is null", ex.Message);
    }

    [Fact]
    public void GenerateValues_EmptyRowValues_Throws()
    {
        using var ctx = new GeneratorTestDbContext();
        var generator = CreateGenerator(ctx);

        var values = new ValuesExpression("v", Array.Empty<RowValueExpression>(), ["Value"]);

        Assert.Throws<InvalidOperationException>(() => generator.RunGenerateValues(values));
    }

    // ─── GenerateValues happy path (unit) ────────────────────
    // EF Core 10 with the default MultipleParameters mode rewrites primitive-collection
    // Contains() into IN(?, ?, ?), so no integration LINQ pattern actually reaches the
    // happy-path body. Drive it directly with a constructed ValuesExpression.

    [Fact]
    public void GenerateValues_MultiRow_EmitsSelectUnionAllWithColumnAliases()
    {
        using var ctx = new GeneratorTestDbContext();
        var generator = CreateGenerator(ctx);

        var typeMappingSource = ctx.GetService<IRelationalTypeMappingSource>();
        var intMapping = typeMappingSource.FindMapping(typeof(int))!;
        var stringMapping = typeMappingSource.FindMapping(typeof(string))!;

        var rows = new[]
        {
            new RowValueExpression([
                new SqlConstantExpression(1, intMapping),
                new SqlConstantExpression("a", stringMapping)
            ]),
            new RowValueExpression([
                new SqlConstantExpression(2, intMapping),
                new SqlConstantExpression("b", stringMapping)
            ])
        };

        var values = new ValuesExpression("v", rows, ["Id", "Name"]);

        var sql = generator.RenderGenerateValues(values);

        Assert.Equal(
            "SELECT 1 AS `Id`, 'a' AS `Name`" + Environment.NewLine +
            "UNION ALL SELECT 2 AS `Id`, 'b' AS `Name`",
            sql);
    }

    [Fact]
    public void GenerateValues_SingleRow_NoUnionAll()
    {
        using var ctx = new GeneratorTestDbContext();
        var generator = CreateGenerator(ctx);

        var typeMappingSource = ctx.GetService<IRelationalTypeMappingSource>();
        var intMapping = typeMappingSource.FindMapping(typeof(int))!;

        var rows = new[]
        {
            new RowValueExpression([new SqlConstantExpression(42, intMapping)])
        };

        var values = new ValuesExpression("v", rows, ["Value"]);

        var sql = generator.RenderGenerateValues(values);

        Assert.Equal("SELECT 42 AS `Value`", sql);
        Assert.DoesNotContain("UNION ALL", sql);
    }

    // ─── helpers ────────────────────────────────────────────

    private static TestableClickHouseQuerySqlGenerator CreateGenerator(DbContext ctx)
        => new(ctx.GetService<QuerySqlGeneratorDependencies>());

    private sealed class GeneratorTestDbContext : DbContext
    {
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.UseClickHouse("Host=localhost;Protocol=http;Port=8123;Database=test");
    }

    private sealed class TestableClickHouseQuerySqlGenerator : ClickHouseQuerySqlGenerator
    {
        private static readonly FieldInfo SqlField = typeof(QuerySqlGenerator)
            .GetField("_relationalCommandBuilder", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException(
                "QuerySqlGenerator._relationalCommandBuilder field not found — EF Core internals changed.");

        public TestableClickHouseQuerySqlGenerator(QuerySqlGeneratorDependencies dependencies)
            : base(dependencies)
        {
        }

        public string RenderExpression(SqlExpression expression)
        {
            var builder = AttachFreshCommandBuilder();
            Visit(expression);
            return builder.Build().CommandText;
        }

        public void RunGenerateValues(ValuesExpression valuesExpression)
        {
            AttachFreshCommandBuilder();
            GenerateValues(valuesExpression);
        }

        public string RenderGenerateValues(ValuesExpression valuesExpression)
        {
            var builder = AttachFreshCommandBuilder();
            GenerateValues(valuesExpression);
            return builder.Build().CommandText;
        }

        private IRelationalCommandBuilder AttachFreshCommandBuilder()
        {
            var builder = Dependencies.RelationalCommandBuilderFactory.Create();
            SqlField.SetValue(this, builder);
            return builder;
        }
    }
}
