using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.ClickHouse.Tests;

public class ArrayLambdaNullsEntity
{
    public long Id { get; set; }
    public string[] Tags { get; set; } = [];
    public string?[] NTags { get; set; } = [];
    public string?[] LcNTags { get; set; } = [];
    public int[] Ints { get; set; } = [];
    public int?[] NInts { get; set; } = [];
    public List<int?> NIntList { get; set; } = [];
}

public class ArrayLambdaNullsDbContext : DbContext
{
    public DbSet<ArrayLambdaNullsEntity> Entities => Set<ArrayLambdaNullsEntity>();

    private readonly string _connectionString;

    public ArrayLambdaNullsDbContext(string connectionString) => _connectionString = connectionString;

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseClickHouse(_connectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ArrayLambdaNullsEntity>(entity =>
        {
            entity.ToTable("array_lambda_nulls");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.Tags).HasColumnName("tags").HasColumnType("Array(String)");
            entity.Property(e => e.NTags).HasColumnName("ntags").HasColumnType("Array(Nullable(String))");
            entity.Property(e => e.LcNTags).HasColumnName("lc_ntags").HasColumnType("Array(LowCardinality(Nullable(String)))");
            entity.Property(e => e.Ints).HasColumnName("ints").HasColumnType("Array(Int32)");
            entity.Property(e => e.NInts).HasColumnName("nints").HasColumnType("Array(Nullable(Int32))");
            entity.Property(e => e.NIntList).HasColumnName("nint_list").HasColumnType("Array(Nullable(Int32))");
        });
    }
}

public class ArrayLambdaNullsFixture : IAsyncLifetime
{
    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        ConnectionString = await SharedContainer.GetConnectionStringAsync();

        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(ConnectionString);
        await connection.OpenAsync();

        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE array_lambda_nulls (
                id Int64,
                tags Array(String),
                ntags Array(Nullable(String)),
                lc_ntags Array(LowCardinality(Nullable(String))),
                ints Array(Int32),
                nints Array(Nullable(Int32)),
                nint_list Array(Nullable(Int32))
            ) ENGINE = MergeTree()
            ORDER BY id
            """;
        await command.ExecuteNonQueryAsync();

        // Row 1 and row 4 have NULL elements. Row 3 has empty arrays.
        command.CommandText = """
            INSERT INTO array_lambda_nulls VALUES
            (1, ['a', 'b'], ['a', NULL], ['a', NULL], [1, 2], [1, NULL], [1, NULL]),
            (2, ['zzz'],    ['zzz'],     ['zzz'],     [2],    [2],       [2]),
            (3, [],         [],          [],          [],     [],        []),
            (4, ['b'],      [NULL],      [NULL],      [3],    [NULL],    [NULL]),
            (5, ['b', 'c'], ['b', 'c'],  ['b', 'c'],  [1, 3], [1, 3],    [1, 3])
            """;
        await command.ExecuteNonQueryAsync();
    }

    public Task DisposeAsync() => Task.CompletedTask;
}

/// <summary>
/// The lambda parameter of an array lambda (<c>arrayExists</c>, <c>arrayCount</c>, <c>arrayMap</c>,
/// <c>arraySort</c>) over an <c>Array(Nullable(T))</c> column can be NULL. Each test runs the same
/// predicate on the server and with LINQ to Objects on the materialized rows, and the two results
/// must be the same.
/// </summary>
public class ArrayLambdaNullableElementTests : IClassFixture<ArrayLambdaNullsFixture>
{
    private readonly ArrayLambdaNullsFixture _fixture;

    public ArrayLambdaNullableElementTests(ArrayLambdaNullsFixture fixture)
    {
        _fixture = fixture;
    }

    // ─── Any(predicate) → arrayExists ───────────────────────────────

    [Fact]
    public async Task Any_CollectionContains_WithNull_MatchesNullElements()
    {
        var names = new[] { "zzz", null };

        var sql = await AssertSameRowsAsDotNet(e => e.NTags.Any(t => names.Contains(t)), [1, 2, 4]);

        // The element can be NULL, so the provider uses the EF Core expansion, which adds the IS NULL term
        // for the null in the collection.
        Assert.Contains(
            "arrayExists(p -> p IS NULL OR (p = {names1:Nullable(String)} AND p IS NOT NULL), `a`.`ntags`)",
            sql);
    }

    [Fact]
    public async Task Any_CollectionContains_WithNull_LowCardinalityNullable_MatchesNullElements()
    {
        var names = new[] { "zzz", null };

        var sql = await AssertSameRowsAsDotNet(e => e.LcNTags.Any(t => names.Contains(t)), [1, 2, 4]);

        Assert.Contains(
            "arrayExists(p -> p IS NULL OR (p = {names1:LowCardinality(Nullable(String))} AND p IS NOT NULL), `a`.`lc_ntags`)",
            sql);
    }

    [Fact]
    public async Task Any_CollectionContains_WithoutNull_DoesNotMatchNullElements()
    {
        var names = new[] { "zzz" };

        var sql = await AssertSameRowsAsDotNet(e => e.NTags.Any(t => names.Contains(t)), [2]);

        Assert.Contains("arrayExists(p -> p = {names1:Nullable(String)} AND p IS NOT NULL, `a`.`ntags`)", sql);
    }

    [Fact]
    public async Task Any_CollectionContains_NonNullableElements_UsesArrayParameter()
    {
        var names = new[] { "zzz", null };

        var sql = await AssertSameRowsAsDotNet(e => e.Tags.Any(t => names.Contains(t)), [2]);

        // An element of Array(String) is never NULL: the collection goes as one array parameter and the
        // null in the collection is removed.
        Assert.Contains("arrayExists(p -> p IN {names:Array(String)}, `a`.`tags`)", sql);
        Assert.DoesNotContain("IS NULL", sql);
    }

    [Fact]
    public async Task Any_EqualsNullOrOtherTest_MatchesNullElements()
    {
        // EF Core translates Any(t => t == null) alone to has(arr, NULL), without a lambda.
        var sql = await AssertSameRowsAsDotNet(e => e.NTags.Any(t => t == null || t.StartsWith("zz")), [1, 2, 4]);

        Assert.Contains("arrayExists(p -> p IS NULL OR ", sql);
    }

    [Fact]
    public async Task Any_NotEqualsValue_MatchesNullElements()
    {
        // In .NET, null != "a" is true.
        var sql = await AssertSameRowsAsDotNet(e => e.NTags.Any(t => t != "a"), [1, 2, 4, 5]);

        Assert.Contains("arrayExists(p -> p <> 'a' OR p IS NULL, `a`.`ntags`)", sql);
    }

    [Fact]
    public async Task Any_NotEqualsValue_NullableValueType_MatchesNullElements()
    {
        // In .NET, (int?)null != 1 is true.
        var sql = await AssertSameRowsAsDotNet(e => e.NInts.Any(n => n != 1), [1, 2, 4, 5]);

        Assert.Contains("arrayExists(p -> p <> 1 OR p IS NULL, `a`.`nints`)", sql);
    }

    [Fact]
    public async Task Any_GreaterThan_NullableValueType_DoesNotMatchNullElements()
    {
        // In .NET, (int?)null > 1 is false. EF Core writes the nullable comparison as a CASE that gives 0
        // for a NULL element.
        var sql = await AssertSameRowsAsDotNet(e => e.NInts.Any(n => n > 1), [2, 5]);

        Assert.Contains("WHEN p > 1 THEN 1", sql);
    }

    [Fact]
    public async Task Any_NonNullableElements_HasNoNullTerms()
    {
        var sql = await AssertSameRowsAsDotNet(e => e.Tags.Any(t => t != "a"), [1, 2, 4, 5]);

        Assert.Contains("arrayExists(p -> p <> 'a', `a`.`tags`)", sql);
        Assert.DoesNotContain("NULL", sql);
    }

    [Fact]
    public async Task Any_NonNullableValueTypeElements_HasNoNullTerms()
    {
        var sql = await AssertSameRowsAsDotNet(e => e.Ints.Any(n => n != 1), [1, 2, 4, 5]);

        Assert.Contains("arrayExists(p -> p <> 1, `a`.`ints`)", sql);
        Assert.DoesNotContain("NULL", sql);
    }

    [Fact]
    public async Task Any_IsNullOrEmpty_MatchesNullElements()
    {
        var sql = await AssertSameRowsAsDotNet(e => e.NTags.Any(t => string.IsNullOrEmpty(t)), [1, 4]);

        Assert.Contains("p IS NULL", sql);
    }

    [Fact]
    public async Task Any_AfterSkip_KeepsElementNullability()
    {
        // arraySlice keeps the element mapping of the source array. Row 1 ['a', NULL] → [NULL].
        var sql = await AssertSameRowsAsDotNet(e => e.NTags.Skip(1).Any(t => t != "c"), [1]);

        Assert.Contains("arrayExists(p -> p <> 'c' OR p IS NULL, arraySlice(`a`.`ntags`, ", sql);
    }

    // ─── Count(predicate) → arrayCount ──────────────────────────────

    [Fact]
    public async Task Count_NotEqualsNull_CountsOnlyNonNullElements()
    {
        var sql = await AssertSameRowsAsDotNet(e => e.NTags.Count(t => t != null) == 1, [1, 2]);

        Assert.Contains("arrayCount(p -> p IS NOT NULL, `a`.`ntags`)", sql);
    }

    [Fact]
    public async Task Count_EqualsNull_LowCardinalityNullable_CountsNullElements()
    {
        var sql = await AssertSameRowsAsDotNet(e => e.LcNTags.Count(t => t == null) >= 1, [1, 4]);

        Assert.Contains("arrayCount(p -> p IS NULL, `a`.`lc_ntags`)", sql);
    }

    [Fact]
    public async Task Count_EqualsNull_ListOfNullableValueType_CountsNullElements()
    {
        var sql = await AssertSameRowsAsDotNet(e => e.NIntList.Count(n => n == null) == 1, [1, 4]);

        Assert.Contains("arrayCount(p -> p IS NULL, `a`.`nint_list`)", sql);
    }

    [Fact]
    public async Task LongCount_NotEqualsValue_CountsNullElements()
    {
        // Row 1 [1, NULL] and row 5 [1, 3]: two elements that are not 2.
        var sql = await AssertSameRowsAsDotNet(e => e.NInts.LongCount(n => n != 2) == 2L, [1, 5]);

        Assert.Contains("arrayCount(p -> p <> 2 OR p IS NULL, `a`.`nints`)", sql);
    }

    [Fact]
    public async Task Count_EqualsNull_InProjection()
    {
        await using var ctx = new ArrayLambdaNullsDbContext(_fixture.ConnectionString);

        var rows = await ctx.Entities.AsNoTracking().OrderBy(e => e.Id).ToListAsync();
        var expected = rows.Select(e => e.NTags.Count(t => t == null)).ToList();

        var query = ctx.Entities.OrderBy(e => e.Id).Select(e => e.NTags.Count(t => t == null));
        Assert.Contains("arrayCount(p -> p IS NULL, `a`.`ntags`)", query.ToQueryString());

        var actual = await query.ToListAsync();

        Assert.Equal([1, 0, 0, 1, 0], expected);
        Assert.Equal(expected, actual);
    }

    // ─── Select(lambda).Contains(value) → has(arrayMap(...), value) ─

    [Fact]
    public async Task Select_Coalesce_Contains_MatchesNullElements()
    {
        var sql = await AssertSameRowsAsDotNet(e => e.NTags.Select(t => t ?? "none").Contains("none"), [1, 4]);

        Assert.Contains("has(arrayMap(p -> COALESCE(p, 'none'), `a`.`ntags`), 'none')", sql);
    }

    [Fact]
    public async Task Select_EqualsNull_Contains_MatchesNullElements()
    {
        var sql = await AssertSameRowsAsDotNet(e => e.NTags.Select(t => t == null).Contains(true), [1, 4]);

        Assert.Contains("has(arrayMap(p -> p IS NULL, `a`.`ntags`), 1)", sql);
    }

    [Fact]
    public async Task Select_Coalesce_NullableValueType_Contains_MatchesNullElements()
    {
        var sql = await AssertSameRowsAsDotNet(e => e.NInts.Select(n => n ?? 0).Contains(0), [1, 4]);

        Assert.Contains("has(arrayMap(p -> COALESCE(p, 0), `a`.`nints`), 0)", sql);
    }

    [Fact]
    public async Task Select_Concat_Contains_MatchesNullElements()
    {
        // In .NET, null + "x" is "x".
        await AssertSameRowsAsDotNet(e => e.NTags.Select(t => t + "x").Contains("x"), [1, 4]);
    }

    // ─── OrderBy(key) → arraySort(lambda, arr) ──────────────────────

    [Fact]
    public async Task OrderBy_CoalesceKey_SortsNullElementsByTheKey()
    {
        await using var ctx = new ArrayLambdaNullsDbContext(_fixture.ConnectionString);

        // The key of a NULL element is -100, so NULL sorts first. ClickHouse sorts a NULL key last, so
        // the COALESCE must stay in the key.
        var rows = await ctx.Entities.AsNoTracking().OrderBy(e => e.Id).ToListAsync();
        var expected = rows.Select(e => e.NInts.OrderBy(n => n ?? -100).FirstOrDefault()).ToList();

        var query = ctx.Entities.OrderBy(e => e.Id).Select(e => e.NInts.OrderBy(n => n ?? -100).FirstOrDefault());
        Assert.Contains("arraySort(p -> COALESCE(p, -100), `a`.`nints`)", query.ToQueryString());

        var actual = await query.ToListAsync();

        Assert.Equal([null, 2, null, null, 1], expected);
        Assert.Equal(expected, actual);
    }

    // ─── FirstOrDefault() → arrayElement(arr, 1) ────────────────────

    [Fact]
    public async Task FirstOrDefault_LowCardinalityNullable_EqualsNull_MatchesNullAndEmpty()
    {
        // Row 3 is empty: arrayElement gives the default of the element type, which is NULL for a
        // Nullable element. Row 4 starts with NULL.
        var sql = await AssertSameRowsAsDotNet(e => e.LcNTags.FirstOrDefault() == null, [3, 4]);

        Assert.Contains("arrayElement(`a`.`lc_ntags`, 1) IS NULL", sql);
    }

    // ─── helpers ────────────────────────────────────────────────────

    /// <summary>
    /// Runs <paramref name="predicate"/> on the server and with LINQ to Objects on all rows, and makes
    /// sure that both give <paramref name="expectedIds"/>. Returns the SQL of the server query.
    /// </summary>
    private async Task<string> AssertSameRowsAsDotNet(
        Expression<Func<ArrayLambdaNullsEntity, bool>> predicate,
        long[] expectedIds)
    {
        await using var ctx = new ArrayLambdaNullsDbContext(_fixture.ConnectionString);

        var rows = await ctx.Entities.AsNoTracking().OrderBy(e => e.Id).ToListAsync();
        var dotNetIds = rows.AsQueryable().Where(predicate).Select(e => e.Id).ToList();
        Assert.Equal(expectedIds, dotNetIds);

        var query = ctx.Entities.Where(predicate);
        var sql = query.ToQueryString();
        var serverIds = await query.OrderBy(e => e.Id).Select(e => e.Id).ToListAsync();
        Assert.True(expectedIds.SequenceEqual(serverIds), $"Expected [{string.Join(", ", expectedIds)}], got [{string.Join(", ", serverIds)}].\n{sql}");

        return sql;
    }
}
