using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.ClickHouse.Tests;

public enum ContainsColor { Red = 1, Green = 2, Blue = 3 }

public class ParamContainsEntity
{
    public long Id { get; set; }
    public int IntVal { get; set; }
    public string StrVal { get; set; } = "";
    public Guid GuidVal { get; set; }
    public ContainsColor Color { get; set; }
    public DateTime DateVal { get; set; }
    public decimal DecimalVal { get; set; }
    public bool BoolVal { get; set; }
    public int? NullableInt { get; set; }
}

public class ParamContainsDbContext : DbContext
{
    public DbSet<ParamContainsEntity> Entities => Set<ParamContainsEntity>();

    private readonly string _connectionString;

    public ParamContainsDbContext(string connectionString)
    {
        _connectionString = connectionString;
    }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseClickHouse(_connectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ParamContainsEntity>(entity =>
        {
            entity.ToTable("param_contains");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.IntVal).HasColumnName("int_val");
            entity.Property(e => e.StrVal).HasColumnName("str_val");
            entity.Property(e => e.GuidVal).HasColumnName("guid_val").HasColumnType("UUID");
            entity.Property(e => e.Color).HasColumnName("color").HasColumnType("Enum8('Red'=1,'Green'=2,'Blue'=3)");
            entity.Property(e => e.DateVal).HasColumnName("date_val").HasColumnType("DateTime");
            entity.Property(e => e.DecimalVal).HasColumnName("decimal_val").HasColumnType("Decimal(18, 4)");
            entity.Property(e => e.BoolVal).HasColumnName("bool_val").HasColumnType("Bool");
            entity.Property(e => e.NullableInt).HasColumnName("nullable_int").HasColumnType("Nullable(Int32)");
        });
    }
}

public class ParamContainsFixture : IAsyncLifetime
{
    public string ConnectionString { get; private set; } = string.Empty;

    public static readonly Guid Guid1 = new("11111111-1111-1111-1111-111111111111");
    public static readonly Guid Guid2 = new("22222222-2222-2222-2222-222222222222");
    public static readonly Guid Guid3 = new("33333333-3333-3333-3333-333333333333");

    public async Task InitializeAsync()
    {
        ConnectionString = await SharedContainer.GetConnectionStringAsync();

        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(ConnectionString);
        await connection.OpenAsync();

        using var createCmd = connection.CreateCommand();
        createCmd.CommandText = """
            CREATE TABLE param_contains (
                id Int64,
                int_val Int32,
                str_val String,
                guid_val UUID,
                color Enum8('Red'=1,'Green'=2,'Blue'=3),
                date_val DateTime,
                decimal_val Decimal(18, 4),
                bool_val Bool,
                nullable_int Nullable(Int32)
            ) ENGINE = MergeTree()
            ORDER BY id
            """;
        await createCmd.ExecuteNonQueryAsync();

        using var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = $"""
            INSERT INTO param_contains VALUES
            (1, 10, 'alpha', toUUID('{Guid1}'), 'Red',   '2021-01-01 00:00:00', 1.5,  true,  100),
            (2, 20, 'beta',  toUUID('{Guid2}'), 'Green', '2022-02-02 00:00:00', 2.5,  false, NULL),
            (3, 30, 'gamma', toUUID('{Guid3}'), 'Blue',  '2023-03-03 00:00:00', 3.5,  true,  300)
            """;
        await insertCmd.ExecuteNonQueryAsync();
    }

    public Task DisposeAsync() => Task.CompletedTask;
}

/// <summary>
/// Integration tests for issue #39: a captured collection used with <c>Contains</c> translates to a
/// single native ClickHouse array parameter (<c>has({p:Array(T)}, column)</c>) instead of one bound
/// parameter per element, with per-query <c>EF.Constant</c>/<c>EF.MultipleParameters</c> opt-outs.
/// </summary>
public class ParameterizedContainsTests : IClassFixture<ParamContainsFixture>
{
    private readonly ParamContainsFixture _fixture;

    public ParameterizedContainsTests(ParamContainsFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Int64_Collection_TranslatesToArrayParameter()
    {
        await using var ctx = new ParamContainsDbContext(_fixture.ConnectionString);
        var ids = new[] { 1L, 3L };

        var query = ctx.Entities.Where(e => ids.Contains(e.Id));
        var sql = query.ToQueryString();

        // One array parameter, not one parameter per element.
        Assert.Contains("has({", sql);
        Assert.Contains(":Array(Int64)}", sql);
        Assert.DoesNotContain(" IN (", sql);

        var rows = await query.OrderBy(e => e.Id).Select(e => e.Id).ToListAsync();
        Assert.Equal([1L, 3L], rows);
    }

    [Fact]
    public async Task Int32_Collection_AlignsElementStoreType()
    {
        await using var ctx = new ParamContainsDbContext(_fixture.ConnectionString);
        var vals = new[] { 10, 30 };

        var query = ctx.Entities.Where(e => vals.Contains(e.IntVal));
        Assert.Contains(":Array(Int32)}", query.ToQueryString());

        var rows = await query.OrderBy(e => e.Id).Select(e => e.Id).ToListAsync();
        Assert.Equal([1L, 3L], rows);
    }

    [Fact]
    public async Task String_Collection_TranslatesAndMatches()
    {
        await using var ctx = new ParamContainsDbContext(_fixture.ConnectionString);
        var names = new List<string> { "alpha", "gamma" };

        var query = ctx.Entities.Where(e => names.Contains(e.StrVal));
        Assert.Contains(":Array(String)}", query.ToQueryString());

        var rows = await query.OrderBy(e => e.Id).Select(e => e.Id).ToListAsync();
        Assert.Equal([1L, 3L], rows);
    }

    [Fact]
    public async Task Guid_Collection_TranslatesAndMatches()
    {
        await using var ctx = new ParamContainsDbContext(_fixture.ConnectionString);
        var guids = new[] { ParamContainsFixture.Guid1, ParamContainsFixture.Guid3 };

        var query = ctx.Entities.Where(e => guids.Contains(e.GuidVal));
        Assert.Contains(":Array(UUID)}", query.ToQueryString());

        var rows = await query.OrderBy(e => e.Id).Select(e => e.Id).ToListAsync();
        Assert.Equal([1L, 3L], rows);
    }

    [Fact]
    public async Task DateTime_Collection_FallsBackToPerElementExpansion()
    {
        await using var ctx = new ParamContainsDbContext(_fixture.ConnectionString);
        var dates = new[] { new DateTime(2021, 1, 1), new DateTime(2023, 3, 3) };

        // DateTime is excluded from the array-parameter path: the driver serializes DateTime array
        // elements without the quoting ClickHouse needs, so Array(DateTime) parameters fail to parse.
        // It falls back to the per-element expansion, which serializes each value correctly.
        var query = ctx.Entities.Where(e => dates.Contains(e.DateVal));
        Assert.DoesNotContain("has({", query.ToQueryString());

        var rows = await query.OrderBy(e => e.Id).Select(e => e.Id).ToListAsync();
        Assert.Equal([1L, 3L], rows);
    }

    [Fact]
    public async Task Decimal_Collection_TranslatesAndMatches()
    {
        await using var ctx = new ParamContainsDbContext(_fixture.ConnectionString);
        var amounts = new[] { 1.5m, 3.5m };

        var query = ctx.Entities.Where(e => amounts.Contains(e.DecimalVal));
        Assert.Contains("has({", query.ToQueryString());

        var rows = await query.OrderBy(e => e.Id).Select(e => e.Id).ToListAsync();
        Assert.Equal([1L, 3L], rows);
    }

    [Fact]
    public async Task Bool_Collection_TranslatesAndMatches()
    {
        await using var ctx = new ParamContainsDbContext(_fixture.ConnectionString);
        var flags = new[] { true };

        var query = ctx.Entities.Where(e => flags.Contains(e.BoolVal));
        Assert.Contains("has({", query.ToQueryString());

        var rows = await query.OrderBy(e => e.Id).Select(e => e.Id).ToListAsync();
        Assert.Equal([1L, 3L], rows);
    }

    [Fact]
    public async Task NullableColumn_Contains_FallsBackToPreserveNullSemantics()
    {
        await using var ctx = new ParamContainsDbContext(_fixture.ConnectionString);
        var vals = new int?[] { 100, 300 };

        // A nullable tested column falls back to the base expansion rather than the has(...) array
        // path: `has(arr, NULL)` returns a concrete 0, so a negated `has` would treat a NULL row
        // differently from `x NOT IN (...)`. Deferring to the base path keeps whatever null semantics
        // the standard IN translation produces, so the array-parameter path never changes them.
        var positive = ctx.Entities.Where(e => vals.Contains(e.NullableInt));
        Assert.DoesNotContain("has({", positive.ToQueryString());
        Assert.Equal([1L, 3L], await positive.OrderBy(e => e.Id).Select(e => e.Id).ToListAsync());
    }

    [Fact]
    public async Task Enum_Collection_FallsBackToPerElementExpansion()
    {
        await using var ctx = new ParamContainsDbContext(_fixture.ConnectionString);
        var colors = new[] { ContainsColor.Red, ContainsColor.Blue };

        // A CLR enum maps to Enum8 via a value converter. The driver can't serialize the raw enum
        // collection as a native array, so this element type falls back to the standard per-element
        // expansion (the converter is applied to each parameter) rather than the has(...) array path.
        var query = ctx.Entities.Where(e => colors.Contains(e.Color));
        var sql = query.ToQueryString();
        Assert.DoesNotContain("has({", sql);
        Assert.Contains(" IN (", sql);

        var rows = await query.OrderBy(e => e.Id).Select(e => e.Id).ToListAsync();
        Assert.Equal([1L, 3L], rows);
    }

    [Fact]
    public async Task EmptyCollection_MatchesNoRows()
    {
        await using var ctx = new ParamContainsDbContext(_fixture.ConnectionString);
        var ids = System.Array.Empty<long>();

        var query = ctx.Entities.Where(e => ids.Contains(e.Id));
        // Still a single array parameter — no special-casing to `WHERE 1=0`.
        Assert.Contains("has({", query.ToQueryString());

        var count = await query.CountAsync();
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task LargeCollection_SendsSingleParameter()
    {
        await using var ctx = new ParamContainsDbContext(_fixture.ConnectionString);
        // Comfortably beyond the ~10k one-parameter-per-element ceiling that large IN lists hit,
        // while staying within the server's default parameter-value size limit.
        var ids = Enumerable.Range(1, 12_000).Select(i => (long)i).ToArray();

        var query = ctx.Entities.Where(e => ids.Contains(e.Id));

        // Query text carries exactly one parameter placeholder regardless of collection size.
        var sql = query.ToQueryString();
        Assert.Contains("has({", sql);
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(sql, ":Array(Int64)}".Replace("(", "\\(").Replace(")", "\\)")));

        var count = await query.CountAsync();
        Assert.Equal(3, count);
    }

    [Fact]
    public async Task ListContains_TranslatesToArrayParameter()
    {
        await using var ctx = new ParamContainsDbContext(_fixture.ConnectionString);
        var ids = new List<long> { 2L };

        var query = ctx.Entities.Where(e => ids.Contains(e.Id));
        Assert.Contains("has({", query.ToQueryString());

        var rows = await query.Select(e => e.Id).ToListAsync();
        Assert.Equal([2L], rows);
    }

    [Fact]
    public async Task EfConstant_InlinesValues()
    {
        await using var ctx = new ParamContainsDbContext(_fixture.ConnectionString);
        var ids = new[] { 1L, 2L };

        var query = ctx.Entities.Where(e => EF.Constant(ids).Contains(e.Id));
        var sql = query.ToQueryString();

        // EF.Constant opts out of the array parameter and inlines the values as literals.
        Assert.DoesNotContain("has({", sql);
        Assert.Contains(" IN (1, 2)", sql);

        var rows = await query.OrderBy(e => e.Id).Select(e => e.Id).ToListAsync();
        Assert.Equal([1L, 2L], rows);
    }

    [Fact]
    public async Task EfMultipleParameters_ExpandsToOneParameterPerElement()
    {
        await using var ctx = new ParamContainsDbContext(_fixture.ConnectionString);
        var ids = new[] { 1L, 2L };

        var query = ctx.Entities.Where(e => EF.MultipleParameters(ids).Contains(e.Id));
        var sql = query.ToQueryString();

        // EF.MultipleParameters keeps the legacy one-parameter-per-element expansion.
        Assert.DoesNotContain("has({", sql);
        Assert.Contains(" IN ({", sql);

        var rows = await query.OrderBy(e => e.Id).Select(e => e.Id).ToListAsync();
        Assert.Equal([1L, 2L], rows);
    }

    [Fact]
    public async Task InlineCollection_StillInlines()
    {
        await using var ctx = new ParamContainsDbContext(_fixture.ConnectionString);

        // A literal (non-captured) collection is not a parameter, so it continues to inline.
        var query = ctx.Entities.Where(e => new[] { 1L, 2L }.Contains(e.Id));
        var sql = query.ToQueryString();

        Assert.DoesNotContain("has({", sql);
        Assert.Contains(" IN (1, 2)", sql);

        var rows = await query.OrderBy(e => e.Id).Select(e => e.Id).ToListAsync();
        Assert.Equal([1L, 2L], rows);
    }

    [Fact]
    public async Task ParameterCollectionJoin_StillUsesUnionAll()
    {
        await using var ctx = new ParamContainsDbContext(_fixture.ConnectionString);
        var ids = new[] { 1L, 3L };

        // A collection parameter used as a JOIN source is NOT a Contains, so it keeps the
        // SELECT … UNION ALL … rewrite (the array-parameter path is Contains-only).
        var query = from e in ctx.Entities
                    join id in ids on e.Id equals id
                    orderby e.Id
                    select e.Id;
        var sql = query.ToQueryString();

        Assert.Contains("UNION ALL", sql);
        Assert.DoesNotContain("has({", sql);

        Assert.Equal([1L, 3L], await query.ToListAsync());
    }

    [Fact]
    public async Task NegatedContains_MatchesComplement()
    {
        await using var ctx = new ParamContainsDbContext(_fixture.ConnectionString);
        var ids = new[] { 1L };

        var query = ctx.Entities.Where(e => !ids.Contains(e.Id));
        Assert.Contains("has({", query.ToQueryString());

        var rows = await query.OrderBy(e => e.Id).Select(e => e.Id).ToListAsync();
        Assert.Equal([2L, 3L], rows);
    }
}
