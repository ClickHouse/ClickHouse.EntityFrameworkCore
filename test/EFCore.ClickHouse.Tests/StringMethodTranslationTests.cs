using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EFCore.ClickHouse.Tests;

public class StringTestEntity
{
    public long Id { get; set; }
    public string Val { get; set; } = string.Empty;
}

public class StringTestDbContext : DbContext
{
    public DbSet<StringTestEntity> Strings => Set<StringTestEntity>();

    private readonly string _connectionString;

    public StringTestDbContext(string connectionString)
    {
        _connectionString = connectionString;
    }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseClickHouse(_connectionString);
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<StringTestEntity>(entity =>
        {
            entity.ToTable("string_test");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.Val).HasColumnName("val");
        });
    }
}

public class StringTestFixture : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder("clickhouse/clickhouse-server:latest").Build();

    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        ConnectionString = _container.GetConnectionString();

        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(ConnectionString);
        await connection.OpenAsync();

        using var createCmd = connection.CreateCommand();
        createCmd.CommandText = """
            CREATE TABLE string_test (
                id Int64,
                val String
            ) ENGINE = MergeTree()
            ORDER BY id
            """;
        await createCmd.ExecuteNonQueryAsync();

        using var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = """
            INSERT INTO string_test (id, val) VALUES
            (1, 'Hello World'),
            (2, '  spaces  '),
            (3, ''),
            (4, 'UPPERCASE'),
            (5, 'Special''Chars\\Here'),
            (6, '你好世界猫狗'),
            (7, 'abc'),
            (8, 'abcdef')
            """;
        await insertCmd.ExecuteNonQueryAsync();
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }
}

public class StringMethodTranslationTests : IClassFixture<StringTestFixture>
{
    private readonly StringTestFixture _fixture;

    public StringMethodTranslationTests(StringTestFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task ToLower_TranslatesToLowerUTF8()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        var result = await ctx.Strings
            .Where(e => e.Id == 4) // "UPPERCASE"
            .Select(e => e.Val.ToLower())
            .AsNoTracking()
            .SingleAsync();

        Assert.Equal("uppercase", result);
    }

    [Fact]
    public async Task ToUpper_TranslatesToUpperUTF8()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        var result = await ctx.Strings
            .Where(e => e.Id == 7) // "abc"
            .Select(e => e.Val.ToUpper())
            .AsNoTracking()
            .SingleAsync();

        Assert.Equal("ABC", result);
    }

    [Fact]
    public async Task ToLower_Unicode()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        var result = await ctx.Strings
            .Where(e => e.Id == 6) // "你好世界猫狗"
            .Select(e => e.Val.ToLower())
            .AsNoTracking()
            .SingleAsync();

        Assert.Equal("你好世界猫狗", result);
    }

    [Fact]
    public async Task Contains_WhereFilter()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        var results = await ctx.Strings
            .Where(e => e.Val.Contains("World"))
            .AsNoTracking()
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(1, results[0].Id);
    }

    [Fact]
    public async Task Contains_EmptySubstring_MatchesAll()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        var count = await ctx.Strings
            .Where(e => e.Val.Contains(""))
            .CountAsync();

        Assert.Equal(8, count);
    }

    [Fact]
    public async Task Contains_NoMatch()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        var results = await ctx.Strings
            .Where(e => e.Val.Contains("ZZZZZ"))
            .AsNoTracking()
            .ToListAsync();

        Assert.Empty(results);
    }

    [Fact]
    public async Task StartsWith_WhereFilter()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        var results = await ctx.Strings
            .Where(e => e.Val.StartsWith("Hello"))
            .AsNoTracking()
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal("Hello World", results[0].Val);
    }

    [Fact]
    public async Task StartsWith_Prefix_MultipleMatches()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        var results = await ctx.Strings
            .Where(e => e.Val.StartsWith("abc"))
            .OrderBy(e => e.Id)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(2, results.Count); // "abc" and "abcdef"
    }

    [Fact]
    public async Task EndsWith_WhereFilter()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        var results = await ctx.Strings
            .Where(e => e.Val.EndsWith("World"))
            .AsNoTracking()
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(1, results[0].Id);
    }

    [Fact]
    public async Task IndexOf_ReturnsZeroBased()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        var result = await ctx.Strings
            .Where(e => e.Id == 1) // "Hello World"
            .Select(e => e.Val.IndexOf("World"))
            .SingleAsync();

        Assert.Equal(6, result); // 0-based: "Hello " = 6 chars
    }

    [Fact]
    public async Task IndexOf_NotFound_ReturnsNegative()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        var result = await ctx.Strings
            .Where(e => e.Id == 1)
            .Select(e => e.Val.IndexOf("missing"))
            .SingleAsync();

        // positionUTF8 returns 0 when not found, minus 1 = -1
        Assert.Equal(-1, result);
    }

    [Fact]
    public async Task Replace_TranslatesToReplaceAll()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        var result = await ctx.Strings
            .Where(e => e.Id == 1) // "Hello World"
            .Select(e => e.Val.Replace("World", "Earth"))
            .AsNoTracking()
            .SingleAsync();

        Assert.Equal("Hello Earth", result);
    }

    [Fact]
    public async Task Substring_OneArg_ZeroBasedToOneBased()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        var result = await ctx.Strings
            .Where(e => e.Id == 1) // "Hello World"
            .Select(e => e.Val.Substring(6))
            .AsNoTracking()
            .SingleAsync();

        Assert.Equal("World", result);
    }

    [Fact]
    public async Task Substring_TwoArgs_ZeroBasedToOneBased()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        var result = await ctx.Strings
            .Where(e => e.Id == 1) // "Hello World"
            .Select(e => e.Val.Substring(0, 5))
            .AsNoTracking()
            .SingleAsync();

        Assert.Equal("Hello", result);
    }

    [Fact]
    public async Task Trim_RemovesWhitespace()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        var result = await ctx.Strings
            .Where(e => e.Id == 2) // "  spaces  "
            .Select(e => e.Val.Trim())
            .AsNoTracking()
            .SingleAsync();

        Assert.Equal("spaces", result);
    }

    [Fact]
    public async Task Trim_WithConstantChar_RemovesWhitespace()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        var result = await ctx.Strings
            .Where(e => e.Id == 2) // "  spaces  "
            .Select(e => e.Val.Trim(' '))
            .AsNoTracking()
            .SingleAsync();

        Assert.Equal("spaces", result);
    }

    [Fact]
    public async Task TrimStart_RemovesLeadingWhitespace()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        var result = await ctx.Strings
            .Where(e => e.Id == 2) // "  spaces  "
            .Select(e => e.Val.TrimStart())
            .AsNoTracking()
            .SingleAsync();

        Assert.Equal("spaces  ", result);
    }

    [Fact]
    public async Task TrimStart_WithConstantChar_RemovesLeadingWhitespace()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        var result = await ctx.Strings
            .Where(e => e.Id == 2) // "  spaces  "
            .Select(e => e.Val.TrimStart(' '))
            .AsNoTracking()
            .SingleAsync();

        Assert.Equal("spaces  ", result);
    }

    [Fact]
    public async Task TrimEnd_RemovesTrailingWhitespace()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        var result = await ctx.Strings
            .Where(e => e.Id == 2) // "  spaces  "
            .Select(e => e.Val.TrimEnd())
            .AsNoTracking()
            .SingleAsync();

        Assert.Equal("  spaces", result);
    }

    [Fact]
    public async Task TrimEnd_WithConstantChar_RemovesTrailingWhitespace()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        var result = await ctx.Strings
            .Where(e => e.Id == 2) // "  spaces  "
            .Select(e => e.Val.TrimEnd(' '))
            .AsNoTracking()
            .SingleAsync();

        Assert.Equal("  spaces", result);
    }

    [Fact]
    public async Task IsNullOrEmpty_WhereFilter()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        var results = await ctx.Strings
            .Where(e => string.IsNullOrEmpty(e.Val))
            .AsNoTracking()
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(3, results[0].Id); // empty string row
    }

    [Fact]
    public async Task Length_InProjection()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        var result = await ctx.Strings
            .Where(e => e.Id == 1) // "Hello World"
            .Select(e => e.Val.Length)
            .SingleAsync();

        Assert.Equal(11, result);
    }

    [Fact]
    public async Task Length_Unicode_CharCount()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        var result = await ctx.Strings
            .Where(e => e.Id == 6) // "你好世界猫狗" — 6 characters
            .Select(e => e.Val.Length)
            .SingleAsync();

        Assert.Equal(6, result);
    }

    [Fact]
    public async Task Length_EmptyString()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        var result = await ctx.Strings
            .Where(e => e.Id == 3) // ""
            .Select(e => e.Val.Length)
            .SingleAsync();

        Assert.Equal(0, result);
    }

    [Fact]
    public async Task StringConcat_InProjection()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        var result = await ctx.Strings
            .Where(e => e.Id == 7) // "abc"
            .Select(e => e.Val + "_suffix")
            .AsNoTracking()
            .SingleAsync();

        Assert.Equal("abc_suffix", result);
    }

    [Fact]
    public async Task StringConcat_PrefixAndSuffix()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        var result = await ctx.Strings
            .Where(e => e.Id == 7) // "abc"
            .Select(e => "prefix_" + e.Val + "_suffix")
            .AsNoTracking()
            .SingleAsync();

        Assert.Equal("prefix_abc_suffix", result);
    }

    [Fact]
    public async Task StringConcat_TwoColumns()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        // Concatenate Id (as part of expression) with Val
        var result = await ctx.Strings
            .Where(e => e.Id == 1) // "Hello World"
            .Select(e => e.Val + e.Val)
            .AsNoTracking()
            .SingleAsync();

        Assert.Equal("Hello WorldHello World", result);
    }

    [Fact]
    public async Task StringConcat_MultipleOperands()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        // a + b + c + d — produces nested binary adds
        var result = await ctx.Strings
            .Where(e => e.Id == 7) // "abc"
            .Select(e => "[" + e.Val + "|" + e.Val + "]")
            .AsNoTracking()
            .SingleAsync();

        Assert.Equal("[abc|abc]", result);
    }

    [Fact]
    public async Task StringConcat_EmptyString()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        var result = await ctx.Strings
            .Where(e => e.Id == 3) // ""
            .Select(e => e.Val + "appended")
            .AsNoTracking()
            .SingleAsync();

        Assert.Equal("appended", result);
    }

    [Fact]
    public async Task StringConcat_InWhere()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        var results = await ctx.Strings
            .Where(e => e.Val + "!" == "abc!")
            .AsNoTracking()
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(7, results[0].Id);
    }

    [Fact]
    public async Task StringConcat_Unicode()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        // Row 6 has "你好世界猫狗" (6 Unicode characters)
        var result = await ctx.Strings
            .Where(e => e.Id == 6)
            .Select(e => e.Val + "!")
            .AsNoTracking()
            .SingleAsync();

        Assert.Equal("你好世界猫狗!", result);
    }

    [Fact]
    public async Task Where_Length_Filter()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        var results = await ctx.Strings
            .Where(e => e.Val.Length > 5)
            .OrderBy(e => e.Id)
            .AsNoTracking()
            .ToListAsync();

        // "Hello World"(11), "  spaces  "(10), "UPPERCASE"(9), "Special'Chars\Here"(18), "你好世界猫狗"(6), "abcdef"(6)
        Assert.Equal(6, results.Count);
    }

    [Fact]
    public async Task Contains_Unicode()
    {
        await using var ctx = new StringTestDbContext(_fixture.ConnectionString);

        var results = await ctx.Strings
            .Where(e => e.Val.Contains("你好"))
            .AsNoTracking()
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(6, results[0].Id);
    }
}
