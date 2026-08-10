using ClickHouse.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.ClickHouse.Tests;

public class DateTimeEntity
{
    public long Id { get; set; }

    /// <summary>Mapped to ClickHouse <c>DateTime</c>.</summary>
    public DateTime Timestamp { get; set; }

    /// <summary>Mapped to ClickHouse <c>DateTime64(3)</c>.</summary>
    public DateTime Timestamp64 { get; set; }

    /// <summary>Mapped to ClickHouse <c>Date32</c>.</summary>
    public DateOnly Date { get; set; }
}

public class DateTimeDbContext : DbContext
{
    public DbSet<DateTimeEntity> Events => Set<DateTimeEntity>();

    private readonly string _connectionString;

    public DateTimeDbContext(string connectionString)
    {
        _connectionString = connectionString;
    }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseClickHouse(_connectionString);
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<DateTimeEntity>(entity =>
        {
            entity.ToTable("datetime_functions_test");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.Timestamp).HasColumnName("ts");
            entity.Property(e => e.Timestamp64).HasColumnName("ts64").HasColumnType("DateTime64(3)");
            entity.Property(e => e.Date).HasColumnName("d");
        });
    }
}

public class DateTimeFixture : IAsyncLifetime
{
    public string ConnectionString { get; private set; } = string.Empty;

    // Reference instant used for all assertions: Monday 2026-08-10 13:47:32.500.
    public static readonly DateTime Instant = new(2026, 8, 10, 13, 47, 32);

    public async Task InitializeAsync()
    {
        ConnectionString = await SharedContainer.GetConnectionStringAsync();

        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(ConnectionString);
        await connection.OpenAsync();

        using var createCmd = connection.CreateCommand();
        createCmd.CommandText = """
                                CREATE TABLE datetime_functions_test (
                                    id Int64,
                                    ts DateTime,
                                    ts64 DateTime64(3),
                                    d Date32
                                ) ENGINE = MergeTree()
                                ORDER BY id
                                """;
        await createCmd.ExecuteNonQueryAsync();

        using var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = """
                                INSERT INTO datetime_functions_test (id, ts, ts64, d) VALUES
                                (1, '2026-08-10 13:47:32', '2026-08-10 13:47:32.500', '2026-08-10')
                                """;
        await insertCmd.ExecuteNonQueryAsync();
    }

    public Task DisposeAsync() => Task.CompletedTask;
}

public class DateTimeFunctionsTranslationTest : IClassFixture<DateTimeFixture>
{
    private readonly DateTimeFixture _fixture;

    public DateTimeFunctionsTranslationTest(DateTimeFixture fixture)
    {
        _fixture = fixture;
    }

    private async Task<T> SelectSingleAsync<T>(Func<IQueryable<DateTimeEntity>, IQueryable<T>> selector)
    {
        await using var context = new DateTimeDbContext(_fixture.ConnectionString);
        return await selector(context.Events.AsNoTracking().Where(e => e.Id == 1)).SingleAsync();
    }

    [Fact]
    public async Task ToStartOfYear_truncates_to_first_of_year()
    {
        var result = await SelectSingleAsync(q => q.Select(e => EF.Functions.ToStartOfYear(e.Timestamp)));
        Assert.Equal(new DateTime(2026, 1, 1), result);
    }

    [Fact]
    public async Task ToStartOfQuarter_truncates_to_first_of_quarter()
    {
        var result = await SelectSingleAsync(q => q.Select(e => EF.Functions.ToStartOfQuarter(e.Timestamp)));
        Assert.Equal(new DateTime(2026, 7, 1), result);
    }

    [Fact]
    public async Task ToStartOfMonth_truncates_to_first_of_month()
    {
        var result = await SelectSingleAsync(q => q.Select(e => EF.Functions.ToStartOfMonth(e.Timestamp)));
        Assert.Equal(new DateTime(2026, 8, 1), result);
    }

    [Fact]
    public async Task ToStartOfWeek_default_mode_starts_on_sunday()
    {
        // 2026-08-10 is a Monday; default mode 0 (Sunday-based) rolls back to 2026-08-09.
        var result = await SelectSingleAsync(q => q.Select(e => EF.Functions.ToStartOfWeek(e.Timestamp)));
        Assert.Equal(new DateTime(2026, 8, 9), result);
    }

    [Fact]
    public async Task ToStartOfWeek_mode_one_starts_on_monday()
    {
        // Mode 1 (Monday-based); 2026-08-10 is itself a Monday.
        var result = await SelectSingleAsync(q => q.Select(e => EF.Functions.ToStartOfWeek(e.Timestamp, 1)));
        Assert.Equal(new DateTime(2026, 8, 10), result);
    }

    [Fact]
    public async Task ToStartOfDay_truncates_to_midnight()
    {
        var result = await SelectSingleAsync(q => q.Select(e => EF.Functions.ToStartOfDay(e.Timestamp)));
        Assert.Equal(new DateTime(2026, 8, 10, 0, 0, 0), result);
    }

    [Fact]
    public async Task ToStartOfHour_truncates_to_hour()
    {
        var result = await SelectSingleAsync(q => q.Select(e => EF.Functions.ToStartOfHour(e.Timestamp)));
        Assert.Equal(new DateTime(2026, 8, 10, 13, 0, 0), result);
    }

    [Fact]
    public async Task ToStartOfMinute_truncates_to_minute()
    {
        var result = await SelectSingleAsync(q => q.Select(e => EF.Functions.ToStartOfMinute(e.Timestamp)));
        Assert.Equal(new DateTime(2026, 8, 10, 13, 47, 0), result);
    }

    [Fact]
    public async Task ToStartOfSecond_truncates_subsecond()
    {
        // toStartOfSecond requires a DateTime64 argument.
        var result = await SelectSingleAsync(q => q.Select(e => EF.Functions.ToStartOfSecond(e.Timestamp64)));
        Assert.Equal(new DateTime(2026, 8, 10, 13, 47, 32), result);
    }

    [Fact]
    public async Task ToStartOfFiveMinutes_truncates_to_five_minute_bucket()
    {
        var result = await SelectSingleAsync(q => q.Select(e => EF.Functions.ToStartOfFiveMinutes(e.Timestamp)));
        Assert.Equal(new DateTime(2026, 8, 10, 13, 45, 0), result);
    }

    [Fact]
    public async Task ToStartOfTenMinutes_truncates_to_ten_minute_bucket()
    {
        var result = await SelectSingleAsync(q => q.Select(e => EF.Functions.ToStartOfTenMinutes(e.Timestamp)));
        Assert.Equal(new DateTime(2026, 8, 10, 13, 40, 0), result);
    }

    [Fact]
    public async Task ToStartOfFifteenMinutes_truncates_to_fifteen_minute_bucket()
    {
        var result = await SelectSingleAsync(q => q.Select(e => EF.Functions.ToStartOfFifteenMinutes(e.Timestamp)));
        Assert.Equal(new DateTime(2026, 8, 10, 13, 45, 0), result);
    }

    [Fact]
    public async Task ToStartOfInterval_with_minute_unit_truncates_to_bucket()
    {
        var result = await SelectSingleAsync(
            q => q.Select(e => EF.Functions.ToStartOfInterval(e.Timestamp, 15, ClickHouseInterval.Minute)));
        Assert.Equal(new DateTime(2026, 8, 10, 13, 45, 0), result);
    }

    [Fact]
    public async Task ToStartOfInterval_with_hour_unit_truncates_to_bucket()
    {
        var result = await SelectSingleAsync(
            q => q.Select(e => EF.Functions.ToStartOfInterval(e.Timestamp, 1, ClickHouseInterval.Hour)));
        Assert.Equal(new DateTime(2026, 8, 10, 13, 0, 0), result);
    }

    [Fact]
    public async Task ToStartOfInterval_on_date_with_month_unit_truncates_to_month()
    {
        var result = await SelectSingleAsync(
            q => q.Select(e => EF.Functions.ToStartOfInterval(e.Date, 1, ClickHouseInterval.Month)));
        Assert.Equal(new DateOnly(2026, 8, 1), result);
    }

    [Fact]
    public async Task ToStartOf_functions_work_in_group_by()
    {
        await using var context = new DateTimeDbContext(_fixture.ConnectionString);

        var buckets = await context.Events
            .AsNoTracking()
            .GroupBy(e => EF.Functions.ToStartOfMonth(e.Timestamp))
            .Select(g => new { Month = g.Key, Count = g.Count() })
            .ToListAsync();

        Assert.Single(buckets);
        Assert.Equal(new DateTime(2026, 8, 1), buckets[0].Month);
        Assert.Equal(1, buckets[0].Count);
    }

    [Fact]
    public void ToStartOfInterval_translates_to_expected_sql()
    {
        using var context = new DateTimeDbContext(_fixture.ConnectionString);

        var sql = context.Events
            .Select(e => EF.Functions.ToStartOfInterval(e.Timestamp, 15, ClickHouseInterval.Minute))
            .ToQueryString();

        Assert.Contains("toStartOfInterval", sql);
        Assert.Contains("toIntervalMinute", sql);
    }

    [Fact]
    public void ToStartOf_functions_translate_to_expected_sql()
    {
        using var context = new DateTimeDbContext(_fixture.ConnectionString);

        var sql = context.Events
            .Select(e => new
            {
                Year = EF.Functions.ToStartOfYear(e.Timestamp),
                Month = EF.Functions.ToStartOfMonth(e.Timestamp),
                Day = EF.Functions.ToStartOfDay(e.Timestamp),
                FifteenMinutes = EF.Functions.ToStartOfFifteenMinutes(e.Timestamp)
            })
            .ToQueryString();

        Assert.Contains("toStartOfYear", sql);
        Assert.Contains("toStartOfMonth", sql);
        Assert.Contains("toStartOfDay", sql);
        Assert.Contains("toStartOfFifteenMinutes", sql);
    }

    [Fact]
    public async Task ToStartOfInterval_with_non_constant_unit_is_not_translatable()
    {
        await using var context = new DateTimeDbContext(_fixture.ConnectionString);

        // A captured variable is parameterized by EF, so the unit is not a SqlConstantExpression and the
        // translator returns null; the call also cannot be client-evaluated, so the query fails to translate.
        var unit = ClickHouseInterval.Minute;

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            context.Events
                .AsNoTracking()
                .Select(e => EF.Functions.ToStartOfInterval(e.Timestamp, 15, unit))
                .ToListAsync());
    }

    [Fact]
    public void ToStartOf_functions_throw_on_client_evaluation()
    {
        var t = DateTime.UnixEpoch;
        Assert.Throws<InvalidOperationException>(() => EF.Functions.ToStartOfYear(t));
        Assert.Throws<InvalidOperationException>(() => EF.Functions.ToStartOfQuarter(t));
        Assert.Throws<InvalidOperationException>(() => EF.Functions.ToStartOfMonth(t));
        Assert.Throws<InvalidOperationException>(() => EF.Functions.ToStartOfWeek(t));
        Assert.Throws<InvalidOperationException>(() => EF.Functions.ToStartOfWeek(t, 1));
        Assert.Throws<InvalidOperationException>(() => EF.Functions.ToStartOfDay(t));
        Assert.Throws<InvalidOperationException>(() => EF.Functions.ToStartOfHour(t));
        Assert.Throws<InvalidOperationException>(() => EF.Functions.ToStartOfMinute(t));
        Assert.Throws<InvalidOperationException>(() => EF.Functions.ToStartOfSecond(t));
        Assert.Throws<InvalidOperationException>(() => EF.Functions.ToStartOfFiveMinutes(t));
        Assert.Throws<InvalidOperationException>(() => EF.Functions.ToStartOfTenMinutes(t));
        Assert.Throws<InvalidOperationException>(() => EF.Functions.ToStartOfFifteenMinutes(t));
        Assert.Throws<InvalidOperationException>(
            () => EF.Functions.ToStartOfInterval(t, 15, ClickHouseInterval.Minute));
    }
}
