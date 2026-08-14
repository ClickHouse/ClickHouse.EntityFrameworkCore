using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.ClickHouse.Tests;

public class DateTimeMemberEntity
{
    public long Id { get; set; }

    /// <summary>Mapped to ClickHouse <c>DateTime</c>, which holds whole seconds only.</summary>
    public DateTime Timestamp { get; set; }

    /// <summary>Mapped to ClickHouse <c>DateTime64(7)</c>. Precision 7 is one .NET tick.</summary>
    public DateTime Timestamp64 { get; set; }

    /// <summary>Mapped to ClickHouse <c>Date32</c>.</summary>
    public DateOnly Date { get; set; }

    /// <summary>Mapped to ClickHouse <c>DateTime64(7, 'UTC')</c> by the DateTimeOffset mapping.</summary>
    public DateTimeOffset Offset { get; set; }
}

public class DateTimeMemberDbContext : DbContext
{
    public DbSet<DateTimeMemberEntity> Events => Set<DateTimeMemberEntity>();

    private readonly string _connectionString;

    public DateTimeMemberDbContext(string connectionString)
    {
        _connectionString = connectionString;
    }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseClickHouse(_connectionString);
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<DateTimeMemberEntity>(entity =>
        {
            entity.ToTable("datetime_member_test");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.Timestamp).HasColumnName("ts");
            entity.Property(e => e.Timestamp64).HasColumnName("ts64").HasColumnType("DateTime64(7)");
            entity.Property(e => e.Date).HasColumnName("d");
            entity.Property(e => e.Offset).HasColumnName("off");
        });
    }
}

public class DateTimeMemberFixture : IAsyncLifetime
{
    public string ConnectionString { get; private set; } = string.Empty;

    /// <summary>
    /// Row 1's instant: Sunday 2026-08-16 13:47:32.1234567. A Sunday on purpose — ClickHouse
    /// <c>toDayOfWeek</c> gives Sunday 7 in its default mode and 0 in mode 2, so a Sunday is the day
    /// that proves the mode argument reaches the server.
    /// </summary>
    public static readonly DateTime Instant = new DateTime(2026, 8, 16, 13, 47, 32).AddTicks(1_234_567);

    /// <summary>Row 1's time of day, to one tick.</summary>
    public static readonly TimeSpan InstantTimeOfDay = TimeSpan.FromTicks(496_521_234_567);

    public async Task InitializeAsync()
    {
        ConnectionString = await SharedContainer.GetConnectionStringAsync();

        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(ConnectionString);
        await connection.OpenAsync();

        using var createCmd = connection.CreateCommand();
        createCmd.CommandText = """
                                CREATE TABLE datetime_member_test (
                                    id Int64,
                                    ts DateTime,
                                    ts64 DateTime64(7),
                                    d Date32,
                                    off DateTime64(7, 'UTC')
                                ) ENGINE = MergeTree()
                                ORDER BY id
                                """;
        await createCmd.ExecuteNonQueryAsync();

        using var insertCmd = connection.CreateCommand();
        // Row 2 is the last day of a month, so AddMonths and AddYears have a day to clamp.
        insertCmd.CommandText = """
                                INSERT INTO datetime_member_test (id, ts, ts64, d, off) VALUES
                                (1, '2026-08-16 13:47:32', '2026-08-16 13:47:32.1234567', '2026-08-16', '2026-08-16 13:47:32.1234567'),
                                (2, '2026-01-31 00:00:00', '2026-01-31 00:00:00.0000000', '2026-01-31', '2026-01-31 00:00:00.0000000')
                                """;
        await insertCmd.ExecuteNonQueryAsync();
    }

    public Task DisposeAsync() => Task.CompletedTask;
}

public class DateTimeMemberTranslationTest : IClassFixture<DateTimeMemberFixture>
{
    private readonly DateTimeMemberFixture _fixture;

    public DateTimeMemberTranslationTest(DateTimeMemberFixture fixture)
    {
        _fixture = fixture;
    }

    private async Task<T> SelectSingleAsync<T>(
        Func<IQueryable<DateTimeMemberEntity>, IQueryable<T>> selector,
        long id = 1)
    {
        await using var context = new DateTimeMemberDbContext(_fixture.ConnectionString);
        return await selector(context.Events.AsNoTracking().Where(e => e.Id == id)).SingleAsync();
    }

    private async Task<List<long>> WhereIdsAsync(
        System.Linq.Expressions.Expression<Func<DateTimeMemberEntity, bool>> predicate)
    {
        await using var context = new DateTimeMemberDbContext(_fixture.ConnectionString);
        return await context.Events.AsNoTracking().Where(predicate)
            .OrderBy(e => e.Id).Select(e => e.Id).ToListAsync();
    }

    // ---------------------------------------------------------------- components

    [Fact]
    public async Task Year_translates_to_toYear()
        => Assert.Equal(2026, await SelectSingleAsync(q => q.Select(e => e.Timestamp64.Year)));

    [Fact]
    public async Task Month_translates_to_toMonth()
        => Assert.Equal(8, await SelectSingleAsync(q => q.Select(e => e.Timestamp64.Month)));

    [Fact]
    public async Task Day_translates_to_toDayOfMonth()
        => Assert.Equal(16, await SelectSingleAsync(q => q.Select(e => e.Timestamp64.Day)));

    [Fact]
    public async Task Hour_translates_to_toHour()
        => Assert.Equal(13, await SelectSingleAsync(q => q.Select(e => e.Timestamp64.Hour)));

    [Fact]
    public async Task Minute_translates_to_toMinute()
        => Assert.Equal(47, await SelectSingleAsync(q => q.Select(e => e.Timestamp64.Minute)));

    [Fact]
    public async Task Second_translates_to_toSecond()
        => Assert.Equal(32, await SelectSingleAsync(q => q.Select(e => e.Timestamp64.Second)));

    [Fact]
    public async Task Millisecond_translates_to_toMillisecond()
        => Assert.Equal(123, await SelectSingleAsync(q => q.Select(e => e.Timestamp64.Millisecond)));

    [Fact]
    public async Task DayOfYear_translates_to_toDayOfYear()
        => Assert.Equal(228, await SelectSingleAsync(q => q.Select(e => e.Timestamp64.DayOfYear)));

    [Fact]
    public async Task Components_agree_with_dotnet_on_a_second_precision_column()
    {
        var result = await SelectSingleAsync(q => q.Select(e => new { e.Timestamp.Year, e.Timestamp.Hour, e.Timestamp.Second }));

        Assert.Equal(DateTimeMemberFixture.Instant.Year, result.Year);
        Assert.Equal(DateTimeMemberFixture.Instant.Hour, result.Hour);
        Assert.Equal(DateTimeMemberFixture.Instant.Second, result.Second);
    }

    // ---------------------------------------------------------------- DayOfWeek

    [Fact]
    public async Task DayOfWeek_projects_the_dotnet_value()
    {
        var result = await SelectSingleAsync(q => q.Select(e => e.Timestamp64.DayOfWeek));

        // .NET DayOfWeek.Sunday is 0. ClickHouse mode 2 agrees; the default mode would give 7.
        Assert.Equal(DayOfWeek.Sunday, result);
    }

    [Fact]
    public async Task DayOfWeek_compares_against_a_dotnet_constant()
    {
        // The provider maps a C# enum to a ClickHouse string, so this is the test that proves the
        // constant renders as a number rather than as 'Sunday'.
        Assert.Equal([1L], await WhereIdsAsync(e => e.Timestamp64.DayOfWeek == DayOfWeek.Sunday));
    }

    [Fact]
    public async Task DayOfWeek_of_a_Saturday_is_six()
    {
        // Row 2 is 2026-01-31, a Saturday.
        Assert.Equal(DayOfWeek.Saturday, await SelectSingleAsync(q => q.Select(e => e.Timestamp64.DayOfWeek), id: 2));
    }

    // ---------------------------------------------------------------- Date / TimeOfDay

    [Fact]
    public async Task Date_translates_to_toStartOfDay()
        => Assert.Equal(new DateTime(2026, 8, 16), await SelectSingleAsync(q => q.Select(e => e.Timestamp64.Date)));

    [Fact]
    public async Task TimeOfDay_keeps_tick_precision()
    {
        var result = await SelectSingleAsync(q => q.Select(e => e.Timestamp64.TimeOfDay));

        // toTime would drop the fraction; toTime64(x, 7) keeps every tick.
        Assert.Equal(DateTimeMemberFixture.InstantTimeOfDay, result);
    }

    // ---------------------------------------------------------------- DateOnly

    [Fact]
    public async Task DateOnly_components_translate()
    {
        var result = await SelectSingleAsync(q => q.Select(e => new
        {
            e.Date.Year,
            e.Date.Month,
            e.Date.Day,
            e.Date.DayOfYear,
            e.Date.DayOfWeek
        }));

        Assert.Equal(2026, result.Year);
        Assert.Equal(8, result.Month);
        Assert.Equal(16, result.Day);
        Assert.Equal(228, result.DayOfYear);
        Assert.Equal(DayOfWeek.Sunday, result.DayOfWeek);
    }

    // ---------------------------------------------------------------- DateTimeOffset

    [Fact]
    public async Task DateTimeOffset_components_translate()
    {
        var result = await SelectSingleAsync(q => q.Select(e => new
        {
            e.Offset.Year,
            e.Offset.Month,
            e.Offset.Day,
            e.Offset.Hour,
            e.Offset.Minute,
            e.Offset.Second,
            e.Offset.DayOfYear,
            e.Offset.DayOfWeek
        }));

        // The store type is UTC-pinned, and a value read back carries +00:00, so every component
        // describes the same instant on both sides.
        Assert.Equal(2026, result.Year);
        Assert.Equal(8, result.Month);
        Assert.Equal(16, result.Day);
        Assert.Equal(13, result.Hour);
        Assert.Equal(47, result.Minute);
        Assert.Equal(32, result.Second);
        Assert.Equal(228, result.DayOfYear);
        Assert.Equal(DayOfWeek.Sunday, result.DayOfWeek);
    }

    [Fact]
    public async Task DateTimeOffset_components_agree_with_dotnet()
    {
        await using var context = new DateTimeMemberDbContext(_fixture.ConnectionString);

        var expected = await context.Events.AsNoTracking().Where(e => e.Id == 1)
            .Select(e => e.Offset).SingleAsync();
        var actual = await SelectSingleAsync(q => q.Select(e => new { e.Offset.Year, e.Offset.Hour, e.Offset.Second }));

        Assert.Equal(expected.Year, actual.Year);
        Assert.Equal(expected.Hour, actual.Hour);
        Assert.Equal(expected.Second, actual.Second);
    }

    [Fact]
    public async Task DateTimeOffset_Date_translates()
        => Assert.Equal(
            new DateTime(2026, 8, 16),
            await SelectSingleAsync(q => q.Select(e => e.Offset.Date)));

    [Fact]
    public async Task DateTimeOffset_TimeOfDay_keeps_tick_precision()
        => Assert.Equal(
            DateTimeMemberFixture.InstantTimeOfDay,
            await SelectSingleAsync(q => q.Select(e => e.Offset.TimeOfDay)));

    [Fact]
    public async Task DateTimeOffset_AddDays_translates()
    {
        var result = await SelectSingleAsync(q => q.Select(e => e.Offset.AddDays(1)));

        Assert.Equal(new DateTimeOffset(2026, 8, 17, 13, 47, 32, TimeSpan.Zero).AddTicks(1_234_567), result);
    }

    [Fact]
    public async Task DateTimeOffset_AddMonths_clamps_like_dotnet()
        => Assert.Equal(
            new DateTimeOffset(2026, 2, 28, 0, 0, 0, TimeSpan.Zero),
            await SelectSingleAsync(q => q.Select(e => e.Offset.AddMonths(1)), id: 2));

    [Fact]
    public async Task DateTimeOffset_DayOfWeek_compares_against_a_dotnet_constant()
        => Assert.Equal([1L], await WhereIdsAsync(e => e.Offset.DayOfWeek == DayOfWeek.Sunday));

    [Fact]
    public async Task DateTimeOffset_UtcNow_runs_on_the_server()
    {
        await using var context = new DateTimeMemberDbContext(_fixture.ConnectionString);
        var query = context.Events.AsNoTracking()
            .Where(e => e.Offset < DateTimeOffset.UtcNow.AddYears(100))
            .Select(e => e.Id);

        Assert.Contains("now64", query.ToQueryString());
        Assert.Equal([1L, 2L], await query.OrderBy(id => id).ToListAsync());
    }

    // ---------------------------------------------------------------- Add*

    [Fact]
    public async Task AddYears_translates_to_addYears()
        => Assert.Equal(
            DateTimeMemberFixture.Instant.AddYears(1),
            await SelectSingleAsync(q => q.Select(e => e.Timestamp64.AddYears(1))));

    [Fact]
    public async Task AddMonths_translates_to_addMonths()
        => Assert.Equal(
            DateTimeMemberFixture.Instant.AddMonths(1),
            await SelectSingleAsync(q => q.Select(e => e.Timestamp64.AddMonths(1))));

    [Fact]
    public async Task AddMonths_clamps_the_day_like_dotnet()
    {
        // Row 2 is 2026-01-31, so one month lands on 2026-02-28 in both systems.
        var result = await SelectSingleAsync(q => q.Select(e => e.Timestamp64.AddMonths(1)), id: 2);

        Assert.Equal(new DateTime(2026, 2, 28), result);
        Assert.Equal(new DateTime(2026, 1, 31).AddMonths(1), result);
    }

    [Fact]
    public async Task AddDays_with_a_whole_number_translates_to_addDays()
        => Assert.Equal(
            DateTimeMemberFixture.Instant.AddDays(1),
            await SelectSingleAsync(q => q.Select(e => e.Timestamp64.AddDays(1))));

    [Fact]
    public async Task AddDays_with_a_negative_whole_number_translates_to_addDays()
        => Assert.Equal(
            DateTimeMemberFixture.Instant.AddDays(-1),
            await SelectSingleAsync(q => q.Select(e => e.Timestamp64.AddDays(-1))));

    [Fact]
    public async Task AddDays_with_a_fraction_keeps_dotnet_semantics()
    {
        // addDays(x, 1.5) would discard the fraction and add one day. The translation must not.
        var result = await SelectSingleAsync(q => q.Select(e => e.Timestamp64.AddDays(1.5)));

        Assert.Equal(DateTimeMemberFixture.Instant.AddDays(1.5), result);
        Assert.Equal(new DateTime(2026, 8, 18, 1, 47, 32).AddTicks(1_234_567), result);
    }

    [Fact]
    public async Task AddHours_translates()
        => Assert.Equal(
            DateTimeMemberFixture.Instant.AddHours(2),
            await SelectSingleAsync(q => q.Select(e => e.Timestamp64.AddHours(2))));

    [Fact]
    public async Task AddMinutes_with_a_fraction_keeps_dotnet_semantics()
        => Assert.Equal(
            DateTimeMemberFixture.Instant.AddMinutes(0.5),
            await SelectSingleAsync(q => q.Select(e => e.Timestamp64.AddMinutes(0.5))));

    [Fact]
    public async Task AddSeconds_with_a_fraction_keeps_dotnet_semantics()
        => Assert.Equal(
            DateTimeMemberFixture.Instant.AddSeconds(1.5),
            await SelectSingleAsync(q => q.Select(e => e.Timestamp64.AddSeconds(1.5))));

    [Fact]
    public async Task AddMilliseconds_translates()
        => Assert.Equal(
            DateTimeMemberFixture.Instant.AddMilliseconds(1),
            await SelectSingleAsync(q => q.Select(e => e.Timestamp64.AddMilliseconds(1))));

    [Fact]
    public async Task AddDays_on_a_Date32_column_keeps_the_date_store_type()
    {
        // DateOnly.AddDays takes an int, so this must use addDays — addMilliseconds rejects a Date32.
        var result = await SelectSingleAsync(q => q.Select(e => e.Date.AddDays(1)));

        Assert.Equal(new DateOnly(2026, 8, 17), result);
    }

    [Fact]
    public async Task AddMonths_on_a_Date32_column_clamps_like_dotnet()
        => Assert.Equal(
            new DateOnly(2026, 2, 28),
            await SelectSingleAsync(q => q.Select(e => e.Date.AddMonths(1)), id: 2));

    [Fact]
    public async Task AddSeconds_below_millisecond_resolution_keeps_dotnet_semantics()
    {
        await using var context = new DateTimeMemberDbContext(_fixture.ConnectionString);
        var query = context.Events.AsNoTracking().Where(e => e.Id == 1)
            .Select(e => e.Timestamp64.AddSeconds(0.1234567));

        // .NET adds exactly 1 234 567 ticks. No ClickHouse unit holds that without promoting the result
        // to DateTime64(9), so the call is left untranslated and the client supplies the exact value.
        Assert.DoesNotContain("addSeconds", query.ToQueryString());
        Assert.DoesNotContain("addMilliseconds", query.ToQueryString());

        var result = await query.SingleAsync();
        Assert.Equal(DateTimeMemberFixture.Instant.AddSeconds(0.1234567), result);
        Assert.Equal(DateTimeMemberFixture.Instant.AddTicks(1_234_567), result);
    }

    [Fact]
    public async Task AddMilliseconds_below_millisecond_resolution_keeps_dotnet_semantics()
    {
        // .NET rounds to the nearest tick, so this adds 5 000 ticks — not 0 ms and not 1 ms.
        var result = await SelectSingleAsync(q => q.Select(e => e.Timestamp64.AddMilliseconds(0.5)));

        Assert.Equal(DateTimeMemberFixture.Instant.AddMilliseconds(0.5), result);
        Assert.Equal(DateTimeMemberFixture.Instant.AddTicks(5_000), result);
    }

    [Fact]
    public async Task AddDays_with_a_parameter_keeps_dotnet_semantics()
    {
        var days = 1.5;

        await using var context = new DateTimeMemberDbContext(_fixture.ConnectionString);
        var query = context.Events.AsNoTracking().Where(e => e.Id == 1)
            .Select(e => e.Timestamp64.AddDays(days));

        // A parameter cannot be checked for exactness, so it is not translated. Rounding it on the server
        // would disagree with .NET, because ClickHouse round() is banker's rounding.
        Assert.DoesNotContain("addDays", query.ToQueryString());
        Assert.Equal(DateTimeMemberFixture.Instant.AddDays(1.5), await query.SingleAsync());
    }

    [Fact]
    public async Task AddDays_with_a_parameter_in_a_predicate_reports_a_reason()
    {
        var days = 1.5;

        await using var context = new DateTimeMemberDbContext(_fixture.ConnectionString);
        var query = context.Events.AsNoTracking().Where(e => e.Timestamp64.AddDays(days) > e.Timestamp);

        await Assert.ThrowsAsync<InvalidOperationException>(() => query.ToListAsync());
    }

    [Fact]
    public async Task AddDays_beyond_the_dotnet_range_throws_the_dotnet_exception()
    {
        await using var context = new DateTimeMemberDbContext(_fixture.ConnectionString);
        var query = context.Events.AsNoTracking().Where(e => e.Id == 1)
            .Select(e => e.Timestamp64.AddDays(1e30));

        // Folding this would wrap to Int64.MaxValue and give a server decimal-overflow error, or worse a
        // silently wrong date. Left untranslated, .NET raises its own exception on the client.
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => query.SingleAsync());
    }

    [Fact]
    public async Task Add_composes_with_a_component_member()
        => Assert.Equal(2027, await SelectSingleAsync(q => q.Select(e => e.Timestamp64.AddYears(1).Year)));

    // ---------------------------------------------------------------- server clock

    [Fact]
    public async Task UtcNow_runs_on_the_server()
    {
        await using var context = new DateTimeMemberDbContext(_fixture.ConnectionString);

        // Both bounds are offset by a century so the outcome does not depend on the day the suite runs.
        var query = context.Events.AsNoTracking().Where(e => e.Timestamp64 < DateTime.UtcNow.AddYears(100))
            .Select(e => e.Id);

        // If EF Core evaluated DateTime.UtcNow on the client, the SQL would carry a literal instead.
        Assert.Contains("now64", query.ToQueryString());
        Assert.Equal([1L, 2L], await query.OrderBy(id => id).ToListAsync());

        var none = await context.Events.AsNoTracking()
            .Where(e => e.Timestamp64 < DateTime.UtcNow.AddYears(-100))
            .Select(e => e.Id).ToListAsync();

        Assert.Empty(none);
    }

    [Fact]
    public async Task Today_runs_on_the_server()
    {
        await using var context = new DateTimeMemberDbContext(_fixture.ConnectionString);
        var query = context.Events.AsNoTracking().Where(e => e.Timestamp64 < DateTime.Today)
            .Select(e => e.Id);

        Assert.Contains("toStartOfDay", query.ToQueryString());
        Assert.Contains("now()", query.ToQueryString());

        // Execute it too: a SQL-shape assertion alone would not catch the server rejecting the call.
        var beforeToday = await query.ToListAsync();

        // Row 2 is 2026-01-31, which is before today on any day this suite can run.
        Assert.Contains(2L, beforeToday);
    }

    // ---------------------------------------------------------------- subtraction

    [Fact]
    public async Task Subtracting_two_date_times_in_a_projection_now_works()
    {
        await using var context = new DateTimeMemberDbContext(_fixture.ConnectionString);

        // Previously this failed with an opaque cast/coercion error from type-mapping inference. The
        // subtraction is now reported as not translatable, so EF Core reads both columns and subtracts
        // on the client, which is the correct .NET result.
        var result = await context.Events.AsNoTracking().Where(e => e.Id == 1)
            .Select(e => e.Timestamp64 - e.Timestamp).SingleAsync();

        Assert.Equal(TimeSpan.FromTicks(1_234_567), result);
    }

    [Fact]
    public async Task Subtracting_two_date_times_in_a_predicate_reports_a_clear_reason()
    {
        await using var context = new DateTimeMemberDbContext(_fixture.ConnectionString);

        // A predicate cannot fall back to the client, so this is where the reason must surface.
        var query = context.Events.AsNoTracking()
            .Where(e => e.Timestamp64 - e.Timestamp > TimeSpan.Zero);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => query.ToListAsync());

        Assert.Contains("Arithmetic on two date or time values", exception.Message);
    }

    [Fact]
    public async Task Subtracting_two_times_of_day_in_a_projection_works()
    {
        await using var context = new DateTimeMemberDbContext(_fixture.ConnectionString);

        // ClickHouse Time64 subtraction gives a Decimal of seconds, not a TimeSpan, so this must stay
        // on the client rather than emit SQL that materializes into the wrong type.
        var result = await context.Events.AsNoTracking().Where(e => e.Id == 1)
            .Select(e => e.Timestamp64.TimeOfDay - e.Timestamp.TimeOfDay).SingleAsync();

        Assert.Equal(TimeSpan.FromTicks(1_234_567), result);
    }

    [Fact]
    public async Task Adding_a_time_of_day_to_a_date_in_a_projection_works()
    {
        await using var context = new DateTimeMemberDbContext(_fixture.ConnectionString);

        // ClickHouse rejects DateTime + Time64 outright, so this must stay on the client too.
        var result = await context.Events.AsNoTracking().Where(e => e.Id == 1)
            .Select(e => e.Timestamp64.Date + e.Timestamp64.TimeOfDay).SingleAsync();

        Assert.Equal(DateTimeMemberFixture.Instant, result);
    }

    [Fact]
    public async Task Adding_a_TimeSpan_to_a_date_in_a_projection_works()
    {
        await using var context = new DateTimeMemberDbContext(_fixture.ConnectionString);

        var result = await context.Events.AsNoTracking().Where(e => e.Id == 1)
            .Select(e => e.Timestamp64 + TimeSpan.FromHours(1)).SingleAsync();

        Assert.Equal(DateTimeMemberFixture.Instant.AddHours(1), result);
    }
}

public class DateTimeMemberTranslationOfflineTest
{
    private sealed class OfflineContext : DbContext
    {
        public DbSet<DateTimeMemberEntity> Events => Set<DateTimeMemberEntity>();

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseClickHouse("Host=localhost;Protocol=http;Port=8123;Database=test");
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<DateTimeMemberEntity>(entity =>
            {
                entity.ToTable("datetime_member_test");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Timestamp64).HasColumnType("DateTime64(7)");
            });
        }
    }

    private static string Sql<T>(Func<IQueryable<DateTimeMemberEntity>, IQueryable<T>> selector)
    {
        using var context = new OfflineContext();
        return selector(context.Events).ToQueryString();
    }

    [Fact]
    public void Year_emits_toYear()
        => Assert.Contains("toYear(", Sql(q => q.Select(e => e.Timestamp64.Year)));

    [Fact]
    public void Day_emits_toDayOfMonth()
        => Assert.Contains("toDayOfMonth(", Sql(q => q.Select(e => e.Timestamp64.Day)));

    [Fact]
    public void DayOfWeek_emits_week_mode_two()
    {
        var sql = Sql(q => q.Select(e => e.Timestamp64.DayOfWeek));

        // Assert the mode argument itself — a bare "2" would also match toDayOfWeek(x, 12).
        Assert.Contains(", 2)", sql);
        Assert.Contains("toDayOfWeek(", sql);
    }

    [Fact]
    public void DayOfWeek_comparison_emits_a_number_not_a_string()
    {
        var sql = Sql(q => q.Where(e => e.Timestamp64.DayOfWeek == DayOfWeek.Sunday).Select(e => e.Id));

        Assert.DoesNotContain("'Sunday'", sql);
        Assert.Contains("= 0", sql);
    }

    [Fact]
    public void TimeOfDay_emits_toTime64_with_tick_precision()
        => Assert.Contains("toTime64(", Sql(q => q.Select(e => e.Timestamp64.TimeOfDay)));

    [Fact]
    public void AddDays_with_a_whole_number_emits_addDays()
    {
        var sql = Sql(q => q.Select(e => e.Timestamp64.AddDays(1)));

        Assert.Contains("addDays(", sql);
        Assert.DoesNotContain("addMilliseconds", sql);
    }

    [Fact]
    public void AddDays_with_a_fraction_emits_addMilliseconds()
    {
        var sql = Sql(q => q.Select(e => e.Timestamp64.AddDays(1.5)));

        // 1.5 days is exactly 129 600 000 ms, folded at translation time.
        Assert.Contains("addMilliseconds(", sql);
        Assert.Contains("129600000", sql);
    }

    [Fact]
    public void AddMilliseconds_below_millisecond_resolution_emits_no_add_function()
    {
        // 0.5 ms is 5 000 ticks, which no ClickHouse unit holds exactly without promoting to
        // DateTime64(9), so the call must not be translated.
        var sql = Sql(q => q.Select(e => e.Timestamp64.AddMilliseconds(0.5)));

        Assert.DoesNotContain("addMilliseconds", sql);
        Assert.DoesNotContain("addNanoseconds", sql);
    }

    [Fact]
    public void AddSeconds_with_a_whole_number_of_milliseconds_emits_addMilliseconds()
    {
        // 1.5 s is 1 500 ms exactly, so it is still translatable — just not in seconds.
        var sql = Sql(q => q.Select(e => e.Timestamp64.AddSeconds(1.5)));

        Assert.Contains("addMilliseconds(", sql);
        Assert.Contains("1500", sql);
    }

    [Fact]
    public void AddHours_with_a_whole_number_emits_addHours()
        => Assert.Contains("addHours(", Sql(q => q.Select(e => e.Timestamp64.AddHours(3))));

    [Fact]
    public void UtcNow_emits_a_utc_pinned_now64()
        => Assert.Contains("now64(7, 'UTC')", Sql(q => q.Where(e => e.Timestamp64 < DateTime.UtcNow).Select(e => e.Id)));

    [Fact]
    public void Now_emits_a_timezone_less_now64()
    {
        var sql = Sql(q => q.Where(e => e.Timestamp64 < DateTime.Now).Select(e => e.Id));

        Assert.Contains("now64(7)", sql);
        Assert.DoesNotContain("'UTC'", sql);
    }
}
