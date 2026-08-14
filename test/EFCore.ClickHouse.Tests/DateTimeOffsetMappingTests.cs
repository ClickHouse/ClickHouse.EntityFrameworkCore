using ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Xunit;

namespace EFCore.ClickHouse.Tests;

public class DateTimeOffsetEntity
{
    public long Id { get; set; }
    public DateTimeOffset Default { get; set; }
    public DateTimeOffset? Nullable { get; set; }
    public double Value { get; set; }
}

/// <summary>Points DateTimeOffset properties at columns with differing declared timezones.</summary>
public class DateTimeOffsetTzEntity
{
    public long Id { get; set; }
    public DateTimeOffset Utc { get; set; }
    public DateTimeOffset Naive { get; set; }
    public DateTimeOffset Tokyo { get; set; }
    public DateTimeOffset Seconds { get; set; }
}

/// <summary>A zone with daylight saving, for the ambiguous-wall-clock case.</summary>
public class DateTimeOffsetDstEntity
{
    public long Id { get; set; }
    public DateTimeOffset London { get; set; }
}

/// <summary>Precision set through <c>HasPrecision</c> rather than <c>HasColumnType</c>.</summary>
public class DateTimeOffsetPrecisionEntity
{
    public long Id { get; set; }
    public DateTimeOffset Millis { get; set; }
}

/// <summary>
/// Columns that declare a fixed UTC offset instead of a named zone. ClickHouse spells these
/// <c>Fixed/UTC±HH:MM:SS</c>, and .NET has no timezone of that name.
/// </summary>
public class DateTimeOffsetFixedEntity
{
    public long Id { get; set; }
    public DateTimeOffset Half { get; set; }
    public DateTimeOffset Negative { get; set; }
    public DateTimeOffset Quarter { get; set; }
    public DateTimeOffset Zero { get; set; }
    public DateTimeOffset Seconds { get; set; }
}

/// <summary>
/// A column whose fixed-offset name carries minutes above 59. ClickHouse accepts the name, the
/// driver cannot read it, so the read must report that rather than guess an offset.
/// </summary>
public class DateTimeOffsetCarriedEntity
{
    public long Id { get; set; }
    public DateTimeOffset Carried { get; set; }
}

public class DateTimeOffsetDbContext : DbContext
{
    private readonly string _connectionString;

    public DateTimeOffsetDbContext(string connectionString) => _connectionString = connectionString;

    public DbSet<DateTimeOffsetEntity> Entities => Set<DateTimeOffsetEntity>();
    public DbSet<DateTimeOffsetTzEntity> TzEntities => Set<DateTimeOffsetTzEntity>();
    public DbSet<DateTimeOffsetDstEntity> DstEntities => Set<DateTimeOffsetDstEntity>();
    public DbSet<DateTimeOffsetPrecisionEntity> PrecisionEntities => Set<DateTimeOffsetPrecisionEntity>();
    public DbSet<DateTimeOffsetFixedEntity> FixedEntities => Set<DateTimeOffsetFixedEntity>();
    public DbSet<DateTimeOffsetCarriedEntity> CarriedEntities => Set<DateTimeOffsetCarriedEntity>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseClickHouse(_connectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<DateTimeOffsetEntity>(e =>
        {
            e.ToTable("dto_default");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.Default).HasColumnName("dt");
            e.Property(x => x.Nullable).HasColumnName("dt_null");
            e.Property(x => x.Value).HasColumnName("value");
        });

        modelBuilder.Entity<DateTimeOffsetTzEntity>(e =>
        {
            e.ToTable("dto_tz");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.Utc).HasColumnName("utc").HasColumnType("DateTime64(6, 'UTC')");
            // A timezone-less column only round trips here because the test container's
            // session_timezone is UTC. This is the hazard the default UTC pin avoids.
            e.Property(x => x.Naive).HasColumnName("naive").HasColumnType("DateTime64(6)");
            e.Property(x => x.Tokyo).HasColumnName("tokyo").HasColumnType("DateTime64(6, 'Asia/Tokyo')");
            e.Property(x => x.Seconds).HasColumnName("seconds").HasColumnType("DateTime('UTC')");
        });

        modelBuilder.Entity<DateTimeOffsetDstEntity>(e =>
        {
            e.ToTable("dto_dst");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.London).HasColumnName("london").HasColumnType("DateTime64(7, 'Europe/London')");
        });

        modelBuilder.Entity<DateTimeOffsetPrecisionEntity>(e =>
        {
            e.ToTable("dto_precision");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            // Precision only — no HasColumnType, so the UTC pin must be kept.
            e.Property(x => x.Millis).HasColumnName("millis").HasPrecision(3);
        });

        modelBuilder.Entity<DateTimeOffsetFixedEntity>(e =>
        {
            e.ToTable("dto_fixed");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.Half).HasColumnName("half")
                .HasColumnType("DateTime64(7, 'Fixed/UTC+05:30:00')");
            e.Property(x => x.Negative).HasColumnName("negative")
                .HasColumnType("DateTime64(7, 'Fixed/UTC-07:00:00')");
            // A quarter-hour offset, which no whole-hour shortcut would handle.
            e.Property(x => x.Quarter).HasColumnName("quarter")
                .HasColumnType("DateTime64(7, 'Fixed/UTC+05:45:00')");
            // Offset zero: the driver reports Kind=Utc here rather than a wall clock.
            e.Property(x => x.Zero).HasColumnName("zero")
                .HasColumnType("DateTime64(7, 'Fixed/UTC+00:00:00')");
            // A whole-minute offset written with the seconds field the name always carries.
            e.Property(x => x.Seconds).HasColumnName("seconds")
                .HasColumnType("DateTime64(7, 'Fixed/UTC+00:01:00')");
        });

        modelBuilder.Entity<DateTimeOffsetCarriedEntity>(e =>
        {
            e.ToTable("dto_carried");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            // ClickHouse reads this as +06:00. The driver does not read it at all.
            e.Property(x => x.Carried).HasColumnName("carried")
                .HasColumnType("DateTime64(7, 'Fixed/UTC+05:60:00')");
        });
    }
}

public class StringDateTimeOffsetEntity
{
    public long Id { get; set; }
    public DateTimeOffset ViaConversion { get; set; }
    public DateTimeOffset ViaColumnType { get; set; }
}

public class StringDateTimeOffsetDbContext : DbContext
{
    private readonly string _connectionString;

    public StringDateTimeOffsetDbContext(string connectionString) => _connectionString = connectionString;

    public DbSet<StringDateTimeOffsetEntity> Entities => Set<StringDateTimeOffsetEntity>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseClickHouse(_connectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<StringDateTimeOffsetEntity>(e =>
        {
            e.ToTable("dto_string");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.ViaConversion).HasColumnName("via_conversion").HasConversion<string>();
            e.Property(x => x.ViaColumnType).HasColumnName("via_column_type").HasColumnType("String");
        });
    }
}

public class DateTimeOffsetMappingFixture : IAsyncLifetime
{
    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        ConnectionString = await SharedContainer.GetConnectionStringAsync();
        using var ctx = new DateTimeOffsetDbContext(ConnectionString);
        await ctx.Database.EnsureCreatedAsync();
    }

    public Task DisposeAsync() => Task.CompletedTask;
}

public class DateTimeOffsetMappingTests : IClassFixture<DateTimeOffsetMappingFixture>
{
    private readonly DateTimeOffsetMappingFixture _fixture;

    public DateTimeOffsetMappingTests(DateTimeOffsetMappingFixture fixture) => _fixture = fixture;

    private static long ToMicros(DateTimeOffset value) => value.ToUnixTimeMilliseconds() * 1000L;

    // --- mapping resolution -------------------------------------------------

    [Fact]
    public void Default_mapping_is_utc_pinned_datetime64()
    {
        using var ctx = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        var property = ctx.Model
            .FindEntityType(typeof(DateTimeOffsetEntity))!
            .FindProperty(nameof(DateTimeOffsetEntity.Default))!;

        var mapping = property.GetRelationalTypeMapping();

        Assert.IsType<ClickHouseDateTimeOffsetTypeMapping>(mapping);
        Assert.Equal(typeof(DateTimeOffset), mapping.ClrType);
        Assert.Equal("DateTime64(7, 'UTC')", mapping.StoreType);
        // The old behaviour resolved a String column through DateTimeOffsetToStringConverter.
        Assert.Null(mapping.Converter);
    }

    [Fact]
    public void Nullable_property_resolves_the_same_mapping()
    {
        using var ctx = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        var property = ctx.Model
            .FindEntityType(typeof(DateTimeOffsetEntity))!
            .FindProperty(nameof(DateTimeOffsetEntity.Nullable))!;

        var mapping = property.GetRelationalTypeMapping();

        Assert.IsType<ClickHouseDateTimeOffsetTypeMapping>(mapping);
        Assert.Equal("DateTime64(7, 'UTC')", mapping.StoreType);
    }

    [Theory]
    [InlineData(nameof(DateTimeOffsetTzEntity.Utc), "DateTime64(6, 'UTC')")]
    [InlineData(nameof(DateTimeOffsetTzEntity.Naive), "DateTime64(6)")]
    [InlineData(nameof(DateTimeOffsetTzEntity.Tokyo), "DateTime64(6, 'Asia/Tokyo')")]
    [InlineData(nameof(DateTimeOffsetTzEntity.Seconds), "DateTime('UTC')")]
    public void Explicit_store_type_is_preserved(string propertyName, string expectedStoreType)
    {
        using var ctx = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        var property = ctx.Model
            .FindEntityType(typeof(DateTimeOffsetTzEntity))!
            .FindProperty(propertyName)!;

        var mapping = property.GetRelationalTypeMapping();

        Assert.IsType<ClickHouseDateTimeOffsetTypeMapping>(mapping);
        Assert.Equal(typeof(DateTimeOffset), mapping.ClrType);
        Assert.Equal(expectedStoreType, property.GetColumnType());
    }

    [Fact]
    public async Task EnsureCreated_makes_a_datetime64_column_not_a_string()
    {
        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "DESCRIBE TABLE dto_default";

        var columns = new Dictionary<string, string>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            columns[(string)reader.GetValue(0)] = (string)reader.GetValue(1);

        Assert.Equal("DateTime64(7, 'UTC')", columns["dt"]);
        Assert.Equal("Nullable(DateTime64(7, 'UTC'))", columns["dt_null"]);
    }

    /// <summary>
    /// <c>HasPrecision(n)</c> with no <c>HasColumnType</c> must change the precision and keep the
    /// UTC pin. Dropping the facet would silently give the default precision 7 instead.
    /// </summary>
    [Fact]
    public void HasPrecision_is_honoured_and_keeps_the_utc_pin()
    {
        using var ctx = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        var property = ctx.Model
            .FindEntityType(typeof(DateTimeOffsetPrecisionEntity))!
            .FindProperty(nameof(DateTimeOffsetPrecisionEntity.Millis))!;

        var mapping = property.GetRelationalTypeMapping();

        Assert.Equal("DateTime64(3, 'UTC')", mapping.StoreType);
        Assert.Equal("DateTime64(3, 'UTC')", property.GetColumnType());
        Assert.Equal(3, mapping.Precision);
    }

    /// <summary>A bare <c>DateTime64</c> with no argument is precision 3 in ClickHouse, not 7.</summary>
    [Fact]
    public void Bare_datetime64_store_type_uses_clickhouse_default_precision()
    {
        using var ctx = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        var source = ctx.GetService<Microsoft.EntityFrameworkCore.Storage.IRelationalTypeMappingSource>();

        var mapping = source.FindMapping(typeof(DateTimeOffset), "DateTime64");

        Assert.Equal(3, mapping!.Precision);
    }

    [Fact]
    public void Bare_datetime_store_type_maps_to_seconds_precision()
    {
        var mapping = new ClickHouseDateTimeOffsetTypeMapping(precision: null, timezone: null);

        Assert.Equal("DateTime", mapping.StoreType);
        Assert.Null(mapping.Precision);
    }

    /// <summary>
    /// When the configured text differs from the mapping's canonical store type,
    /// <c>PreserveExplicitStoreType</c> clones the mapping to keep that text. The clone must carry
    /// <c>Timezone</c> over, or the read path would lose the offset of the declared zone.
    /// </summary>
    [Theory]
    [InlineData("DateTime64(6,'Asia/Tokyo')", "Asia/Tokyo")]
    [InlineData("Nullable(DateTime64(6, 'Asia/Tokyo'))", "Asia/Tokyo")]
    [InlineData("LowCardinality(DateTime64(6, 'Asia/Tokyo'))", "Asia/Tokyo")]
    [InlineData("datetime64(6, 'Asia/Tokyo')", "Asia/Tokyo")]
    public void Clone_for_a_preserved_store_type_keeps_the_timezone(string columnType, string expectedTimezone)
    {
        using var ctx = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        var source = ctx.GetService<Microsoft.EntityFrameworkCore.Storage.IRelationalTypeMappingSource>();

        var mapping = source.FindMapping(typeof(DateTimeOffset), columnType);

        var typed = Assert.IsType<ClickHouseDateTimeOffsetTypeMapping>(mapping);
        Assert.Equal(typeof(DateTimeOffset), typed.ClrType);
        // The user's text survives verbatim...
        Assert.Equal(columnType, typed.StoreType);
        // ...and the timezone needed by the read path survives the clone.
        Assert.Equal(expectedTimezone, typed.Timezone);
    }

    // --- SQL literals -------------------------------------------------------

    [Theory]
    [InlineData(7, "UTC", "DateTime64(7, 'UTC')", "'2026-01-15 10:00:00.1234567+05:00'")]
    [InlineData(6, "UTC", "DateTime64(6, 'UTC')", "'2026-01-15 10:00:00.123456+05:00'")]
    [InlineData(3, null, "DateTime64(3)", "'2026-01-15 10:00:00.123+05:00'")]
    [InlineData(0, "UTC", "DateTime64(0, 'UTC')", "'2026-01-15 10:00:00+05:00'")]
    public void Literal_carries_the_offset(int precision, string? timezone, string expectedStoreType, string expectedLiteral)
    {
        var mapping = new ClickHouseDateTimeOffsetTypeMapping(precision, timezone);
        var value = new DateTimeOffset(2026, 1, 15, 10, 0, 0, TimeSpan.FromHours(5)).AddTicks(1234567);

        Assert.Equal(expectedStoreType, mapping.StoreType);
        Assert.Equal(expectedLiteral, mapping.GenerateSqlLiteral(value));
    }

    [Fact]
    public void Seconds_precision_literal_has_no_fractional_digits()
    {
        var mapping = new ClickHouseDateTimeOffsetTypeMapping(precision: null, timezone: "UTC");
        var value = new DateTimeOffset(2026, 1, 15, 10, 0, 0, TimeSpan.FromHours(5));

        Assert.Equal("DateTime('UTC')", mapping.StoreType);
        Assert.Equal("'2026-01-15 10:00:00+05:00'", mapping.GenerateSqlLiteral(value));
    }

    /// <summary>
    /// A literal that carries its offset must land on the same instant whatever timezone the
    /// target column declares. A bare wall clock does not, which is why the offset is emitted.
    /// </summary>
    [Theory]
    [InlineData("DateTime64(6)")]
    [InlineData("DateTime64(6, 'UTC')")]
    [InlineData("DateTime64(6, 'Asia/Tokyo')")]
    [InlineData("DateTime64(6, 'Fixed/UTC+05:30:00')")]
    [InlineData("DateTime")]
    [InlineData("DateTime('Asia/Tokyo')")]
    public async Task Literal_is_instant_exact_for_any_column_timezone(string columnType)
    {
        var mapping = new ClickHouseDateTimeOffsetTypeMapping(6, "UTC");
        var value = new DateTimeOffset(2026, 1, 15, 10, 0, 0, TimeSpan.FromHours(5));

        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText =
            $"SELECT toUnixTimestamp64Micro(toDateTime64(CAST({mapping.GenerateSqlLiteral(value)} AS {columnType}), 6))";

        Assert.Equal(ToMicros(value), Convert.ToInt64(await command.ExecuteScalarAsync()));
    }

    // --- round trip ---------------------------------------------------------

    [Fact]
    public async Task Insert_and_read_back_preserves_the_instant_at_tick_precision()
    {
        var value = new DateTimeOffset(2026, 1, 15, 10, 0, 0, TimeSpan.FromHours(5)).AddTicks(1234567);

        using var writeContext = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        writeContext.Entities.Add(new DateTimeOffsetEntity { Id = 1, Default = value, Nullable = value, Value = 1.5 });
        await writeContext.SaveChangesAsync();

        using var readContext = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        var row = await readContext.Entities.SingleAsync(e => e.Id == 1);

        // The instant survives exactly; the offset becomes the column's, which is UTC.
        Assert.Equal(value.ToUniversalTime(), row.Default);
        Assert.Equal(TimeSpan.Zero, row.Default.Offset);
        Assert.Equal(value.ToUniversalTime(), row.Nullable);
    }

    /// <summary>
    /// The pattern in issue #53 uses <c>DateTimeOffset.MinValue</c>/<c>MaxValue</c> as open-ended
    /// range sentinels, so both ends of the CLR range must survive. Precision 7 keeps this working:
    /// 100 ns units in an Int64 span about 29,000 years, which covers all of
    /// <see cref="DateTimeOffset"/>. A higher precision would not — <c>DateTime64(9)</c> overflows
    /// well before year 9999.
    /// </summary>
    [Fact]
    public async Task Min_and_max_values_round_trip_and_work_as_range_sentinels()
    {
        using var writeContext = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        writeContext.Entities.AddRange(
            new DateTimeOffsetEntity { Id = 40, Default = DateTimeOffset.MinValue, Value = 1 },
            new DateTimeOffsetEntity { Id = 41, Default = DateTimeOffset.MaxValue, Value = 2 });
        await writeContext.SaveChangesAsync();

        using var readContext = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        var min = await readContext.Entities.SingleAsync(e => e.Id == 40);
        var max = await readContext.Entities.SingleAsync(e => e.Id == 41);

        Assert.Equal(DateTimeOffset.MinValue, min.Default);
        Assert.Equal(DateTimeOffset.MaxValue, max.Default);

        // Used as sentinels, they must bracket everything.
        DateTimeOffset? startDate = null;
        DateTimeOffset? endDate = null;
        startDate ??= DateTimeOffset.MinValue;
        endDate ??= DateTimeOffset.MaxValue;

        var total = await readContext.Entities
            .Where(e => e.Id == 40 || e.Id == 41)
            .Where(e => e.Default >= startDate)
            .Where(e => e.Default <= endDate)
            .CountAsync();

        Assert.Equal(2, total);
    }

    /// <summary>
    /// The sentinel pattern above only works on a column whose timezone offset is zero. Both ends of
    /// the <see cref="DateTimeOffset"/> range sit at the edge of <see cref="DateTime"/>'s range, and
    /// the driver has to build a wall clock in the column's timezone to return one. Any non-zero
    /// offset pushes one end outside <see cref="DateTime"/>, and the driver throws while doing so —
    /// before the provider sees the value, so this cannot be reported any better from here.
    /// </summary>
    /// <remarks>
    /// This is not specific to a fixed offset; a named zone such as <c>Asia/Tokyo</c> behaves the
    /// same way. Reading the low end of the range from a named zone is worse than an error: zones
    /// carry a Local Mean Time offset for year 1 (<c>+09:18:59</c> for Tokyo), so the value comes
    /// back quietly shifted.
    /// </remarks>
    [Fact]
    public async Task Max_value_does_not_read_back_from_a_non_zero_offset_column()
    {
        using var writeContext = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        writeContext.TzEntities.Add(new DateTimeOffsetTzEntity
        {
            Id = 50,
            Utc = DateTimeOffset.MaxValue,
            Naive = DateTimeOffset.MaxValue,
            Tokyo = DateTimeOffset.MaxValue,
            Seconds = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)
        });

        // The write itself is fine: an instant needs no wall clock.
        await writeContext.SaveChangesAsync();

        using var readContext = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => readContext.TzEntities.SingleAsync(e => e.Id == 50));

        // Reading the same row through the zero-offset column alone is fine, which is why the
        // sentinel pattern still works on the default store type. (These columns are precision 6,
        // so the value truncates to microseconds; Min_and_max_values_round_trip_and_work_as_range
        // _sentinels covers the exact round trip on a precision 7 column.)
        using var projectingContext = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        var utcOnly = await projectingContext.TzEntities
            .Where(e => e.Id == 50)
            .Select(e => e.Utc)
            .SingleAsync();

        Assert.Equal(DateTimeOffset.MaxValue.ToUnixTimeMilliseconds(), utcOnly.ToUnixTimeMilliseconds());
    }

    [Fact]
    public async Task Null_round_trips()
    {
        using var writeContext = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        writeContext.Entities.Add(new DateTimeOffsetEntity
        {
            Id = 2,
            Default = new DateTimeOffset(2026, 2, 1, 0, 0, 0, TimeSpan.Zero),
            Nullable = null,
            Value = 1
        });
        await writeContext.SaveChangesAsync();

        using var readContext = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        var row = await readContext.Entities.SingleAsync(e => e.Id == 2);

        Assert.Null(row.Nullable);
    }

    /// <summary>
    /// Reading a column that declares a non-UTC timezone must still give the right instant. The
    /// driver returns a wall clock in the column's timezone, so the mapping attaches that offset.
    /// </summary>
    [Fact]
    public async Task Read_attaches_the_offset_of_the_declared_timezone()
    {
        var value = new DateTimeOffset(2026, 1, 15, 5, 0, 0, TimeSpan.Zero);

        using var writeContext = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        writeContext.TzEntities.Add(new DateTimeOffsetTzEntity
        {
            Id = 1,
            Utc = value,
            Naive = value,
            Tokyo = value,
            Seconds = value
        });
        await writeContext.SaveChangesAsync();

        using var readContext = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        var row = await readContext.TzEntities.SingleAsync(e => e.Id == 1);

        Assert.Equal(value, row.Utc);
        Assert.Equal(value, row.Naive);
        Assert.Equal(value, row.Tokyo);
        Assert.Equal(value, row.Seconds);

        // Same instant, rendered in the column's zone.
        Assert.Equal(TimeSpan.Zero, row.Utc.Offset);
        Assert.Equal(TimeSpan.FromHours(9), row.Tokyo.Offset);
    }

    // --- queries ------------------------------------------------------------

    /// <summary>Reproduces issue #53 directly: a range filter plus an aggregate.</summary>
    [Fact]
    public async Task Range_filter_with_parameters_and_aggregate()
    {
        using var seedContext = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        seedContext.Entities.AddRange(
            new DateTimeOffsetEntity { Id = 10, Default = new DateTimeOffset(2026, 1, 15, 0, 0, 0, TimeSpan.Zero), Value = 1.5 },
            new DateTimeOffsetEntity { Id = 11, Default = new DateTimeOffset(2026, 2, 15, 0, 0, 0, TimeSpan.Zero), Value = 2.5 },
            new DateTimeOffsetEntity { Id = 12, Default = new DateTimeOffset(2026, 5, 15, 0, 0, 0, TimeSpan.Zero), Value = 4.0 });
        await seedContext.SaveChangesAsync();

        using var ctx = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        DateTimeOffset? startDate = null;
        DateTimeOffset? endDate = null;
        startDate ??= new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        endDate ??= new DateTimeOffset(2026, 3, 1, 0, 0, 0, TimeSpan.Zero);

        var query = ctx.Entities
            .Where(e => e.Id >= 10 && e.Id <= 12)
            .Where(e => e.Default >= startDate)
            .Where(e => e.Default < endDate);

        // The parameter must be declared as the column type, not String.
        Assert.Contains("{startDate:DateTime64(7, 'UTC')}", query.ToQueryString());

        Assert.Equal(4.0, await query.SumAsync(e => e.Value));
    }

    [Fact]
    public async Task Filter_with_a_non_zero_offset_parameter_matches_the_same_instant()
    {
        using var seedContext = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        seedContext.Entities.Add(new DateTimeOffsetEntity
        {
            Id = 20,
            Default = new DateTimeOffset(2026, 6, 15, 5, 0, 0, TimeSpan.Zero),
            Value = 7.0
        });
        await seedContext.SaveChangesAsync();

        using var ctx = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        // The same instant written with a +05:00 offset.
        var equivalent = new DateTimeOffset(2026, 6, 15, 10, 0, 0, TimeSpan.FromHours(5));

        var found = await ctx.Entities.SingleOrDefaultAsync(e => e.Id == 20 && e.Default == equivalent);

        Assert.NotNull(found);
    }

    [Fact]
    public async Task Ordering_and_projection_work()
    {
        using var seedContext = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        seedContext.Entities.AddRange(
            new DateTimeOffsetEntity { Id = 30, Default = new DateTimeOffset(2026, 9, 3, 0, 0, 0, TimeSpan.Zero), Value = 1 },
            new DateTimeOffsetEntity { Id = 31, Default = new DateTimeOffset(2026, 9, 1, 0, 0, 0, TimeSpan.Zero), Value = 2 });
        await seedContext.SaveChangesAsync();

        using var ctx = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        var ordered = await ctx.Entities
            .Where(e => e.Id == 30 || e.Id == 31)
            .OrderBy(e => e.Default)
            .Select(e => e.Default)
            .ToListAsync();

        Assert.Equal([
            new DateTimeOffset(2026, 9, 1, 0, 0, 0, TimeSpan.Zero),
            new DateTimeOffset(2026, 9, 3, 0, 0, 0, TimeSpan.Zero)
        ], ordered);
    }

    // --- opt out of the new default -----------------------------------------

    /// <summary>
    /// <c>HasConversion&lt;string&gt;()</c> is the documented way to keep the old <c>String</c> shape.
    /// It composes a converter, so the CLR type stays <see cref="DateTimeOffset"/>.
    /// Note that writing any converted property is a separate defect (#54).
    /// </summary>
    [Fact]
    public void HasConversion_string_keeps_the_old_string_shape()
    {
        using var ctx = new StringDateTimeOffsetDbContext(_fixture.ConnectionString);
        var property = ctx.Model
            .FindEntityType(typeof(StringDateTimeOffsetEntity))!
            .FindProperty(nameof(StringDateTimeOffsetEntity.ViaConversion))!;

        var mapping = property.GetRelationalTypeMapping();

        Assert.Equal("String", mapping.StoreType);
        Assert.Equal(typeof(DateTimeOffset), mapping.ClrType);
        Assert.NotNull(mapping.Converter);
    }

    /// <summary>
    /// By contrast, a bare <c>HasColumnType("String")</c> gives the plain string mapping with no
    /// converter, so the CLR type does not agree with the property. This is pre-existing behaviour
    /// for any CLR type pointed at an unrelated store type, and is why the README tells users to
    /// use <c>HasConversion&lt;string&gt;()</c> instead.
    /// </summary>
    [Fact]
    public void HasColumnType_string_alone_does_not_compose_a_converter()
    {
        using var ctx = new StringDateTimeOffsetDbContext(_fixture.ConnectionString);
        var property = ctx.Model
            .FindEntityType(typeof(StringDateTimeOffsetEntity))!
            .FindProperty(nameof(StringDateTimeOffsetEntity.ViaColumnType))!;

        var mapping = property.GetRelationalTypeMapping();

        Assert.Equal("String", mapping.StoreType);
        Assert.Equal(typeof(string), mapping.ClrType);
        Assert.Null(mapping.Converter);
    }

    // --- daylight saving ----------------------------------------------------

    /// <summary>
    /// The hour that repeats when clocks go back is ambiguous, because the driver gives a wall
    /// clock and drops the offset. For a zone whose standard offset is zero the instant is still
    /// recoverable: the driver only returns <see cref="DateTimeKind.Unspecified"/> when the true
    /// offset is not zero, so the zero candidate can be discarded.
    /// </summary>
    [Fact]
    public async Task Ambiguous_wall_clock_round_trips_in_a_zero_standard_offset_zone()
    {
        // On 2026-10-25 the UK goes back from +01:00 to +00:00 at 02:00 local.
        // These two distinct instants share the wall clock 01:30 in Europe/London.
        var duringDaylightSaving = new DateTimeOffset(2026, 10, 25, 0, 30, 0, TimeSpan.Zero);
        var afterDaylightSaving = new DateTimeOffset(2026, 10, 25, 1, 30, 0, TimeSpan.Zero);

        using var writeContext = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        writeContext.DstEntities.AddRange(
            new DateTimeOffsetDstEntity { Id = 1, London = duringDaylightSaving },
            new DateTimeOffsetDstEntity { Id = 2, London = afterDaylightSaving });
        await writeContext.SaveChangesAsync();

        using var readContext = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        var first = await readContext.DstEntities.SingleAsync(e => e.Id == 1);
        var second = await readContext.DstEntities.SingleAsync(e => e.Id == 2);

        // Both instants survive, and they stay distinct.
        Assert.Equal(duringDaylightSaving, first.London.ToUniversalTime());
        Assert.Equal(afterDaylightSaving, second.London.ToUniversalTime());
        Assert.NotEqual(first.London.ToUniversalTime(), second.London.ToUniversalTime());

        // The offsets show which side of the change each one is on.
        Assert.Equal(TimeSpan.FromHours(1), first.London.Offset);
        Assert.Equal(TimeSpan.Zero, second.London.Offset);
    }

    [Fact]
    public void Ambiguous_wall_clock_picks_the_non_zero_offset_when_standard_is_zero()
    {
        // 01:30 on 2026-10-25 is ambiguous in Europe/London: +01:00 or +00:00.
        var wallClock = new DateTime(2026, 10, 25, 1, 30, 0, DateTimeKind.Unspecified);

        var result = ClickHouseDateTimeOffsetTypeMapping.ConvertToDateTimeOffset(wallClock, "Europe/London");

        // Kind=Unspecified means the driver found a non-zero offset, so it must be the daylight one.
        Assert.Equal(TimeSpan.FromHours(1), result.Offset);
        Assert.Equal(new DateTimeOffset(2026, 10, 25, 0, 30, 0, TimeSpan.Zero), result.ToUniversalTime());
    }

    /// <summary>
    /// Documents the known limit. In a zone where both candidate offsets are non-zero the offset
    /// that the driver dropped cannot be recovered, so the reading keeps standard time. Europe/Paris
    /// goes back from +02:00 to +01:00, so an ambiguous wall clock reads as +01:00 either way.
    /// </summary>
    [Fact]
    public void Ambiguous_wall_clock_keeps_standard_time_when_both_offsets_are_non_zero()
    {
        // 02:30 on 2026-10-25 is ambiguous in Europe/Paris: +02:00 or +01:00.
        var wallClock = new DateTime(2026, 10, 25, 2, 30, 0, DateTimeKind.Unspecified);

        var result = ClickHouseDateTimeOffsetTypeMapping.ConvertToDateTimeOffset(wallClock, "Europe/Paris");

        Assert.Equal(TimeSpan.FromHours(1), result.Offset);
    }

    [Fact]
    public void Unresolvable_declared_timezone_throws_rather_than_guessing()
    {
        var wallClock = new DateTime(2026, 1, 15, 5, 0, 0, DateTimeKind.Unspecified);

        var ex = Assert.Throws<InvalidOperationException>(
            () => ClickHouseDateTimeOffsetTypeMapping.ConvertToDateTimeOffset(wallClock, "Not/AZone"));

        Assert.Contains("Not/AZone", ex.Message);
    }

    // --- fixed-offset timezones ---------------------------------------------

    /// <summary>
    /// ClickHouse lets a column declare a fixed UTC offset, spelled <c>Fixed/UTC±HH:MM:SS</c>.
    /// No such .NET timezone exists, so <c>TimeZoneInfo.FindSystemTimeZoneById</c> cannot resolve
    /// the name however complete the host's timezone data is. Reading it must still work.
    /// </summary>
    [Fact]
    public void A_fixed_offset_name_is_not_a_dotnet_timezone()
        => Assert.Throws<TimeZoneNotFoundException>(
            () => TimeZoneInfo.FindSystemTimeZoneById("Fixed/UTC+05:30:00"));

    [Theory]
    [InlineData("Fixed/UTC+05:30:00", 5, 30)]
    [InlineData("Fixed/UTC-07:00:00", -7, 0)]
    [InlineData("Fixed/UTC+05:45:00", 5, 45)]
    [InlineData("Fixed/UTC-09:30:00", -9, -30)]
    [InlineData("Fixed/UTC+00:01:00", 0, 1)]
    [InlineData("Fixed/UTC+14:00:00", 14, 0)]
    [InlineData("Fixed/UTC-14:00:00", -14, 0)]
    public void ConvertToDateTimeOffset_reads_a_fixed_offset_timezone(
        string timezone, int expectedHours, int expectedMinutes)
    {
        var wallClock = new DateTime(2026, 1, 15, 10, 0, 0, DateTimeKind.Unspecified);

        var result = ClickHouseDateTimeOffsetTypeMapping.ConvertToDateTimeOffset(wallClock, timezone);

        var expectedOffset = new TimeSpan(expectedHours, expectedMinutes, 0);
        Assert.Equal(expectedOffset, result.Offset);
        // The wall clock is kept as given; only the offset is attached.
        Assert.Equal(wallClock, result.DateTime);
        Assert.Equal(wallClock - expectedOffset, result.UtcDateTime);
    }

    /// <summary>
    /// A fixed offset never changes, so the daylight-saving ambiguity that affects a named zone
    /// cannot arise. The same wall clock therefore always gives the same instant.
    /// </summary>
    [Fact]
    public void A_fixed_offset_is_never_ambiguous()
    {
        // In Europe/London this wall clock is the repeated hour when clocks go back.
        var ambiguousElsewhere = new DateTime(2026, 10, 25, 1, 30, 0, DateTimeKind.Unspecified);

        var result = ClickHouseDateTimeOffsetTypeMapping.ConvertToDateTimeOffset(
            ambiguousElsewhere, "Fixed/UTC+01:00:00");

        Assert.Equal(TimeSpan.FromHours(1), result.Offset);
        Assert.Equal(new DateTimeOffset(2026, 10, 25, 0, 30, 0, TimeSpan.Zero), result.ToUniversalTime());
    }

    /// <summary>
    /// ClickHouse accepts offsets that <see cref="DateTimeOffset"/> cannot hold: it caps the
    /// magnitude at 14 hours and requires whole minutes. Both must report the column rather than
    /// let the constructor throw naming only the rule it enforces.
    /// </summary>
    [Theory]
    [InlineData("Fixed/UTC+15:00:00", "plus or minus 14 hours")]
    [InlineData("Fixed/UTC-15:00:00", "plus or minus 14 hours")]
    [InlineData("Fixed/UTC+24:00:00", "plus or minus 14 hours")]
    [InlineData("Fixed/UTC+00:00:42", "whole minutes")]
    [InlineData("Fixed/UTC+05:30:30", "whole minutes")]
    [InlineData("Fixed/UTC+09:99:99", "whole minutes")]
    public void An_unrepresentable_fixed_offset_reports_the_timezone_and_the_limit(
        string timezone, string expectedReason)
    {
        var wallClock = new DateTime(2026, 1, 15, 10, 0, 0, DateTimeKind.Unspecified);

        var ex = Assert.Throws<InvalidOperationException>(
            () => ClickHouseDateTimeOffsetTypeMapping.ConvertToDateTimeOffset(wallClock, timezone));

        Assert.Contains(timezone, ex.Message);
        Assert.Contains(expectedReason, ex.Message);
    }

    /// <summary>
    /// A name that only looks like a fixed offset is not one. ClickHouse rejects each of these, so
    /// no such column can exist, and the unresolvable-timezone error is the right answer.
    /// </summary>
    [Theory]
    [InlineData("Fixed/UTC+05:30")]
    [InlineData("fixed/utc+05:30:00")]
    [InlineData("Fixed/UTC05:30:00")]
    [InlineData("Fixed/UTC+5:30:00")]
    public void A_name_that_only_looks_like_a_fixed_offset_is_not_guessed_at(string timezone)
    {
        var wallClock = new DateTime(2026, 1, 15, 10, 0, 0, DateTimeKind.Unspecified);

        var ex = Assert.Throws<InvalidOperationException>(
            () => ClickHouseDateTimeOffsetTypeMapping.ConvertToDateTimeOffset(wallClock, timezone));

        Assert.Contains(timezone, ex.Message);
        Assert.Contains("does not know the timezone", ex.Message);
    }

    /// <summary>
    /// ClickHouse does not hold the minutes and seconds fields to 59 — it carries the excess, so
    /// <c>Fixed/UTC+05:60:00</c> is a legal name for <c>+06:00</c>. The driver does not read those
    /// names, and gives a UTC wall clock rather than one in the column's timezone, so attaching the
    /// offset would move the instant by the whole offset and report nothing. Such a column must
    /// report the driver's limit and the spelling to use instead.
    /// </summary>
    [Theory]
    [InlineData("Fixed/UTC+05:60:00", "Fixed/UTC+06:00:00")]
    [InlineData("Fixed/UTC+05:00:60", "Fixed/UTC+05:01:00")]
    [InlineData("Fixed/UTC+00:99:00", "Fixed/UTC+01:39:00")]
    [InlineData("Fixed/UTC-05:60:00", "Fixed/UTC-06:00:00")]
    public void A_carried_fixed_offset_name_reports_the_driver_limit(string timezone, string suggested)
    {
        var wallClock = new DateTime(2026, 1, 15, 10, 0, 0, DateTimeKind.Unspecified);

        var ex = Assert.Throws<InvalidOperationException>(
            () => ClickHouseDateTimeOffsetTypeMapping.ConvertToDateTimeOffset(wallClock, timezone));

        Assert.Contains(timezone, ex.Message);
        Assert.Contains("driver does not support", ex.Message);
        // The canonical spelling of the same offset, which the driver does read.
        Assert.Contains(suggested, ex.Message);
        // It must not be reported as missing host timezone data, which was the old wrong answer.
        Assert.DoesNotContain("tzdata", ex.Message);
    }

    [Theory]
    [InlineData(nameof(DateTimeOffsetFixedEntity.Half), "DateTime64(7, 'Fixed/UTC+05:30:00')", "Fixed/UTC+05:30:00")]
    [InlineData(nameof(DateTimeOffsetFixedEntity.Negative), "DateTime64(7, 'Fixed/UTC-07:00:00')", "Fixed/UTC-07:00:00")]
    [InlineData(nameof(DateTimeOffsetFixedEntity.Zero), "DateTime64(7, 'Fixed/UTC+00:00:00')", "Fixed/UTC+00:00:00")]
    public void A_fixed_offset_store_type_resolves_and_keeps_its_timezone(
        string propertyName, string expectedStoreType, string expectedTimezone)
    {
        using var ctx = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        var property = ctx.Model
            .FindEntityType(typeof(DateTimeOffsetFixedEntity))!
            .FindProperty(propertyName)!;

        var mapping = property.GetRelationalTypeMapping();

        var typed = Assert.IsType<ClickHouseDateTimeOffsetTypeMapping>(mapping);
        Assert.Equal(expectedStoreType, property.GetColumnType());
        Assert.Equal(expectedTimezone, typed.Timezone);
    }

    /// <summary>
    /// The end-to-end case from the review. Each column declares a different fixed offset, and
    /// every instant must survive with the offset the column declares.
    /// </summary>
    [Fact]
    public async Task Fixed_offset_columns_round_trip_the_instant_and_report_their_offset()
    {
        var value = new DateTimeOffset(2026, 3, 21, 14, 25, 36, TimeSpan.Zero).AddTicks(1234567);

        using var writeContext = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        writeContext.FixedEntities.Add(new DateTimeOffsetFixedEntity
        {
            Id = 1,
            Half = value,
            Negative = value,
            Quarter = value,
            Zero = value,
            Seconds = value
        });
        await writeContext.SaveChangesAsync();

        using var readContext = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        var row = await readContext.FixedEntities.SingleAsync(e => e.Id == 1);

        // The instant survives exactly at tick precision through every declared offset.
        Assert.Equal(value, row.Half.ToUniversalTime());
        Assert.Equal(value, row.Negative.ToUniversalTime());
        Assert.Equal(value, row.Quarter.ToUniversalTime());
        Assert.Equal(value, row.Zero.ToUniversalTime());
        Assert.Equal(value, row.Seconds.ToUniversalTime());

        // Each value is rendered at the offset its column declares.
        Assert.Equal(new TimeSpan(5, 30, 0), row.Half.Offset);
        Assert.Equal(new TimeSpan(-7, 0, 0), row.Negative.Offset);
        Assert.Equal(new TimeSpan(5, 45, 0), row.Quarter.Offset);
        Assert.Equal(TimeSpan.Zero, row.Zero.Offset);
        Assert.Equal(new TimeSpan(0, 1, 0), row.Seconds.Offset);
    }

    /// <summary>
    /// The same case end to end. ClickHouse accepts <c>Fixed/UTC+05:60:00</c> and creates the
    /// column, so the write succeeds; the read must then say what is actually wrong. Reporting
    /// missing host timezone data, as it did before, sends the user to install <c>tzdata</c> that
    /// would never help.
    /// </summary>
    [Fact]
    public async Task A_carried_fixed_offset_column_reports_the_driver_limit_on_read()
    {
        using var writeContext = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        writeContext.CarriedEntities.Add(new DateTimeOffsetCarriedEntity
        {
            Id = 1,
            Carried = new DateTimeOffset(2026, 3, 21, 14, 25, 36, TimeSpan.Zero)
        });
        await writeContext.SaveChangesAsync();

        using var readContext = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => readContext.CarriedEntities.SingleAsync(e => e.Id == 1));

        Assert.Contains("Fixed/UTC+05:60:00", ex.Message);
        Assert.Contains("Fixed/UTC+06:00:00", ex.Message);
        Assert.DoesNotContain("tzdata", ex.Message);
    }

    /// <summary>A filter must still match on the instant, whatever offset the column declares.</summary>
    [Fact]
    public async Task Fixed_offset_column_filters_on_the_instant()
    {
        var value = new DateTimeOffset(2026, 4, 2, 8, 15, 0, TimeSpan.Zero);

        using var writeContext = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        writeContext.FixedEntities.Add(new DateTimeOffsetFixedEntity
        {
            Id = 2,
            Half = value,
            Negative = value,
            Quarter = value,
            Zero = value,
            Seconds = value
        });
        await writeContext.SaveChangesAsync();

        using var ctx = new DateTimeOffsetDbContext(_fixture.ConnectionString);
        // The same instant written at a third offset again.
        var equivalent = new DateTimeOffset(2026, 4, 2, 13, 45, 0, TimeSpan.FromHours(5.5));

        var found = await ctx.FixedEntities.SingleOrDefaultAsync(e => e.Id == 2 && e.Half == equivalent);

        Assert.NotNull(found);
        Assert.Contains("Fixed/UTC+05:30:00", ctx.FixedEntities.Where(e => e.Half == equivalent).ToQueryString());
    }

    // --- conversion helper --------------------------------------------------

    [Fact]
    public void ConvertToDateTimeOffset_reads_utc_kind_as_offset_zero()
    {
        var value = new DateTime(2026, 1, 15, 5, 0, 0, DateTimeKind.Utc);

        var result = ClickHouseDateTimeOffsetTypeMapping.ConvertToDateTimeOffset(value, "UTC");

        Assert.Equal(new DateTimeOffset(2026, 1, 15, 5, 0, 0, TimeSpan.Zero), result);
    }

    [Fact]
    public void ConvertToDateTimeOffset_reads_a_timezone_less_wall_clock_as_utc()
    {
        var value = new DateTime(2026, 1, 15, 5, 0, 0, DateTimeKind.Unspecified);

        var result = ClickHouseDateTimeOffsetTypeMapping.ConvertToDateTimeOffset(value, null);

        Assert.Equal(new DateTimeOffset(2026, 1, 15, 5, 0, 0, TimeSpan.Zero), result);
    }

    [Fact]
    public void ConvertToDateTimeOffset_attaches_a_declared_zone_offset()
    {
        // A Tokyo wall clock of 14:00 is the instant 05:00Z.
        var value = new DateTime(2026, 1, 15, 14, 0, 0, DateTimeKind.Unspecified);

        var result = ClickHouseDateTimeOffsetTypeMapping.ConvertToDateTimeOffset(value, "Asia/Tokyo");

        Assert.Equal(TimeSpan.FromHours(9), result.Offset);
        Assert.Equal(new DateTimeOffset(2026, 1, 15, 5, 0, 0, TimeSpan.Zero), result.ToUniversalTime());
    }

    [Fact]
    public void ConvertToDateTimeOffset_passes_through_a_datetimeoffset()
    {
        var value = new DateTimeOffset(2026, 1, 15, 10, 0, 0, TimeSpan.FromHours(5));

        var result = ClickHouseDateTimeOffsetTypeMapping.ConvertToDateTimeOffset(value, "UTC");

        Assert.Equal(value, result);
    }
}
