using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EFCore.ClickHouse.Tests;

public class AllTypesEntity
{
    public long Id { get; set; }
    public sbyte ValInt8 { get; set; }
    public byte ValUInt8 { get; set; }
    public short ValInt16 { get; set; }
    public ushort ValUInt16 { get; set; }
    public int ValInt32 { get; set; }
    public uint ValUInt32 { get; set; }
    public long ValInt64 { get; set; }
    public ulong ValUInt64 { get; set; }
    public float ValFloat32 { get; set; }
    public double ValFloat64 { get; set; }
    public decimal ValDecimal { get; set; }
    public bool ValBool { get; set; }
    public string ValString { get; set; } = string.Empty;
    public Guid ValUuid { get; set; }
    public DateOnly ValDate { get; set; }
    public DateTime ValDatetime { get; set; }
}

public class AllTypesDbContext : DbContext
{
    public DbSet<AllTypesEntity> AllTypes => Set<AllTypesEntity>();

    private readonly string _connectionString;

    public AllTypesDbContext(string connectionString)
    {
        _connectionString = connectionString;
    }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseClickHouse(_connectionString);
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<AllTypesEntity>(entity =>
        {
            entity.ToTable("all_types");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.ValInt8).HasColumnName("val_int8");
            entity.Property(e => e.ValUInt8).HasColumnName("val_uint8");
            entity.Property(e => e.ValInt16).HasColumnName("val_int16");
            entity.Property(e => e.ValUInt16).HasColumnName("val_uint16");
            entity.Property(e => e.ValInt32).HasColumnName("val_int32");
            entity.Property(e => e.ValUInt32).HasColumnName("val_uint32");
            entity.Property(e => e.ValInt64).HasColumnName("val_int64");
            entity.Property(e => e.ValUInt64).HasColumnName("val_uint64");
            entity.Property(e => e.ValFloat32).HasColumnName("val_float32");
            entity.Property(e => e.ValFloat64).HasColumnName("val_float64");
            entity.Property(e => e.ValDecimal).HasColumnName("val_decimal").HasColumnType("Decimal(18, 4)");
            entity.Property(e => e.ValBool).HasColumnName("val_bool");
            entity.Property(e => e.ValString).HasColumnName("val_string");
            entity.Property(e => e.ValUuid).HasColumnName("val_uuid");
            entity.Property(e => e.ValDate).HasColumnName("val_date");
            entity.Property(e => e.ValDatetime).HasColumnName("val_datetime");
        });
    }
}

public class AllTypesFixture : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder("clickhouse/clickhouse-server:latest").Build();

    public string ConnectionString { get; private set; } = string.Empty;

    // Known test data
    public static readonly Guid Guid1 = new("11111111-1111-1111-1111-111111111111");
    public static readonly Guid Guid2 = new("22222222-2222-2222-2222-222222222222");
    public static readonly Guid Guid3 = new("33333333-3333-3333-3333-333333333333");
    public static readonly Guid Guid4 = new("44444444-4444-4444-4444-444444444444");
    public static readonly Guid Guid5 = new("55555555-5555-5555-5555-555555555555");

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        ConnectionString = _container.GetConnectionString();

        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(ConnectionString);
        await connection.OpenAsync();

        using var createCmd = connection.CreateCommand();
        createCmd.CommandText = """
            CREATE TABLE all_types (
                id Int64,
                val_int8 Int8,
                val_uint8 UInt8,
                val_int16 Int16,
                val_uint16 UInt16,
                val_int32 Int32,
                val_uint32 UInt32,
                val_int64 Int64,
                val_uint64 UInt64,
                val_float32 Float32,
                val_float64 Float64,
                val_decimal Decimal(18, 4),
                val_bool Bool,
                val_string String,
                val_uuid UUID,
                val_date Date,
                val_datetime DateTime
            ) ENGINE = MergeTree()
            ORDER BY id
            """;
        await createCmd.ExecuteNonQueryAsync();

        using var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = """
            INSERT INTO all_types VALUES
            (1, -128, 0, -32768, 0, -2147483648, 0, -9223372036854775808, 0, -1.5, -1.5e100, -12345.6789, 0, '', '11111111-1111-1111-1111-111111111111', '1970-01-01', '1970-01-01 00:00:00'),
            (2, 0, 128, 0, 32768, 0, 2147483648, 0, 9223372036854775807, 0.0, 0.0, 0.0000, 1, 'hello', '22222222-2222-2222-2222-222222222222', '2000-06-15', '2000-06-15 12:30:00'),
            (3, 127, 255, 32767, 65535, 2147483647, 4294967295, 9223372036854775807, 18446744073709551615, 3.14, 3.141592653589793, 99999999999999.9999, 1, 'Special''Chars\\Here', '33333333-3333-3333-3333-333333333333', '2024-02-29', '2024-12-31 23:59:59'),
            (4, -1, 1, -1, 1, -1, 1, -1, 1, 1e-10, 1e-300, 0.0001, 0, '北京', '44444444-4444-4444-4444-444444444444', '2024-06-15', '2024-06-15 10:30:45'),
            (5, 42, 42, 42, 42, 42, 42, 42, 42, 42.0, 42.0, 42.0000, 1, 'test value', '55555555-5555-5555-5555-555555555555', '1999-12-31', '1999-12-31 23:59:59')
            """;
        await insertCmd.ExecuteNonQueryAsync();
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }
}

public class AllTypesQueryTests : IClassFixture<AllTypesFixture>
{
    private readonly AllTypesFixture _fixture;

    public AllTypesQueryTests(AllTypesFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task ReadAll_DeserializesAllTypes()
    {
        await using var ctx = new AllTypesDbContext(_fixture.ConnectionString);

        var rows = await ctx.AllTypes
            .OrderBy(e => e.Id)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(5, rows.Count);

        // Row 1: min/zero boundaries
        var r1 = rows[0];
        Assert.Equal(-128, r1.ValInt8);
        Assert.Equal((byte)0, r1.ValUInt8);
        Assert.Equal((short)-32768, r1.ValInt16);
        Assert.Equal((ushort)0, r1.ValUInt16);
        Assert.Equal(-2147483648, r1.ValInt32);
        Assert.Equal(0u, r1.ValUInt32);
        Assert.Equal(-9223372036854775808L, r1.ValInt64);
        Assert.Equal(0UL, r1.ValUInt64);
        Assert.Equal(-1.5f, r1.ValFloat32);
        Assert.True(Math.Abs(r1.ValFloat64 - (-1.5e100)) / Math.Abs(-1.5e100) < 1e-10, $"Float64 value {r1.ValFloat64} not close to -1.5e100");
        Assert.Equal(-12345.6789m, r1.ValDecimal);
        Assert.False(r1.ValBool);
        Assert.Equal("", r1.ValString);
        Assert.Equal(AllTypesFixture.Guid1, r1.ValUuid);
        Assert.Equal(new DateOnly(1970, 1, 1), r1.ValDate);
        Assert.Equal(new DateTime(1970, 1, 1, 0, 0, 0), r1.ValDatetime);

        // Row 3: max boundaries
        var r3 = rows[2];
        Assert.Equal((sbyte)127, r3.ValInt8);
        Assert.Equal((byte)255, r3.ValUInt8);
        Assert.Equal((short)32767, r3.ValInt16);
        Assert.Equal((ushort)65535, r3.ValUInt16);
        Assert.Equal(2147483647, r3.ValInt32);
        Assert.Equal(4294967295u, r3.ValUInt32);
        Assert.Equal(9223372036854775807L, r3.ValInt64);
        Assert.Equal(18446744073709551615UL, r3.ValUInt64);
        Assert.True(r3.ValBool);
        Assert.Equal("Special'Chars\\Here", r3.ValString);
        Assert.Equal(AllTypesFixture.Guid3, r3.ValUuid);
        Assert.Equal(new DateOnly(2024, 2, 29), r3.ValDate); // Leap year
    }

    [Fact]
    public async Task Where_Int8_BoundaryFilter()
    {
        await using var ctx = new AllTypesDbContext(_fixture.ConnectionString);

        var results = await ctx.AllTypes
            .Where(e => e.ValInt8 < 0)
            .OrderBy(e => e.Id)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Equal(-128, results[0].ValInt8);
        Assert.Equal(-1, results[1].ValInt8);
    }

    [Fact]
    public async Task Where_UInt64_LargeValue()
    {
        await using var ctx = new AllTypesDbContext(_fixture.ConnectionString);

        var results = await ctx.AllTypes
            .Where(e => e.ValUInt64 > 10000000000000000000UL)
            .AsNoTracking()
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(18446744073709551615UL, results[0].ValUInt64);
    }

    [Fact]
    public async Task Where_Float_Comparison()
    {
        await using var ctx = new AllTypesDbContext(_fixture.ConnectionString);

        var results = await ctx.AllTypes
            .Where(e => e.ValFloat32 > 3.0f)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(2, results.Count); // 3.14 and 42.0
    }

    [Fact]
    public async Task Where_Double_SmallValue()
    {
        await using var ctx = new AllTypesDbContext(_fixture.ConnectionString);

        var results = await ctx.AllTypes
            .Where(e => e.ValFloat64 > 0.0 && e.ValFloat64 < 1.0)
            .AsNoTracking()
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(4, results[0].Id);
    }

    [Fact]
    public async Task Where_Decimal_Comparison()
    {
        await using var ctx = new AllTypesDbContext(_fixture.ConnectionString);

        var results = await ctx.AllTypes
            .Where(e => e.ValDecimal > 0m && e.ValDecimal < 1m)
            .AsNoTracking()
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(0.0001m, results[0].ValDecimal);
    }

    [Fact]
    public async Task Where_Guid_Equality()
    {
        await using var ctx = new AllTypesDbContext(_fixture.ConnectionString);
        var targetGuid = AllTypesFixture.Guid3;

        var result = await ctx.AllTypes
            .AsNoTracking()
            .SingleOrDefaultAsync(e => e.ValUuid == targetGuid);

        Assert.NotNull(result);
        Assert.Equal(3, result.Id);
    }

    [Fact]
    public async Task Where_DateOnly_Comparison()
    {
        await using var ctx = new AllTypesDbContext(_fixture.ConnectionString);
        var cutoff = new DateOnly(2024, 1, 1);

        var results = await ctx.AllTypes
            .Where(e => e.ValDate >= cutoff)
            .OrderBy(e => e.Id)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Equal(new DateOnly(2024, 2, 29), results[0].ValDate);
        Assert.Equal(new DateOnly(2024, 6, 15), results[1].ValDate);
    }

    [Fact]
    public async Task Where_DateTime_Range()
    {
        await using var ctx = new AllTypesDbContext(_fixture.ConnectionString);
        var start = new DateTime(2000, 1, 1);
        var end = new DateTime(2024, 6, 15, 23, 59, 59);

        var results = await ctx.AllTypes
            .Where(e => e.ValDatetime >= start && e.ValDatetime <= end)
            .OrderBy(e => e.Id)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Equal(2, results[0].Id);
        Assert.Equal(4, results[1].Id);
    }

    [Fact]
    public async Task OrderBy_SignedIntegers()
    {
        await using var ctx = new AllTypesDbContext(_fixture.ConnectionString);

        var results = await ctx.AllTypes
            .OrderBy(e => e.ValInt8)
            .Select(e => e.ValInt8)
            .ToListAsync();

        Assert.Equal(5, results.Count);
        Assert.Equal(-128, results[0]);
        Assert.Equal(127, results[4]);
    }

    [Fact]
    public async Task OrderBy_DateOnly()
    {
        await using var ctx = new AllTypesDbContext(_fixture.ConnectionString);

        var results = await ctx.AllTypes
            .OrderBy(e => e.ValDate)
            .Select(e => e.ValDate)
            .ToListAsync();

        Assert.Equal(new DateOnly(1970, 1, 1), results[0]);
        Assert.Equal(new DateOnly(2024, 6, 15), results[4]);
    }

    [Fact]
    public async Task Select_MixedTypeProjection()
    {
        await using var ctx = new AllTypesDbContext(_fixture.ConnectionString);

        var results = await ctx.AllTypes
            .OrderBy(e => e.Id)
            .Select(e => new { e.ValString, e.ValInt32, e.ValBool, e.ValUuid })
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(5, results.Count);
        Assert.Equal("", results[0].ValString);
        Assert.Equal("hello", results[1].ValString);
        Assert.True(results[2].ValBool);
        Assert.Equal(AllTypesFixture.Guid4, results[3].ValUuid);
    }

    [Fact]
    public async Task Count_WithDecimalPredicate()
    {
        await using var ctx = new AllTypesDbContext(_fixture.ConnectionString);

        var count = await ctx.AllTypes
            .Where(e => e.ValDecimal > 0m)
            .CountAsync();

        Assert.Equal(3, count); // rows 3, 4, 5 have positive decimals
    }

    [Fact]
    public async Task Where_EmptyString()
    {
        await using var ctx = new AllTypesDbContext(_fixture.ConnectionString);

        var results = await ctx.AllTypes
            .Where(e => e.ValString == "")
            .AsNoTracking()
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(1, results[0].Id);
    }

    [Fact]
    public async Task Where_SpecialCharsInString()
    {
        await using var ctx = new AllTypesDbContext(_fixture.ConnectionString);

        var results = await ctx.AllTypes
            .Where(e => e.ValString == "Special'Chars\\Here")
            .AsNoTracking()
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(3, results[0].Id);
    }

    [Fact]
    public async Task Where_UnicodeString()
    {
        await using var ctx = new AllTypesDbContext(_fixture.ConnectionString);

        var results = await ctx.AllTypes
            .Where(e => e.ValString == "北京")
            .AsNoTracking()
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(4, results[0].Id);
    }
}
