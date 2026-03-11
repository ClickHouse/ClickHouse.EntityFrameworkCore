using ClickHouse.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EFCore.ClickHouse.Tests;

#region Entities and DbContext

public class InsertEntity
{
    public long Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public int Value { get; set; }
}

public class InsertAllTypesEntity
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

public class NullableInsertEntity
{
    public long Id { get; set; }
    public string? NullableString { get; set; }
    public int? NullableInt { get; set; }
    public double? NullableDouble { get; set; }
    public bool? NullableBool { get; set; }
    public DateTime? NullableDatetime { get; set; }
}

public class InsertDbContext : DbContext
{
    public DbSet<InsertEntity> InsertEntities => Set<InsertEntity>();
    public DbSet<InsertAllTypesEntity> AllTypes => Set<InsertAllTypesEntity>();
    public DbSet<NullableInsertEntity> NullableEntities => Set<NullableInsertEntity>();

    private readonly string _connectionString;

    public InsertDbContext(string connectionString)
    {
        _connectionString = connectionString;
    }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseClickHouse(_connectionString);
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<InsertEntity>(entity =>
        {
            entity.ToTable("insert_entities");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.Name).HasColumnName("name");
            entity.Property(e => e.Value).HasColumnName("value");
        });

        modelBuilder.Entity<InsertAllTypesEntity>(entity =>
        {
            entity.ToTable("insert_all_types");
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

        modelBuilder.Entity<NullableInsertEntity>(entity =>
        {
            entity.ToTable("nullable_insert_entities");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.NullableString).HasColumnName("nullable_string");
            entity.Property(e => e.NullableInt).HasColumnName("nullable_int");
            entity.Property(e => e.NullableDouble).HasColumnName("nullable_double");
            entity.Property(e => e.NullableBool).HasColumnName("nullable_bool");
            entity.Property(e => e.NullableDatetime).HasColumnName("nullable_datetime");
        });
    }
}

#endregion

public class InsertFixture : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder("clickhouse/clickhouse-server:latest").Build();

    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        ConnectionString = _container.GetConnectionString();

        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(ConnectionString);
        await connection.OpenAsync();

        using var cmd1 = connection.CreateCommand();
        cmd1.CommandText = """
            CREATE TABLE insert_entities (
                id Int64,
                name String,
                value Int32
            ) ENGINE = MergeTree()
            ORDER BY id
            """;
        await cmd1.ExecuteNonQueryAsync();

        using var cmd2 = connection.CreateCommand();
        cmd2.CommandText = """
            CREATE TABLE insert_all_types (
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
        await cmd2.ExecuteNonQueryAsync();

        using var cmd3 = connection.CreateCommand();
        cmd3.CommandText = """
            CREATE TABLE nullable_insert_entities (
                id Int64,
                nullable_string Nullable(String),
                nullable_int Nullable(Int32),
                nullable_double Nullable(Float64),
                nullable_bool Nullable(Bool),
                nullable_datetime Nullable(DateTime)
            ) ENGINE = MergeTree()
            ORDER BY id
            """;
        await cmd3.ExecuteNonQueryAsync();
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }
}

public class InsertIntegrationTests : IClassFixture<InsertFixture>
{
    private readonly InsertFixture _fixture;

    public InsertIntegrationTests(InsertFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task SaveChanges_InsertSingleEntity()
    {
        await using var context = new InsertDbContext(_fixture.ConnectionString);

        context.InsertEntities.Add(new InsertEntity { Id = 100, Name = "single", Value = 42 });
        await context.SaveChangesAsync();

        await using var readContext = new InsertDbContext(_fixture.ConnectionString);
        var result = await readContext.InsertEntities
            .AsNoTracking()
            .SingleOrDefaultAsync(e => e.Id == 100);

        Assert.NotNull(result);
        Assert.Equal("single", result.Name);
        Assert.Equal(42, result.Value);
    }

    [Fact]
    public async Task SaveChanges_InsertMultipleEntities()
    {
        await using var context = new InsertDbContext(_fixture.ConnectionString);

        for (var i = 200; i < 205; i++)
        {
            context.InsertEntities.Add(new InsertEntity { Id = i, Name = $"multi_{i}", Value = i * 10 });
        }

        await context.SaveChangesAsync();

        await using var readContext = new InsertDbContext(_fixture.ConnectionString);
        var results = await readContext.InsertEntities
            .Where(e => e.Id >= 200 && e.Id < 205)
            .OrderBy(e => e.Id)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(5, results.Count);
        for (var i = 0; i < 5; i++)
        {
            Assert.Equal(200 + i, results[i].Id);
            Assert.Equal($"multi_{200 + i}", results[i].Name);
            Assert.Equal((200 + i) * 10, results[i].Value);
        }
    }

    [Fact]
    public async Task SaveChanges_InsertAllScalarTypes()
    {
        var guid = new Guid("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
        var date = new DateOnly(2024, 6, 15);
        var dateTime = new DateTime(2024, 6, 15, 10, 30, 45);

        await using var context = new InsertDbContext(_fixture.ConnectionString);

        context.AllTypes.Add(new InsertAllTypesEntity
        {
            Id = 300,
            ValInt8 = -42,
            ValUInt8 = 200,
            ValInt16 = -1000,
            ValUInt16 = 50000,
            ValInt32 = -100000,
            ValUInt32 = 3000000000,
            ValInt64 = -9000000000000000000L,
            ValUInt64 = 15000000000000000000UL,
            ValFloat32 = 3.14f,
            ValFloat64 = 2.718281828459045,
            ValDecimal = 12345.6789m,
            ValBool = true,
            ValString = "all types test",
            ValUuid = guid,
            ValDate = date,
            ValDatetime = dateTime
        });

        await context.SaveChangesAsync();

        await using var readContext = new InsertDbContext(_fixture.ConnectionString);
        var result = await readContext.AllTypes
            .AsNoTracking()
            .SingleOrDefaultAsync(e => e.Id == 300);

        Assert.NotNull(result);
        Assert.Equal((sbyte)-42, result.ValInt8);
        Assert.Equal((byte)200, result.ValUInt8);
        Assert.Equal((short)-1000, result.ValInt16);
        Assert.Equal((ushort)50000, result.ValUInt16);
        Assert.Equal(-100000, result.ValInt32);
        Assert.Equal(3000000000u, result.ValUInt32);
        Assert.Equal(-9000000000000000000L, result.ValInt64);
        Assert.Equal(15000000000000000000UL, result.ValUInt64);
        Assert.Equal(3.14f, result.ValFloat32);
        Assert.Equal(2.718281828459045, result.ValFloat64, 10);
        Assert.Equal(12345.6789m, result.ValDecimal);
        Assert.True(result.ValBool);
        Assert.Equal("all types test", result.ValString);
        Assert.Equal(guid, result.ValUuid);
        Assert.Equal(date, result.ValDate);
        Assert.Equal(dateTime, result.ValDatetime);
    }

    [Fact]
    public async Task SaveChanges_InsertWithNulls()
    {
        await using var context = new InsertDbContext(_fixture.ConnectionString);

        context.NullableEntities.Add(new NullableInsertEntity
        {
            Id = 400,
            NullableString = null,
            NullableInt = null,
            NullableDouble = null,
            NullableBool = null,
            NullableDatetime = null
        });

        context.NullableEntities.Add(new NullableInsertEntity
        {
            Id = 401,
            NullableString = "not null",
            NullableInt = 42,
            NullableDouble = 3.14,
            NullableBool = true,
            NullableDatetime = new DateTime(2024, 1, 1, 12, 0, 0)
        });

        await context.SaveChangesAsync();

        await using var readContext = new InsertDbContext(_fixture.ConnectionString);
        var results = await readContext.NullableEntities
            .Where(e => e.Id >= 400 && e.Id <= 401)
            .OrderBy(e => e.Id)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(2, results.Count);

        // Null row
        Assert.Null(results[0].NullableString);
        Assert.Null(results[0].NullableInt);
        Assert.Null(results[0].NullableDouble);
        Assert.Null(results[0].NullableBool);
        Assert.Null(results[0].NullableDatetime);

        // Non-null row
        Assert.Equal("not null", results[1].NullableString);
        Assert.Equal(42, results[1].NullableInt);
        Assert.Equal(3.14, results[1].NullableDouble);
        Assert.True(results[1].NullableBool);
        Assert.Equal(new DateTime(2024, 1, 1, 12, 0, 0), results[1].NullableDatetime);
    }

    [Fact]
    public async Task SaveChanges_EntityState_IsUnchangedAfterSave()
    {
        await using var context = new InsertDbContext(_fixture.ConnectionString);

        var entity = new InsertEntity { Id = 500, Name = "state_test", Value = 99 };
        context.InsertEntities.Add(entity);

        Assert.Equal(EntityState.Added, context.Entry(entity).State);

        await context.SaveChangesAsync();

        Assert.Equal(EntityState.Unchanged, context.Entry(entity).State);
    }

    [Fact]
    public async Task SaveChanges_UpdateThrows()
    {
        await using var context = new InsertDbContext(_fixture.ConnectionString);

        // First insert
        var entity = new InsertEntity { Id = 600, Name = "original", Value = 1 };
        context.InsertEntities.Add(entity);
        await context.SaveChangesAsync();

        // Now modify and try to save
        entity.Name = "modified";

        var ex = await Assert.ThrowsAsync<NotSupportedException>(() => context.SaveChangesAsync());
        Assert.Contains("UPDATE", ex.Message);
    }

    [Fact]
    public async Task SaveChanges_DeleteThrows()
    {
        await using var context = new InsertDbContext(_fixture.ConnectionString);

        // First insert
        var entity = new InsertEntity { Id = 700, Name = "to_delete", Value = 1 };
        context.InsertEntities.Add(entity);
        await context.SaveChangesAsync();

        // Now remove and try to save
        context.InsertEntities.Remove(entity);

        var ex = await Assert.ThrowsAsync<NotSupportedException>(() => context.SaveChangesAsync());
        Assert.Contains("DELETE", ex.Message);
    }

    [Fact]
    public async Task BulkInsertAsync_InsertsEntities()
    {
        await using var context = new InsertDbContext(_fixture.ConnectionString);

        var entities = Enumerable.Range(800, 10)
            .Select(i => new InsertEntity { Id = i, Name = $"bulk_{i}", Value = i })
            .ToList();

        var rowCount = await context.BulkInsertAsync(entities);
        Assert.Equal(10, rowCount);

        await using var readContext = new InsertDbContext(_fixture.ConnectionString);
        var results = await readContext.InsertEntities
            .Where(e => e.Id >= 800 && e.Id < 810)
            .OrderBy(e => e.Id)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(10, results.Count);
        for (var i = 0; i < 10; i++)
        {
            Assert.Equal(800 + i, results[i].Id);
            Assert.Equal($"bulk_{800 + i}", results[i].Name);
        }
    }

    [Fact]
    public async Task BulkInsertAsync_LargeBatch()
    {
        await using var context = new InsertDbContext(_fixture.ConnectionString);

        var entities = Enumerable.Range(1000, 1500)
            .Select(i => new InsertEntity { Id = i, Name = $"large_{i}", Value = i % 100 })
            .ToList();

        var rowCount = await context.BulkInsertAsync(entities);
        Assert.Equal(1500, rowCount);

        await using var readContext = new InsertDbContext(_fixture.ConnectionString);
        var count = await readContext.InsertEntities
            .Where(e => e.Id >= 1000 && e.Id < 2500)
            .CountAsync();

        Assert.Equal(1500, count);
    }
}
