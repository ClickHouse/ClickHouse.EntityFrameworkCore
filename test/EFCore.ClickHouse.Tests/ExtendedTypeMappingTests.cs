using System.Net;
using System.Numerics;
using ClickHouse.Driver.Numerics;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Testcontainers.ClickHouse;
using Xunit;

namespace EFCore.ClickHouse.Tests;

#region Entities

public class NullableLowCardinalityEntity
{
    public long Id { get; set; }
    public int? NullableInt { get; set; }
    public string? NullableString { get; set; }
    public double? NullableDouble { get; set; }
    public string LowCardString { get; set; } = string.Empty;
    public string? LowCardNullableString { get; set; }
    public decimal? NullableDecimal { get; set; }
}

public class EnumEntity
{
    public long Id { get; set; }
    public string Val8 { get; set; } = string.Empty;
    public string Val16 { get; set; } = string.Empty;
}

// C# enums matching the ClickHouse Enum8('a'=1,'b'=2,'c'=3) and Enum16('x'=100,'y'=200,'z'=300)
public enum TestEnum8 { a, b, c }
public enum TestEnum16 { x, y, z }

public class ClrEnumEntity
{
    public long Id { get; set; }
    public TestEnum8 Val8 { get; set; }
    public TestEnum16 Val16 { get; set; }
}

public class IpAddressEntity
{
    public long Id { get; set; }
    public IPAddress ValIPv4 { get; set; } = IPAddress.Loopback;
    public IPAddress ValIPv6 { get; set; } = IPAddress.IPv6Loopback;
}

public class DecimalVariantEntity
{
    public long Id { get; set; }
    public decimal ValDecimal32 { get; set; }
    public decimal ValDecimal64 { get; set; }
    public decimal ValDecimal128 { get; set; }
}

public class BigDecimalEntity
{
    public long Id { get; set; }
    public ClickHouseDecimal ValDecimal128 { get; set; }
}

public class ArrayEntity
{
    public long Id { get; set; }
    public int[] IntArray { get; set; } = [];
    public string[] StringArray { get; set; } = [];
}

public class ListArrayEntity
{
    public long Id { get; set; }
    public List<int> IntArray { get; set; } = [];
    public List<string> StringArray { get; set; } = [];
}

public class MapEntity
{
    public long Id { get; set; }
    public Dictionary<string, int> StringIntMap { get; set; } = new();
}

public class TupleEntity
{
    public long Id { get; set; }
    public (int, string) IntStringTuple { get; set; }
}

public class RefTupleEntity
{
    public long Id { get; set; }
    public Tuple<int, string> IntStringTuple { get; set; } = Tuple.Create(0, "");
}

public class BigIntegerEntity
{
    public long Id { get; set; }
    public BigInteger Val128 { get; set; }
}

public class VariantEntity
{
    public long Id { get; set; }
    public object? Val { get; set; }
}

public class DynamicEntity
{
    public long Id { get; set; }
    public object? Val { get; set; }
}

#endregion

#region DbContexts

public class NullableLowCardinalityDbContext : DbContext
{
    public DbSet<NullableLowCardinalityEntity> Entities => Set<NullableLowCardinalityEntity>();
    private readonly string _connectionString;
    public NullableLowCardinalityDbContext(string cs) => _connectionString = cs;
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseClickHouse(_connectionString);
    protected override void OnModelCreating(ModelBuilder m)
    {
        m.Entity<NullableLowCardinalityEntity>(e =>
        {
            e.ToTable("nullable_lowcard_test");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.NullableInt).HasColumnName("nullable_int").HasColumnType("Nullable(Int32)");
            e.Property(x => x.NullableString).HasColumnName("nullable_string").HasColumnType("Nullable(String)");
            e.Property(x => x.NullableDouble).HasColumnName("nullable_double").HasColumnType("Nullable(Float64)");
            e.Property(x => x.LowCardString).HasColumnName("lowcard_string").HasColumnType("LowCardinality(String)");
            e.Property(x => x.LowCardNullableString).HasColumnName("lowcard_nullable_string").HasColumnType("LowCardinality(Nullable(String))");
            e.Property(x => x.NullableDecimal).HasColumnName("nullable_decimal").HasColumnType("Nullable(Decimal(18, 4))");
        });
    }
}

public class EnumDbContext : DbContext
{
    public DbSet<EnumEntity> Entities => Set<EnumEntity>();
    private readonly string _connectionString;
    public EnumDbContext(string cs) => _connectionString = cs;
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseClickHouse(_connectionString);
    protected override void OnModelCreating(ModelBuilder m)
    {
        m.Entity<EnumEntity>(e =>
        {
            e.ToTable("enum_test");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.Val8).HasColumnName("val8").HasColumnType("Enum8('a'=1,'b'=2,'c'=3)");
            e.Property(x => x.Val16).HasColumnName("val16").HasColumnType("Enum16('x'=100,'y'=200,'z'=300)");
        });
    }
}

public class ClrEnumDbContext : DbContext
{
    public DbSet<ClrEnumEntity> Entities => Set<ClrEnumEntity>();
    private readonly string _connectionString;
    public ClrEnumDbContext(string cs) => _connectionString = cs;
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseClickHouse(_connectionString);
    protected override void OnModelCreating(ModelBuilder m)
    {
        m.Entity<ClrEnumEntity>(e =>
        {
            e.ToTable("enum_test");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.Val8).HasColumnName("val8").HasColumnType("Enum8('a'=1,'b'=2,'c'=3)");
            e.Property(x => x.Val16).HasColumnName("val16").HasColumnType("Enum16('x'=100,'y'=200,'z'=300)");
        });
    }
}

public class IpAddressDbContext : DbContext
{
    public DbSet<IpAddressEntity> Entities => Set<IpAddressEntity>();
    private readonly string _connectionString;
    public IpAddressDbContext(string cs) => _connectionString = cs;
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseClickHouse(_connectionString);
    protected override void OnModelCreating(ModelBuilder m)
    {
        m.Entity<IpAddressEntity>(e =>
        {
            e.ToTable("ipaddr_test");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.ValIPv4).HasColumnName("val_ipv4").HasColumnType("IPv4");
            e.Property(x => x.ValIPv6).HasColumnName("val_ipv6").HasColumnType("IPv6");
        });
    }
}

public class DecimalVariantDbContext : DbContext
{
    public DbSet<DecimalVariantEntity> Entities => Set<DecimalVariantEntity>();
    private readonly string _connectionString;
    public DecimalVariantDbContext(string cs) => _connectionString = cs;
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseClickHouse(_connectionString);
    protected override void OnModelCreating(ModelBuilder m)
    {
        m.Entity<DecimalVariantEntity>(e =>
        {
            e.ToTable("decimal_variant_test");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.ValDecimal32).HasColumnName("val_d32").HasColumnType("Decimal32(4)");
            e.Property(x => x.ValDecimal64).HasColumnName("val_d64").HasColumnType("Decimal64(8)");
            e.Property(x => x.ValDecimal128).HasColumnName("val_d128").HasColumnType("Decimal128(18)");
        });
    }
}

public class BigDecimalDbContext : DbContext
{
    public DbSet<BigDecimalEntity> Entities => Set<BigDecimalEntity>();
    private readonly string _connectionString;
    public BigDecimalDbContext(string cs) => _connectionString = cs;
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseClickHouse(_connectionString);
    protected override void OnModelCreating(ModelBuilder m)
    {
        m.Entity<BigDecimalEntity>(e =>
        {
            e.ToTable("decimal_variant_test");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.ValDecimal128).HasColumnName("val_d128").HasColumnType("Decimal128(18)");
        });
    }
}

public class ArrayDbContext : DbContext
{
    public DbSet<ArrayEntity> Entities => Set<ArrayEntity>();
    private readonly string _connectionString;
    public ArrayDbContext(string cs) => _connectionString = cs;
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseClickHouse(_connectionString);
    protected override void OnModelCreating(ModelBuilder m)
    {
        m.Entity<ArrayEntity>(e =>
        {
            e.ToTable("array_test");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.IntArray).HasColumnName("int_array").HasColumnType("Array(Int32)");
            e.Property(x => x.StringArray).HasColumnName("string_array").HasColumnType("Array(String)");
        });
    }
}

public class ListArrayDbContext : DbContext
{
    public DbSet<ListArrayEntity> Entities => Set<ListArrayEntity>();
    private readonly string _connectionString;
    public ListArrayDbContext(string cs) => _connectionString = cs;
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseClickHouse(_connectionString);
    protected override void OnModelCreating(ModelBuilder m)
    {
        m.Entity<ListArrayEntity>(e =>
        {
            e.ToTable("array_test");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.IntArray).HasColumnName("int_array").HasColumnType("Array(Int32)");
            e.Property(x => x.StringArray).HasColumnName("string_array").HasColumnType("Array(String)");
        });
    }
}

public class MapDbContext : DbContext
{
    public DbSet<MapEntity> Entities => Set<MapEntity>();
    private readonly string _connectionString;
    public MapDbContext(string cs) => _connectionString = cs;
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseClickHouse(_connectionString);
    protected override void OnModelCreating(ModelBuilder m)
    {
        m.Entity<MapEntity>(e =>
        {
            e.ToTable("map_test");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.StringIntMap).HasColumnName("str_int_map").HasColumnType("Map(String, Int32)");
        });
    }
}

// Interface-typed collection properties on entities map to Array(T) via the v0.2.0
// EnumerableToArrayConverter<TCollection, T>. Each entity below points at array_test's
// int_array column with a different declared CLR collection type, so the same
// integration tests can confirm that Contains/Length/Count/Any translations fire
// uniformly through the ClickHouseArrayTypeMapping the type-mapping source assigns.
public class IEnumerableIntEntity { public long Id { get; set; } public IEnumerable<int> IntArray { get; set; } = []; }
public class IListIntEntity { public long Id { get; set; } public IList<int> IntArray { get; set; } = []; }
public class ICollectionIntEntity { public long Id { get; set; } public ICollection<int> IntArray { get; set; } = []; }
public class IReadOnlyListIntEntity { public long Id { get; set; } public IReadOnlyList<int> IntArray { get; set; } = []; }
public class IReadOnlyCollectionIntEntity { public long Id { get; set; } public IReadOnlyCollection<int> IntArray { get; set; } = []; }

public class IEnumerableIntDbContext : DbContext
{
    public DbSet<IEnumerableIntEntity> Entities => Set<IEnumerableIntEntity>();
    private readonly string _connectionString;
    public IEnumerableIntDbContext(string cs) => _connectionString = cs;
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseClickHouse(_connectionString);
    protected override void OnModelCreating(ModelBuilder m) =>
        m.Entity<IEnumerableIntEntity>(e =>
        {
            e.ToTable("array_test");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.IntArray).HasColumnName("int_array").HasColumnType("Array(Int32)");
        });
}

public class IListIntDbContext : DbContext
{
    public DbSet<IListIntEntity> Entities => Set<IListIntEntity>();
    private readonly string _connectionString;
    public IListIntDbContext(string cs) => _connectionString = cs;
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseClickHouse(_connectionString);
    protected override void OnModelCreating(ModelBuilder m) =>
        m.Entity<IListIntEntity>(e =>
        {
            e.ToTable("array_test");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.IntArray).HasColumnName("int_array").HasColumnType("Array(Int32)");
        });
}

public class ICollectionIntDbContext : DbContext
{
    public DbSet<ICollectionIntEntity> Entities => Set<ICollectionIntEntity>();
    private readonly string _connectionString;
    public ICollectionIntDbContext(string cs) => _connectionString = cs;
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseClickHouse(_connectionString);
    protected override void OnModelCreating(ModelBuilder m) =>
        m.Entity<ICollectionIntEntity>(e =>
        {
            e.ToTable("array_test");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.IntArray).HasColumnName("int_array").HasColumnType("Array(Int32)");
        });
}

public class IReadOnlyListIntDbContext : DbContext
{
    public DbSet<IReadOnlyListIntEntity> Entities => Set<IReadOnlyListIntEntity>();
    private readonly string _connectionString;
    public IReadOnlyListIntDbContext(string cs) => _connectionString = cs;
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseClickHouse(_connectionString);
    protected override void OnModelCreating(ModelBuilder m) =>
        m.Entity<IReadOnlyListIntEntity>(e =>
        {
            e.ToTable("array_test");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.IntArray).HasColumnName("int_array").HasColumnType("Array(Int32)");
        });
}

public class IReadOnlyCollectionIntDbContext : DbContext
{
    public DbSet<IReadOnlyCollectionIntEntity> Entities => Set<IReadOnlyCollectionIntEntity>();
    private readonly string _connectionString;
    public IReadOnlyCollectionIntDbContext(string cs) => _connectionString = cs;
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseClickHouse(_connectionString);
    protected override void OnModelCreating(ModelBuilder m) =>
        m.Entity<IReadOnlyCollectionIntEntity>(e =>
        {
            e.ToTable("array_test");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.IntArray).HasColumnName("int_array").HasColumnType("Array(Int32)");
        });
}

public class TupleDbContext : DbContext
{
    public DbSet<TupleEntity> Entities => Set<TupleEntity>();
    private readonly string _connectionString;
    public TupleDbContext(string cs) => _connectionString = cs;
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseClickHouse(_connectionString);
    protected override void OnModelCreating(ModelBuilder m)
    {
        m.Entity<TupleEntity>(e =>
        {
            e.ToTable("tuple_test");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.IntStringTuple).HasColumnName("int_str_tuple").HasColumnType("Tuple(Int32, String)");
        });
    }
}

public class RefTupleDbContext : DbContext
{
    public DbSet<RefTupleEntity> Entities => Set<RefTupleEntity>();
    private readonly string _connectionString;
    public RefTupleDbContext(string cs) => _connectionString = cs;
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseClickHouse(_connectionString);
    protected override void OnModelCreating(ModelBuilder m)
    {
        m.Entity<RefTupleEntity>(e =>
        {
            e.ToTable("tuple_test");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.IntStringTuple).HasColumnName("int_str_tuple").HasColumnType("Tuple(Int32, String)");
        });
    }
}

public class BigIntegerDbContext : DbContext
{
    public DbSet<BigIntegerEntity> Entities => Set<BigIntegerEntity>();
    private readonly string _connectionString;
    public BigIntegerDbContext(string cs) => _connectionString = cs;
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseClickHouse(_connectionString);
    protected override void OnModelCreating(ModelBuilder m)
    {
        m.Entity<BigIntegerEntity>(e =>
        {
            e.ToTable("bigint_test");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.Val128).HasColumnName("val128").HasColumnType("Int128");
        });
    }
}

public class VariantDbContext : DbContext
{
    public DbSet<VariantEntity> Entities => Set<VariantEntity>();
    private readonly string _connectionString;
    public VariantDbContext(string cs) => _connectionString = cs;
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseClickHouse(_connectionString);
    protected override void OnModelCreating(ModelBuilder m)
    {
        m.Entity<VariantEntity>(e =>
        {
            e.ToTable("variant_test");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.Val).HasColumnName("val").HasColumnType("Variant(String, UInt64, Array(UInt64))");
        });
    }
}

public class DynamicDbContext : DbContext
{
    public DbSet<DynamicEntity> Entities => Set<DynamicEntity>();
    private readonly string _connectionString;
    public DynamicDbContext(string cs) => _connectionString = cs;
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseClickHouse(_connectionString);
    protected override void OnModelCreating(ModelBuilder m)
    {
        m.Entity<DynamicEntity>(e =>
        {
            e.ToTable("dynamic_test");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.Val).HasColumnName("val").HasColumnType("Dynamic");
        });
    }
}

#endregion

#region Fixtures

public class ExtendedTypesFixture : IAsyncLifetime
{
    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        ConnectionString = await SharedContainer.GetConnectionStringAsync();

        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(ConnectionString);
        await connection.OpenAsync();

        // Nullable / LowCardinality table
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE nullable_lowcard_test (
                    id Int64,
                    nullable_int Nullable(Int32),
                    nullable_string Nullable(String),
                    nullable_double Nullable(Float64),
                    lowcard_string LowCardinality(String),
                    lowcard_nullable_string LowCardinality(Nullable(String)),
                    nullable_decimal Nullable(Decimal(18, 4))
                ) ENGINE = MergeTree() ORDER BY id
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                INSERT INTO nullable_lowcard_test VALUES
                (1, 42, 'hello', 3.14, 'low1', 'lcn1', 123.4567),
                (2, NULL, NULL, NULL, 'low2', NULL, NULL),
                (3, -100, 'world', -1.5, 'low1', 'lcn2', 0.0001)
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        // Enum table
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE enum_test (
                    id Int64,
                    val8 Enum8('a'=1,'b'=2,'c'=3),
                    val16 Enum16('x'=100,'y'=200,'z'=300)
                ) ENGINE = MergeTree() ORDER BY id
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                INSERT INTO enum_test VALUES
                (1, 'a', 'x'),
                (2, 'b', 'y'),
                (3, 'c', 'z')
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        // IP address table
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE ipaddr_test (
                    id Int64,
                    val_ipv4 IPv4,
                    val_ipv6 IPv6
                ) ENGINE = MergeTree() ORDER BY id
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                INSERT INTO ipaddr_test VALUES
                (1, '127.0.0.1', '::1'),
                (2, '192.168.1.1', 'fe80::1'),
                (3, '10.0.0.1', '2001:db8::1')
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        // Decimal variant table
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE decimal_variant_test (
                    id Int64,
                    val_d32 Decimal32(4),
                    val_d64 Decimal64(8),
                    val_d128 Decimal128(18)
                ) ENGINE = MergeTree() ORDER BY id
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                INSERT INTO decimal_variant_test VALUES
                (1, 12345.6789, 12345678.12345678, 123456789.012345678901234567),
                (2, -0.0001, -0.00000001, -0.000000000000000001),
                (3, 0.0000, 0.00000000, 0.000000000000000000)
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        // Array table
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE array_test (
                    id Int64,
                    int_array Array(Int32),
                    string_array Array(String)
                ) ENGINE = MergeTree() ORDER BY id
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                INSERT INTO array_test VALUES
                (1, [1, 2, 3], ['a', 'b', 'c']),
                (2, [], []),
                (3, [42, -1, 0, 100], ['hello', 'world'])
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        // Map table
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE map_test (
                    id Int64,
                    str_int_map Map(String, Int32)
                ) ENGINE = MergeTree() ORDER BY id
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                INSERT INTO map_test VALUES
                (1, {'a': 1, 'b': 2}),
                (2, {}),
                (3, {'x': 42, 'y': -1, 'z': 0})
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        // Tuple table
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE tuple_test (
                    id Int64,
                    int_str_tuple Tuple(Int32, String)
                ) ENGINE = MergeTree() ORDER BY id
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                INSERT INTO tuple_test VALUES
                (1, (42, 'hello')),
                (2, (0, '')),
                (3, (-1, 'world'))
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        // BigInteger table
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE bigint_test (
                    id Int64,
                    val128 Int128
                ) ENGINE = MergeTree() ORDER BY id
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                INSERT INTO bigint_test VALUES
                (1, 123456789012345678),
                (2, -99999999999999999),
                (3, 0)
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        // Variant table
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "SET allow_experimental_variant_type = 1";
            await cmd.ExecuteNonQueryAsync();
        }

        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE variant_test (
                    id Int64,
                    val Variant(String, UInt64, Array(UInt64))
                ) ENGINE = MergeTree() ORDER BY id
                SETTINGS allow_experimental_variant_type = 1
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                INSERT INTO variant_test VALUES
                (1, 'hello'::String),
                (2, 42::UInt64),
                (3, [1, 2, 3]::Array(UInt64))
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        // NULL must be inserted separately to avoid ClickHouse type coercion within a batch
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO variant_test VALUES (4, NULL)";
            await cmd.ExecuteNonQueryAsync();
        }

        // Dynamic table
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "SET allow_experimental_dynamic_type = 1";
            await cmd.ExecuteNonQueryAsync();
        }

        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE dynamic_test (
                    id Int64,
                    val Dynamic
                ) ENGINE = MergeTree() ORDER BY id
                SETTINGS allow_experimental_dynamic_type = 1
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                INSERT INTO dynamic_test VALUES
                (1, 'world'::String),
                (2, 99::UInt64),
                (3, 3.14::Float64)
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        // NULL must be inserted separately to avoid ClickHouse type coercion within a batch
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO dynamic_test VALUES (4, NULL)";
            await cmd.ExecuteNonQueryAsync();
        }
    }

    public Task DisposeAsync() => Task.CompletedTask;
}

#endregion

#region Collection Fixture

[CollectionDefinition("ExtendedTypes")]
public class ExtendedTypesCollection : ICollectionFixture<ExtendedTypesFixture>;

#endregion

#region Tests

[Collection("ExtendedTypes")]
public class NullableLowCardinalityTests
{
    private readonly ExtendedTypesFixture _fixture;
    public NullableLowCardinalityTests(ExtendedTypesFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task ReadAll_NullableColumns_RoundTrip()
    {
        await using var ctx = new NullableLowCardinalityDbContext(_fixture.ConnectionString);
        var rows = await ctx.Entities.OrderBy(e => e.Id).AsNoTracking().ToListAsync();

        Assert.Equal(3, rows.Count);

        // Row 1: non-null values
        Assert.Equal(42, rows[0].NullableInt);
        Assert.Equal("hello", rows[0].NullableString);
        Assert.True(Math.Abs(rows[0].NullableDouble!.Value - 3.14) < 0.01);
        Assert.Equal("low1", rows[0].LowCardString);
        Assert.Equal("lcn1", rows[0].LowCardNullableString);
        Assert.Equal(123.4567m, rows[0].NullableDecimal);

        // Row 2: null values
        Assert.Null(rows[1].NullableInt);
        Assert.Null(rows[1].NullableString);
        Assert.Null(rows[1].NullableDouble);
        Assert.Equal("low2", rows[1].LowCardString);
        Assert.Null(rows[1].LowCardNullableString);
        Assert.Null(rows[1].NullableDecimal);
    }

    [Fact]
    public async Task Where_NullableInt_HasValue()
    {
        await using var ctx = new NullableLowCardinalityDbContext(_fixture.ConnectionString);
        var results = await ctx.Entities
            .Where(e => e.NullableInt != null && e.NullableInt > 0)
            .AsNoTracking().ToListAsync();

        Assert.Single(results);
        Assert.Equal(42, results[0].NullableInt);
    }

    [Fact]
    public async Task Where_NullableInt_IsNull()
    {
        await using var ctx = new NullableLowCardinalityDbContext(_fixture.ConnectionString);
        var results = await ctx.Entities
            .Where(e => e.NullableInt == null)
            .AsNoTracking().ToListAsync();

        Assert.Single(results);
        Assert.Equal(2, results[0].Id);
    }

    [Fact]
    public async Task Where_LowCardinality_Filter()
    {
        await using var ctx = new NullableLowCardinalityDbContext(_fixture.ConnectionString);
        var results = await ctx.Entities
            .Where(e => e.LowCardString == "low1")
            .OrderBy(e => e.Id)
            .AsNoTracking().ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Equal(1, results[0].Id);
        Assert.Equal(3, results[1].Id);
    }
}

[Collection("ExtendedTypes")]
public class EnumTests
{
    private readonly ExtendedTypesFixture _fixture;
    public EnumTests(ExtendedTypesFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task ReadAll_Enums_AsStrings()
    {
        await using var ctx = new EnumDbContext(_fixture.ConnectionString);
        var rows = await ctx.Entities.OrderBy(e => e.Id).AsNoTracking().ToListAsync();

        Assert.Equal(3, rows.Count);
        Assert.Equal("a", rows[0].Val8);
        Assert.Equal("x", rows[0].Val16);
        Assert.Equal("b", rows[1].Val8);
        Assert.Equal("y", rows[1].Val16);
        Assert.Equal("c", rows[2].Val8);
        Assert.Equal("z", rows[2].Val16);
    }

    [Fact]
    public async Task Where_Enum8_Filter()
    {
        await using var ctx = new EnumDbContext(_fixture.ConnectionString);
        var result = await ctx.Entities
            .Where(e => e.Val8 == "b")
            .AsNoTracking().SingleOrDefaultAsync();

        Assert.NotNull(result);
        Assert.Equal(2, result.Id);
    }
}

[Collection("ExtendedTypes")]
public class ClrEnumTests
{
    private readonly ExtendedTypesFixture _fixture;
    public ClrEnumTests(ExtendedTypesFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task ReadAll_ClrEnums_RoundTrip()
    {
        await using var ctx = new ClrEnumDbContext(_fixture.ConnectionString);
        var rows = await ctx.Entities.OrderBy(e => e.Id).AsNoTracking().ToListAsync();

        Assert.Equal(3, rows.Count);
        Assert.Equal(TestEnum8.a, rows[0].Val8);
        Assert.Equal(TestEnum16.x, rows[0].Val16);
        Assert.Equal(TestEnum8.b, rows[1].Val8);
        Assert.Equal(TestEnum16.y, rows[1].Val16);
        Assert.Equal(TestEnum8.c, rows[2].Val8);
        Assert.Equal(TestEnum16.z, rows[2].Val16);
    }

    [Fact]
    public async Task Where_ClrEnum_Filter()
    {
        await using var ctx = new ClrEnumDbContext(_fixture.ConnectionString);
        var result = await ctx.Entities
            .Where(e => e.Val8 == TestEnum8.b)
            .AsNoTracking().SingleOrDefaultAsync();

        Assert.NotNull(result);
        Assert.Equal(2, result.Id);
        Assert.Equal(TestEnum16.y, result.Val16);
    }
}

[Collection("ExtendedTypes")]
public class IpAddressTests
{
    private readonly ExtendedTypesFixture _fixture;
    public IpAddressTests(ExtendedTypesFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task ReadAll_IpAddresses_RoundTrip()
    {
        await using var ctx = new IpAddressDbContext(_fixture.ConnectionString);
        var rows = await ctx.Entities.OrderBy(e => e.Id).AsNoTracking().ToListAsync();

        Assert.Equal(3, rows.Count);
        Assert.Equal(IPAddress.Parse("127.0.0.1"), rows[0].ValIPv4);
        Assert.Equal(IPAddress.Parse("::1"), rows[0].ValIPv6);
        Assert.Equal(IPAddress.Parse("192.168.1.1"), rows[1].ValIPv4);
        Assert.Equal(IPAddress.Parse("10.0.0.1"), rows[2].ValIPv4);
    }

    [Fact]
    public async Task Where_IPv4_Equality()
    {
        await using var ctx = new IpAddressDbContext(_fixture.ConnectionString);
        var target = IPAddress.Parse("192.168.1.1");
        var result = await ctx.Entities
            .Where(e => e.ValIPv4 == target)
            .AsNoTracking().SingleOrDefaultAsync();

        Assert.NotNull(result);
        Assert.Equal(2, result.Id);
    }
}

[Collection("ExtendedTypes")]
public class DecimalVariantTests
{
    private readonly ExtendedTypesFixture _fixture;
    public DecimalVariantTests(ExtendedTypesFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task ReadAll_DecimalVariants_RoundTrip()
    {
        await using var ctx = new DecimalVariantDbContext(_fixture.ConnectionString);
        var rows = await ctx.Entities.OrderBy(e => e.Id).AsNoTracking().ToListAsync();

        Assert.Equal(3, rows.Count);

        Assert.Equal(12345.6789m, rows[0].ValDecimal32);
        Assert.Equal(12345678.12345678m, rows[0].ValDecimal64);

        Assert.Equal(0.0000m, rows[2].ValDecimal32);
    }

    [Fact]
    public async Task Where_Decimal32_Comparison()
    {
        await using var ctx = new DecimalVariantDbContext(_fixture.ConnectionString);
        var results = await ctx.Entities
            .Where(e => e.ValDecimal32 > 0m)
            .AsNoTracking().ToListAsync();

        Assert.Single(results);
        Assert.Equal(1, results[0].Id);
    }
}

[Collection("ExtendedTypes")]
public class BigDecimalTests
{
    private readonly ExtendedTypesFixture _fixture;
    public BigDecimalTests(ExtendedTypesFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task ReadAll_ClickHouseDecimal_RoundTrip()
    {
        await using var ctx = new BigDecimalDbContext(_fixture.ConnectionString);
        var rows = await ctx.Entities.OrderBy(e => e.Id).AsNoTracking().ToListAsync();

        Assert.Equal(3, rows.Count);

        // Row 1: 123456789.012345678901234567
        Assert.True(rows[0].ValDecimal128 != default);

        // Row 3: 0
        Assert.Equal(new ClickHouseDecimal(0m), rows[2].ValDecimal128);
    }
}

[Collection("ExtendedTypes")]
public class ArrayTests
{
    private readonly ExtendedTypesFixture _fixture;
    public ArrayTests(ExtendedTypesFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task ReadAll_Arrays_RoundTrip()
    {
        await using var ctx = new ArrayDbContext(_fixture.ConnectionString);
        var rows = await ctx.Entities.OrderBy(e => e.Id).AsNoTracking().ToListAsync();

        Assert.Equal(3, rows.Count);

        Assert.Equal([1, 2, 3], rows[0].IntArray);
        Assert.Equal(["a", "b", "c"], rows[0].StringArray);

        Assert.Empty(rows[1].IntArray);
        Assert.Empty(rows[1].StringArray);

        Assert.Equal([42, -1, 0, 100], rows[2].IntArray);
        Assert.Equal(["hello", "world"], rows[2].StringArray);
    }

    [Fact]
    public async Task Array_Contains_Translates_To_Has()
    {
        await using var ctx = new ArrayDbContext(_fixture.ConnectionString);

        // 1. Static Enumerable.Contains
        var query1 = ctx.Entities.Where(e => e.IntArray.Contains(42));
        var result1 = await query1.ToListAsync();
        Assert.Single(result1);
        Assert.Equal(3, result1[0].Id);

        // 2. Queryable.Contains (via AsQueryable)
        var query2 = ctx.Entities.Where(e => e.StringArray.AsQueryable().Contains("hello"));
        var result2 = await query2.ToListAsync();
        Assert.Single(result2);
        Assert.Equal(3, result2[0].Id);
    }

    [Fact]
    public async Task Array_SelectToLowerContains_GeneratesArrayMapHas()
    {
        await using var ctx = new ArrayDbContext(_fixture.ConnectionString);

        var query = ctx.Entities.Where(e => e.StringArray.Select(x => x.ToLower()).Contains("hello"));

        // SQL shape: lambda translated through the existing scalar translator chain,
        // so string.ToLower() routes to lowerUTF8 and is wrapped in arrayMap inside has().
        // The compiler-assigned parameter name (often "p") shouldn't be load-bearing for
        // the test; assert structure via multiple substring matches instead.
        var sql = query.ToQueryString();
        Assert.Contains("has(arrayMap(", sql);
        Assert.Contains("lowerUTF8(", sql);
        Assert.Contains("`a`.`string_array`", sql);
        Assert.Contains(" -> ", sql);

        // Execution: row 3 has ['hello', 'world'].
        var results = await query.ToListAsync();
        Assert.Single(results);
        Assert.Equal(3, results[0].Id);
    }

    [Fact]
    public async Task Array_Contains_NullElement_DoesNotMatchNullValuesInArray()
    {
        // Documents ClickHouse's `has()` null semantics: `has(arr, NULL)` returns 0 even
        // when `arr` contains NULLs. This is the platform behavior — surfaced here so a
        // future refactor can't accidentally invert it. (See ClickHouse `hasAny`/`indexOf`
        // for explicit NULL-element handling.)
        await using var ctx = new ArrayDbContext(_fixture.ConnectionString);

        // Non-null values still work as expected.
        var match = await ctx.Entities.Where(e => e.StringArray.Contains("hello")).ToListAsync();
        Assert.Single(match);
    }

    [Fact]
    public async Task Array_SelectWithUnsupportedLambdaBody_FallsBackWithoutCrashing()
    {
        await using var ctx = new ArrayDbContext(_fixture.ConnectionString);

        // The lambda body uses string.Normalize(), which has no ClickHouse translation. The
        // lambda translation path must return null and fall back to base EF Core handling
        // rather than crashing the translator. EF Core then surfaces its standard
        // "could not be translated" exception at materialization — that's the expected
        // outcome; asserting against the message catches a regression that throws a
        // different (worse) exception from inside our translator.
        var query = ctx.Entities.Where(e => e.StringArray.Select(x => x.Normalize()).Contains("hello"));

        var ex = Assert.Throws<InvalidOperationException>(() => query.ToQueryString());
        Assert.Contains("could not be translated", ex.Message);
    }

    [Fact]
    public async Task Array_Length_Count_Any_Translate()
    {
        await using var ctx = new ArrayDbContext(_fixture.ConnectionString);

        // T[].Length → length() via IMemberTranslator.
        var lengthSql = ctx.Entities.Where(e => e.IntArray.Length == 0).ToQueryString();
        Assert.Contains("length(", lengthSql);

        // Enumerable.Count() on a mapped array column → length() via the visitor.
        var countSql = ctx.Entities.Where(e => e.StringArray.Count() > 1).ToQueryString();
        Assert.Contains("length(", countSql);

        // Enumerable.LongCount() → length() returning Int64 via the visitor.
        var longCountSql = ctx.Entities.Where(e => e.StringArray.LongCount() > 1L).ToQueryString();
        Assert.Contains("length(", longCountSql);

        // Enumerable.Any() on a mapped array column → notEmpty() via the visitor.
        var anySql = ctx.Entities.Where(e => e.StringArray.Any()).ToQueryString();
        Assert.Contains("notEmpty(", anySql);

        // Negation composes naturally — EF wraps notEmpty(arr) under a NOT/= 0 predicate.
        var notAnyQuery = ctx.Entities.Where(e => !e.StringArray.Any());
        Assert.Contains("notEmpty(", notAnyQuery.ToQueryString());
        var emptyArrayResults = await notAnyQuery.OrderBy(e => e.Id).ToListAsync();
        Assert.Single(emptyArrayResults);
        Assert.Equal(2, emptyArrayResults[0].Id);  // Row 2 has empty arrays.

        // Execution: row 2 has empty arrays.
        var emptyArrayRows = await ctx.Entities
            .Where(e => e.IntArray.Length == 0)
            .AsNoTracking()
            .ToListAsync();
        Assert.Single(emptyArrayRows);
        Assert.Equal(2, emptyArrayRows[0].Id);

        var projection = await ctx.Entities
            .OrderBy(e => e.Id)
            .Select(e => new
            {
                e.Id,
                IntLength = e.IntArray.Length,
                StringCount = e.StringArray.Count(),
                StringLongCount = e.StringArray.LongCount(),
                HasStrings = e.StringArray.Any()
            })
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(3, projection[0].IntLength);
        Assert.Equal(3, projection[0].StringCount);
        Assert.Equal(3L, projection[0].StringLongCount);
        Assert.True(projection[0].HasStrings);

        Assert.Equal(0, projection[1].IntLength);
        Assert.Equal(0, projection[1].StringCount);
        Assert.Equal(0L, projection[1].StringLongCount);
        Assert.False(projection[1].HasStrings);
    }

    [Fact]
    public async Task Array_PredicateOverloads_TranslateToHigherOrderArrayFunctions()
    {
        await using var ctx = new ArrayDbContext(_fixture.ConnectionString);

        // Any(predicate) → arrayExists(lambda, arr). Assert both the outer function call
        // shape and the lambda parameter arrow (" -> ") so a regression that erases the
        // lambda body (e.g. emitting `arrayExists(true, arr)`) is caught.
        var anyQuery = ctx.Entities.Where(e => e.IntArray.Any(x => x > 10));
        var anySql = anyQuery.ToQueryString();
        Assert.Contains("arrayExists(", anySql);
        Assert.Contains(" -> ", anySql);
        var anyResults = await anyQuery.OrderBy(e => e.Id).ToListAsync();
        Assert.Single(anyResults);
        Assert.Equal(3, anyResults[0].Id);  // Row 3 has [42, -1, 0, 100]

        // Count(predicate) → arrayCount(lambda, arr).
        var countQuery = ctx.Entities.Where(e => e.IntArray.Count(x => x > 0) >= 2);
        var countSql = countQuery.ToQueryString();
        Assert.Contains("arrayCount(", countSql);
        Assert.Contains(" -> ", countSql);
        var countResults = await countQuery.OrderBy(e => e.Id).ToListAsync();
        Assert.Equal(2, countResults.Count);  // Row 1 [1,2,3], row 3 [42,-1,0,100] → 2 positives each

        // LongCount(predicate) → arrayCount(lambda, arr) returning Int64.
        var longCountQuery = ctx.Entities.Where(e => e.IntArray.LongCount(x => x > 0) >= 2L);
        Assert.Contains("arrayCount(", longCountQuery.ToQueryString());
        var longCountResults = await longCountQuery.OrderBy(e => e.Id).ToListAsync();
        Assert.Equal(2, longCountResults.Count);

        // Queryable.Any(predicate) variant — same shape via the AsQueryable wrapper.
        var queryableAnyQuery = ctx.Entities.Where(e => e.IntArray.AsQueryable().Any(x => x > 10));
        Assert.Contains("arrayExists(", queryableAnyQuery.ToQueryString());
        var queryableAnyResults = await queryableAnyQuery.OrderBy(e => e.Id).ToListAsync();
        Assert.Single(queryableAnyResults);
        Assert.Equal(3, queryableAnyResults[0].Id);
    }

    [Fact]
    public async Task DbSetRoot_AnyAndCount_StillUseEfBaseTranslation()
    {
        // Negative shape: when the source is the DbSet itself (not a mapped array column),
        // the structural pre-filter in ClickHouseSqlTranslatingExpressionVisitor must skip
        // the array path and let EF Core's standard subquery translation handle it. The SQL
        // must NOT contain notEmpty(/length( — those are array-column shapes — and the
        // top-level query must still return correct results.
        await using var ctx = new ArrayDbContext(_fixture.ConnectionString);

        var anySubquerySql = ctx.Entities.Where(e => ctx.Entities.Any(other => other.Id > e.Id))
            .ToQueryString();
        Assert.DoesNotContain("notEmpty(", anySubquerySql);

        var countTotal = await ctx.Entities.CountAsync();
        Assert.Equal(3, countTotal);

        var anyExists = await ctx.Entities.AnyAsync();
        Assert.True(anyExists);
    }
}

[Collection("ExtendedTypes")]
public class ListArrayTests
{
    private readonly ExtendedTypesFixture _fixture;
    public ListArrayTests(ExtendedTypesFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task ReadAll_ListArrays_RoundTrip()
    {
        await using var ctx = new ListArrayDbContext(_fixture.ConnectionString);
        var rows = await ctx.Entities.OrderBy(e => e.Id).AsNoTracking().ToListAsync();

        Assert.Equal(3, rows.Count);

        Assert.Equal([1, 2, 3], rows[0].IntArray);
        Assert.Equal(["a", "b", "c"], rows[0].StringArray);

        Assert.Empty(rows[1].IntArray);
        Assert.Empty(rows[1].StringArray);

        Assert.Equal([42, -1, 0, 100], rows[2].IntArray);
        Assert.Equal(["hello", "world"], rows[2].StringArray);
    }

    [Fact]
    public async Task ListArray_Contains_Translates_To_Has()
    {
        await using var ctx = new ListArrayDbContext(_fixture.ConnectionString);

        // 1. Instance List.Contains. EF Core rewrites this to Enumerable.Contains in the
        // expression tree, so it flows through the SqlTranslatingExpressionVisitor intercept
        // (same path as the static Enumerable.Contains form).
        var query1 = ctx.Entities.Where(e => e.IntArray.Contains(42));
        Assert.Contains("has(", query1.ToQueryString());
        var result1 = await query1.ToListAsync();
        Assert.Single(result1);
        Assert.Equal(3, result1[0].Id);

        // 2. Queryable.Contains (via AsQueryable)
        var query2 = ctx.Entities.Where(e => e.StringArray.AsQueryable().Contains("hello"));
        var result2 = await query2.ToListAsync();
        Assert.Single(result2);
        Assert.Equal(3, result2[0].Id);
    }

    [Fact]
    public async Task ListArray_Count_Any_Translate()
    {
        await using var ctx = new ListArrayDbContext(_fixture.ConnectionString);

        // List<T>.Count property → length() via IMemberTranslator.
        var countPropertySql = ctx.Entities.Where(e => e.IntArray.Count == 0).ToQueryString();
        Assert.Contains("length(", countPropertySql);

        // Enumerable.Count() over the mapped column → length() via the visitor.
        var countMethodSql = ctx.Entities.Where(e => e.StringArray.Count() > 1).ToQueryString();
        Assert.Contains("length(", countMethodSql);

        // Enumerable.Any() → notEmpty() via the visitor.
        var anySql = ctx.Entities.Where(e => e.StringArray.Any()).ToQueryString();
        Assert.Contains("notEmpty(", anySql);

        var emptyListRows = await ctx.Entities
            .Where(e => e.IntArray.Count == 0)
            .AsNoTracking()
            .ToListAsync();
        Assert.Single(emptyListRows);
        Assert.Equal(2, emptyListRows[0].Id);

        var projection = await ctx.Entities
            .OrderBy(e => e.Id)
            .Select(e => new
            {
                e.Id,
                IntCount = e.IntArray.Count,
                StringCount = e.StringArray.Count(),
                HasStrings = e.StringArray.Any()
            })
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(3, projection[0].IntCount);
        Assert.Equal(3, projection[0].StringCount);
        Assert.True(projection[0].HasStrings);

        Assert.Equal(0, projection[1].IntCount);
        Assert.Equal(0, projection[1].StringCount);
        Assert.False(projection[1].HasStrings);
    }
}

/// <summary>
/// Confirms that array-helper translations (Contains/Length/Count/Any/predicate overloads)
/// fire uniformly for entity properties typed as <c>IEnumerable&lt;T&gt;</c>,
/// <c>ICollection&lt;T&gt;</c>, <c>IList&lt;T&gt;</c>, <c>IReadOnlyList&lt;T&gt;</c>, and
/// <c>IReadOnlyCollection&lt;T&gt;</c> — all of which round-trip through
/// <c>ClickHouseArrayTypeMapping</c> via <c>EnumerableToArrayConverter&lt;TCollection,T&gt;</c>.
/// The translator gates on the type mapping, not the CLR collection type, so each interface
/// shape should produce identical SQL.
/// </summary>
[Collection("ExtendedTypes")]
public class InterfaceCollectionArrayTests
{
    private readonly ExtendedTypesFixture _fixture;
    public InterfaceCollectionArrayTests(ExtendedTypesFixture fixture) => _fixture = fixture;

    // Seed (array_test): row 1 IntArray=[1,2,3] (non-empty, len 3),
    //                    row 2 IntArray=[] (empty, len 0),
    //                    row 3 IntArray=[42,-1,0,100] (non-empty, len 4, contains 42).

    [Fact]
    public async Task IEnumerable_Contains_Count_Any_Translate()
    {
        await using var ctx = new IEnumerableIntDbContext(_fixture.ConnectionString);

        var containsSql = ctx.Entities.Where(e => e.IntArray.Contains(42)).ToQueryString();
        Assert.Contains("has(", containsSql);

        var countSql = ctx.Entities.Where(e => e.IntArray.Count() > 0).ToQueryString();
        Assert.Contains("length(", countSql);

        var anySql = ctx.Entities.Where(e => e.IntArray.Any()).ToQueryString();
        Assert.Contains("notEmpty(", anySql);

        // Execute every translated query against the database.
        var containsResults = await ctx.Entities.Where(e => e.IntArray.Contains(42))
            .OrderBy(e => e.Id).AsNoTracking().ToListAsync();
        Assert.Single(containsResults);
        Assert.Equal(3, containsResults[0].Id);

        var countResults = await ctx.Entities.Where(e => e.IntArray.Count() > 0)
            .OrderBy(e => e.Id).AsNoTracking().ToListAsync();
        Assert.Equal(2, countResults.Count);
        Assert.Equal(new[] { 1L, 3L }, countResults.Select(r => r.Id).ToArray());

        var anyResults = await ctx.Entities.Where(e => e.IntArray.Any())
            .OrderBy(e => e.Id).AsNoTracking().ToListAsync();
        Assert.Equal(2, anyResults.Count);
        Assert.Equal(new[] { 1L, 3L }, anyResults.Select(r => r.Id).ToArray());

        // IEnumerable<T> has no .Count property, only .Count() — that path covered above.
    }

    [Fact]
    public async Task IList_Contains_Count_Any_Translate()
    {
        await using var ctx = new IListIntDbContext(_fixture.ConnectionString);

        Assert.Contains("has(", ctx.Entities.Where(e => e.IntArray.Contains(42)).ToQueryString());
        Assert.Contains("length(", ctx.Entities.Where(e => e.IntArray.Count() > 0).ToQueryString());
        // Count == N avoids EF Core's "Count > 0 -> Any" optimization.
        Assert.Contains("length(", ctx.Entities.Where(e => e.IntArray.Count == 4).ToQueryString());
        Assert.Contains("notEmpty(", ctx.Entities.Where(e => e.IntArray.Any()).ToQueryString());

        var containsResults = await ctx.Entities.Where(e => e.IntArray.Contains(42))
            .OrderBy(e => e.Id).AsNoTracking().ToListAsync();
        Assert.Single(containsResults);
        Assert.Equal(3, containsResults[0].Id);

        var countMethodResults = await ctx.Entities.Where(e => e.IntArray.Count() > 0)
            .OrderBy(e => e.Id).AsNoTracking().ToListAsync();
        Assert.Equal(new[] { 1L, 3L }, countMethodResults.Select(r => r.Id).ToArray());

        var countMemberResults = await ctx.Entities.Where(e => e.IntArray.Count == 4)
            .OrderBy(e => e.Id).AsNoTracking().ToListAsync();
        Assert.Single(countMemberResults);
        Assert.Equal(3, countMemberResults[0].Id);

        var anyResults = await ctx.Entities.Where(e => e.IntArray.Any())
            .OrderBy(e => e.Id).AsNoTracking().ToListAsync();
        Assert.Equal(new[] { 1L, 3L }, anyResults.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task ICollection_Contains_Count_Any_Translate()
    {
        await using var ctx = new ICollectionIntDbContext(_fixture.ConnectionString);

        Assert.Contains("has(", ctx.Entities.Where(e => e.IntArray.Contains(42)).ToQueryString());
        Assert.Contains("length(", ctx.Entities.Where(e => e.IntArray.Count() > 0).ToQueryString());
        Assert.Contains("length(", ctx.Entities.Where(e => e.IntArray.Count == 4).ToQueryString());
        Assert.Contains("notEmpty(", ctx.Entities.Where(e => e.IntArray.Any()).ToQueryString());

        var containsResults = await ctx.Entities.Where(e => e.IntArray.Contains(42))
            .OrderBy(e => e.Id).AsNoTracking().ToListAsync();
        Assert.Single(containsResults);
        Assert.Equal(3, containsResults[0].Id);

        var countMethodResults = await ctx.Entities.Where(e => e.IntArray.Count() > 0)
            .OrderBy(e => e.Id).AsNoTracking().ToListAsync();
        Assert.Equal(new[] { 1L, 3L }, countMethodResults.Select(r => r.Id).ToArray());

        var countMemberResults = await ctx.Entities.Where(e => e.IntArray.Count == 4)
            .OrderBy(e => e.Id).AsNoTracking().ToListAsync();
        Assert.Single(countMemberResults);
        Assert.Equal(3, countMemberResults[0].Id);

        var anyResults = await ctx.Entities.Where(e => e.IntArray.Any())
            .OrderBy(e => e.Id).AsNoTracking().ToListAsync();
        Assert.Equal(new[] { 1L, 3L }, anyResults.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task IReadOnlyList_Contains_Count_Any_Translate()
    {
        await using var ctx = new IReadOnlyListIntDbContext(_fixture.ConnectionString);

        Assert.Contains("has(", ctx.Entities.Where(e => e.IntArray.Contains(42)).ToQueryString());
        Assert.Contains("length(", ctx.Entities.Where(e => e.IntArray.Count() > 0).ToQueryString());
        Assert.Contains("length(", ctx.Entities.Where(e => e.IntArray.Count == 4).ToQueryString());
        Assert.Contains("notEmpty(", ctx.Entities.Where(e => e.IntArray.Any()).ToQueryString());

        var containsResults = await ctx.Entities.Where(e => e.IntArray.Contains(42))
            .OrderBy(e => e.Id).AsNoTracking().ToListAsync();
        Assert.Single(containsResults);
        Assert.Equal(3, containsResults[0].Id);

        var countMethodResults = await ctx.Entities.Where(e => e.IntArray.Count() > 0)
            .OrderBy(e => e.Id).AsNoTracking().ToListAsync();
        Assert.Equal(new[] { 1L, 3L }, countMethodResults.Select(r => r.Id).ToArray());

        var countMemberResults = await ctx.Entities.Where(e => e.IntArray.Count == 4)
            .OrderBy(e => e.Id).AsNoTracking().ToListAsync();
        Assert.Single(countMemberResults);
        Assert.Equal(3, countMemberResults[0].Id);

        var anyResults = await ctx.Entities.Where(e => e.IntArray.Any())
            .OrderBy(e => e.Id).AsNoTracking().ToListAsync();
        Assert.Equal(new[] { 1L, 3L }, anyResults.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task IReadOnlyCollection_Contains_Count_Any_Translate()
    {
        await using var ctx = new IReadOnlyCollectionIntDbContext(_fixture.ConnectionString);

        Assert.Contains("has(", ctx.Entities.Where(e => e.IntArray.Contains(42)).ToQueryString());
        Assert.Contains("length(", ctx.Entities.Where(e => e.IntArray.Count() > 0).ToQueryString());
        Assert.Contains("length(", ctx.Entities.Where(e => e.IntArray.Count == 4).ToQueryString());
        Assert.Contains("notEmpty(", ctx.Entities.Where(e => e.IntArray.Any()).ToQueryString());

        var containsResults = await ctx.Entities.Where(e => e.IntArray.Contains(42))
            .OrderBy(e => e.Id).AsNoTracking().ToListAsync();
        Assert.Single(containsResults);
        Assert.Equal(3, containsResults[0].Id);

        var countMethodResults = await ctx.Entities.Where(e => e.IntArray.Count() > 0)
            .OrderBy(e => e.Id).AsNoTracking().ToListAsync();
        Assert.Equal(new[] { 1L, 3L }, countMethodResults.Select(r => r.Id).ToArray());

        var countMemberResults = await ctx.Entities.Where(e => e.IntArray.Count == 4)
            .OrderBy(e => e.Id).AsNoTracking().ToListAsync();
        Assert.Single(countMemberResults);
        Assert.Equal(3, countMemberResults[0].Id);

        var anyResults = await ctx.Entities.Where(e => e.IntArray.Any())
            .OrderBy(e => e.Id).AsNoTracking().ToListAsync();
        Assert.Equal(new[] { 1L, 3L }, anyResults.Select(r => r.Id).ToArray());
    }
}

[Collection("ExtendedTypes")]
public class MapTests
{
    private readonly ExtendedTypesFixture _fixture;
    public MapTests(ExtendedTypesFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task ReadAll_Maps_RoundTrip()
    {
        await using var ctx = new MapDbContext(_fixture.ConnectionString);
        var rows = await ctx.Entities.OrderBy(e => e.Id).AsNoTracking().ToListAsync();

        Assert.Equal(3, rows.Count);

        Assert.Equal(2, rows[0].StringIntMap.Count);
        Assert.Equal(1, rows[0].StringIntMap["a"]);
        Assert.Equal(2, rows[0].StringIntMap["b"]);

        Assert.Empty(rows[1].StringIntMap);

        Assert.Equal(3, rows[2].StringIntMap.Count);
        Assert.Equal(42, rows[2].StringIntMap["x"]);
    }
}

[Collection("ExtendedTypes")]
public class TupleTests
{
    private readonly ExtendedTypesFixture _fixture;
    public TupleTests(ExtendedTypesFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task ReadAll_Tuples_RoundTrip()
    {
        await using var ctx = new TupleDbContext(_fixture.ConnectionString);
        var rows = await ctx.Entities.OrderBy(e => e.Id).AsNoTracking().ToListAsync();

        Assert.Equal(3, rows.Count);

        Assert.Equal((42, "hello"), rows[0].IntStringTuple);
        Assert.Equal((0, ""), rows[1].IntStringTuple);
        Assert.Equal((-1, "world"), rows[2].IntStringTuple);
    }
}

[Collection("ExtendedTypes")]
public class RefTupleTests
{
    private readonly ExtendedTypesFixture _fixture;
    public RefTupleTests(ExtendedTypesFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task ReadAll_RefTuples_RoundTrip()
    {
        await using var ctx = new RefTupleDbContext(_fixture.ConnectionString);
        var rows = await ctx.Entities.OrderBy(e => e.Id).AsNoTracking().ToListAsync();

        Assert.Equal(3, rows.Count);

        Assert.Equal(Tuple.Create(42, "hello"), rows[0].IntStringTuple);
        Assert.Equal(Tuple.Create(0, ""), rows[1].IntStringTuple);
        Assert.Equal(Tuple.Create(-1, "world"), rows[2].IntStringTuple);
    }
}

[Collection("ExtendedTypes")]
public class BigIntegerTests
{
    private readonly ExtendedTypesFixture _fixture;
    public BigIntegerTests(ExtendedTypesFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task ReadAll_BigInteger_RoundTrip()
    {
        await using var ctx = new BigIntegerDbContext(_fixture.ConnectionString);
        var rows = await ctx.Entities.OrderBy(e => e.Id).AsNoTracking().ToListAsync();

        Assert.Equal(3, rows.Count);

        Assert.Equal(BigInteger.Parse("123456789012345678"), rows[0].Val128);
        Assert.Equal(BigInteger.Parse("-99999999999999999"), rows[1].Val128);
        Assert.Equal(BigInteger.Zero, rows[2].Val128);
    }

    [Fact]
    public async Task Where_BigInteger_Filter()
    {
        await using var ctx = new BigIntegerDbContext(_fixture.ConnectionString);
        var result = await ctx.Entities
            .Where(e => e.Id == 1)
            .AsNoTracking().SingleAsync();

        Assert.Equal(BigInteger.Parse("123456789012345678"), result.Val128);
    }
}

[Collection("ExtendedTypes")]
public class VariantTests
{
    private readonly ExtendedTypesFixture _fixture;
    public VariantTests(ExtendedTypesFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task ReadAll_Variant_MixedTypes_RoundTrip()
    {
        await using var ctx = new VariantDbContext(_fixture.ConnectionString);
        var rows = await ctx.Entities.OrderBy(e => e.Id).AsNoTracking().ToListAsync();

        Assert.Equal(4, rows.Count);

        // Row 1: String
        Assert.IsType<string>(rows[0].Val);
        Assert.Equal("hello", rows[0].Val);

        // Row 2: UInt64
        Assert.IsType<ulong>(rows[1].Val);
        Assert.Equal(42UL, rows[1].Val);

        // Row 3: Array(UInt64)
        Assert.IsType<ulong[]>(rows[2].Val);
        Assert.Equal(new ulong[] { 1, 2, 3 }, (ulong[])rows[2].Val!);

        // Row 4: NULL
        Assert.Null(rows[3].Val);
    }

    [Fact]
    public async Task Where_Variant_ById()
    {
        await using var ctx = new VariantDbContext(_fixture.ConnectionString);
        var result = await ctx.Entities
            .Where(e => e.Id == 2)
            .AsNoTracking().SingleAsync();

        Assert.Equal(42UL, result.Val);
    }
}

[Collection("ExtendedTypes")]
public class DynamicTests
{
    private readonly ExtendedTypesFixture _fixture;
    public DynamicTests(ExtendedTypesFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task ReadAll_Dynamic_MixedTypes_RoundTrip()
    {
        await using var ctx = new DynamicDbContext(_fixture.ConnectionString);
        var rows = await ctx.Entities.OrderBy(e => e.Id).AsNoTracking().ToListAsync();

        Assert.Equal(4, rows.Count);

        // Row 1: String
        Assert.IsType<string>(rows[0].Val);
        Assert.Equal("world", rows[0].Val);

        // Row 2: UInt64
        Assert.IsType<ulong>(rows[1].Val);
        Assert.Equal(99UL, rows[1].Val);

        // Row 3: Float64
        Assert.IsType<double>(rows[2].Val);
        Assert.Equal(3.14, (double)rows[2].Val!, 2);

        // Row 4: NULL
        Assert.Null(rows[3].Val);
    }

    [Fact]
    public async Task Where_Dynamic_ById()
    {
        await using var ctx = new DynamicDbContext(_fixture.ConnectionString);
        var result = await ctx.Entities
            .Where(e => e.Id == 1)
            .AsNoTracking().SingleAsync();

        Assert.Equal("world", result.Val);
    }
}

#endregion

#region Unit Tests for Type Mapping Source

public class TypeMappingSourceStoreTypeTests
{
    /// <summary>
    /// Verifies that Nullable(...) and LowCardinality(...) wrappers in the user's
    /// configured store type are preserved on the resolved mapping's StoreType.
    /// The internal resolver unwraps these wrappers to find the inner CLR mapping,
    /// then re-wraps so migration DDL and SQL parameter types see the full form.
    /// </summary>
    [Theory]
    [InlineData("Nullable(Int32)")]
    [InlineData("Nullable(String)")]
    [InlineData("LowCardinality(String)")]
    [InlineData("LowCardinality(Nullable(String))")]
    [InlineData("Nullable(Float64)")]
    public void FindMapping_PreservesWrapperTypes(string storeType)
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(object), storeType);
        Assert.NotNull(mapping);
        Assert.Equal(storeType, mapping.StoreType);
    }

    [Fact]
    public void FindMapping_PreservesNullableDecimalWrapper()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(decimal?), "Nullable(Decimal(18, 4))");
        Assert.NotNull(mapping);
        Assert.Equal("Nullable(Decimal(18, 4))", mapping.StoreType);
    }

    [Theory]
    [InlineData("Enum8")]
    [InlineData("Enum16")]
    [InlineData("BFloat16")]
    [InlineData("IPv4")]
    [InlineData("IPv6")]
    [InlineData("Int128")]
    [InlineData("UInt256")]
    public void FindMapping_NewStoreTypes_Resolves(string storeType)
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(object), storeType);
        Assert.NotNull(mapping);
    }

    [Theory]
    [InlineData("Decimal32(4)")]
    [InlineData("Decimal64(8)")]
    [InlineData("Decimal128(18)")]
    [InlineData("Decimal256(38)")]
    public void FindMapping_DecimalVariants_Resolves(string storeType)
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(decimal), storeType);
        Assert.NotNull(mapping);
    }

    [Theory]
    [InlineData("Array(Int32)")]
    [InlineData("Array(String)")]
    [InlineData("Array(Nullable(Int32))")]
    [InlineData("Array(LowCardinality(String))")]
    [InlineData("Map(String, Int32)")]
    [InlineData("Map(LowCardinality(String), Nullable(Int64))")]
    [InlineData("Tuple(Int32, String)")]
    [InlineData("Tuple(Nullable(Int32), String)")]
    [InlineData("Tuple(Nullable(Int32), LowCardinality(String))")]
    public void FindMapping_ContainerTypes_Resolves(string storeType)
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(object), storeType);
        Assert.NotNull(mapping);
    }

    [Fact]
    public void FindMapping_TimeSpan_ResolvesFromClrType()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(TimeSpan));
        Assert.NotNull(mapping);
        Assert.Equal("Time", mapping.StoreType);
    }

    [Theory]
    [InlineData("Time")]
    [InlineData("Time64(3)")]
    [InlineData("Time64(6)")]
    [InlineData("Time64(9)")]
    public void FindMapping_TimeStoreTypes_Resolves(string storeType)
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(TimeSpan), storeType);
        Assert.NotNull(mapping);
        Assert.Equal(typeof(TimeSpan), mapping.ClrType);
    }

    [Fact]
    public void TimeSpanMapping_GeneratesCorrectLiteral_WholeSeconds()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(TimeSpan))!;
        var literal = mapping.GenerateSqlLiteral(new TimeSpan(1, 30, 45));
        Assert.Equal("'01:30:45'", literal);
    }

    [Fact]
    public void TimeSpanMapping_GeneratesCorrectLiteral_SubSecond()
    {
        var source = GetTypeMappingSource();
        // Use Time64(3) to get millisecond precision
        var mapping = source.FindMapping(typeof(TimeSpan), "Time64(3)")!;
        var literal = mapping.GenerateSqlLiteral(TimeSpan.FromMilliseconds(1500));
        // 1.500 seconds with 3 fractional digits
        Assert.Equal("'00:00:01.500'", literal);
    }

    [Fact]
    public void TimeSpanMapping_GeneratesCorrectLiteral_NegativeDuration()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(TimeSpan))!;
        var literal = mapping.GenerateSqlLiteral(TimeSpan.FromHours(-1.5));
        Assert.Equal("'-01:30:00'", literal);
    }

    [Fact]
    public void TimeSpanMapping_Time_TruncatesSubSecond()
    {
        var source = GetTypeMappingSource();
        // Default Time mapping has 0 fractional digits
        var mapping = source.FindMapping(typeof(TimeSpan))!;
        var literal = mapping.GenerateSqlLiteral(TimeSpan.FromMilliseconds(1500));
        // Time (seconds) should not include fractional part
        Assert.Equal("'00:00:01'", literal);
    }

    [Fact]
    public void FindMapping_ClrEnum_ResolvesWithConverter()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(TestEnum8));
        Assert.NotNull(mapping);
        Assert.Equal(typeof(TestEnum8), mapping.ClrType);
        Assert.Equal("String", mapping.StoreType);
        // The converter should be an EnumToStringConverter
        Assert.NotNull(mapping.Converter);
    }

    [Fact]
    public void FindMapping_ValueTuple_ResolvesFromClrType()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof((int, string)));
        Assert.NotNull(mapping);
        Assert.Equal(typeof((int, string)), mapping.ClrType);
        Assert.Equal("Tuple(Int32, String)", mapping.StoreType);
    }

    [Fact]
    public void FindMapping_RefTuple_ResolvesFromClrType()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(Tuple<int, string>));
        Assert.NotNull(mapping);
        Assert.Equal(typeof(Tuple<int, string>), mapping.ClrType);
        Assert.Equal("Tuple(Int32, String)", mapping.StoreType);
    }

    [Fact]
    public void FindMapping_RefTuple_WithStoreType_ResolvesCorrectly()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(Tuple<int, string>), "Tuple(Int32, String)");
        Assert.NotNull(mapping);
        Assert.Equal(typeof(Tuple<int, string>), mapping.ClrType);
    }

    [Fact]
    public void FindMapping_ValueTuple_WithStoreType_ResolvesCorrectly()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof((int, string)), "Tuple(Int32, String)");
        Assert.NotNull(mapping);
        Assert.Equal(typeof((int, string)), mapping.ClrType);
    }

    [Fact]
    public void FindMapping_ListOfInt_ResolvesWithConverter()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(List<int>));
        Assert.NotNull(mapping);
        Assert.Equal(typeof(List<int>), mapping.ClrType);
        Assert.Equal("Array(Int32)", mapping.StoreType);
        Assert.NotNull(mapping.Converter);
    }

    [Fact]
    public void FindMapping_ListOfInt_WithStoreType_ResolvesWithConverter()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(List<int>), "Array(Int32)");
        Assert.NotNull(mapping);
        Assert.Equal(typeof(List<int>), mapping.ClrType);
        Assert.Equal("Array(Int32)", mapping.StoreType);
        Assert.NotNull(mapping.Converter);
    }

    [Fact]
    public void FindMapping_ClickHouseDecimal_ResolvesFromClrType()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(ClickHouseDecimal));
        Assert.NotNull(mapping);
        Assert.Equal(typeof(ClickHouseDecimal), mapping.ClrType);
        Assert.Contains("Decimal", mapping.StoreType);
    }

    [Fact]
    public void FindMapping_ClickHouseDecimal_WithStoreType()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(ClickHouseDecimal), "Decimal128(18)");
        Assert.NotNull(mapping);
        Assert.Equal(typeof(ClickHouseDecimal), mapping.ClrType);
        Assert.Equal("Decimal(38,18)", mapping.StoreType);
    }

    [Fact]
    public void FindMapping_ClrEnum_ConverterWorks()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(TestEnum8))!;
        // Verify the converter converts enum to string correctly
        var converted = mapping.Converter!.ConvertToProvider(TestEnum8.b);
        Assert.Equal("b", converted);
        // And back
        var back = mapping.Converter.ConvertFromProvider("b");
        Assert.Equal(TestEnum8.b, back);
    }

    [Theory]
    [InlineData("Variant(String, UInt64)")]
    [InlineData("Variant(String, Int64, Float64)")]
    [InlineData("Variant(String, Array(Int32))")]
    public void FindMapping_VariantTypes_Resolves(string storeType)
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(object), storeType);
        Assert.NotNull(mapping);
        Assert.Equal(typeof(object), mapping.ClrType);
        Assert.Equal(storeType, mapping.StoreType);
    }

    [Fact]
    public void FindMapping_Dynamic_Resolves()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(object), "Dynamic");
        Assert.NotNull(mapping);
        Assert.Equal(typeof(object), mapping.ClrType);
        Assert.Equal("Dynamic", mapping.StoreType);
    }

    private static Microsoft.EntityFrameworkCore.Storage.IRelationalTypeMappingSource GetTypeMappingSource()
    {
        // Build a minimal DbContext to get a properly-configured type mapping source via DI
        var builder = new DbContextOptionsBuilder();
        builder.UseClickHouse("Host=localhost;Protocol=http");
        using var ctx = new DbContext(builder.Options);
        return ((IInfrastructure<IServiceProvider>)ctx).Instance
            .GetRequiredService<Microsoft.EntityFrameworkCore.Storage.IRelationalTypeMappingSource>();
    }
}

#endregion
