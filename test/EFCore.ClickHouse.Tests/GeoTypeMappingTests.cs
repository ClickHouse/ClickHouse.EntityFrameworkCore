using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EFCore.ClickHouse.Tests;

#region Entities

public class PointEntity
{
    public long Id { get; set; }
    public Tuple<double, double> Val { get; set; } = Tuple.Create(0.0, 0.0);
}

public class RingEntity
{
    public long Id { get; set; }
    public Tuple<double, double>[] Val { get; set; } = [];
}

public class PolygonEntity
{
    public long Id { get; set; }
    public Tuple<double, double>[][] Val { get; set; } = [];
}

public class MultiPolygonEntity
{
    public long Id { get; set; }
    public Tuple<double, double>[][][] Val { get; set; } = [];
}

public class GeometryEntity
{
    public long Id { get; set; }
    public object? Val { get; set; }
}

#endregion

#region DbContexts

public class PointDbContext : DbContext
{
    public DbSet<PointEntity> Entities => Set<PointEntity>();
    private readonly string _connectionString;
    public PointDbContext(string cs) => _connectionString = cs;
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseClickHouse(_connectionString);
    protected override void OnModelCreating(ModelBuilder m)
    {
        m.Entity<PointEntity>(e =>
        {
            e.ToTable("geo_point_test");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.Val).HasColumnName("val").HasColumnType("Point");
        });
    }
}

public class RingDbContext : DbContext
{
    public DbSet<RingEntity> Entities => Set<RingEntity>();
    private readonly string _connectionString;
    public RingDbContext(string cs) => _connectionString = cs;
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseClickHouse(_connectionString);
    protected override void OnModelCreating(ModelBuilder m)
    {
        m.Entity<RingEntity>(e =>
        {
            e.ToTable("geo_ring_test");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.Val).HasColumnName("val").HasColumnType("Ring");
        });
    }
}

public class PolygonDbContext : DbContext
{
    public DbSet<PolygonEntity> Entities => Set<PolygonEntity>();
    private readonly string _connectionString;
    public PolygonDbContext(string cs) => _connectionString = cs;
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseClickHouse(_connectionString);
    protected override void OnModelCreating(ModelBuilder m)
    {
        m.Entity<PolygonEntity>(e =>
        {
            e.ToTable("geo_polygon_test");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.Val).HasColumnName("val").HasColumnType("Polygon");
        });
    }
}

public class MultiPolygonDbContext : DbContext
{
    public DbSet<MultiPolygonEntity> Entities => Set<MultiPolygonEntity>();
    private readonly string _connectionString;
    public MultiPolygonDbContext(string cs) => _connectionString = cs;
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseClickHouse(_connectionString);
    protected override void OnModelCreating(ModelBuilder m)
    {
        m.Entity<MultiPolygonEntity>(e =>
        {
            e.ToTable("geo_multipolygon_test");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.Val).HasColumnName("val").HasColumnType("MultiPolygon");
        });
    }
}

public class GeometryDbContext : DbContext
{
    public DbSet<GeometryEntity> Entities => Set<GeometryEntity>();
    private readonly string _connectionString;
    public GeometryDbContext(string cs) => _connectionString = cs;
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseClickHouse(_connectionString);
    protected override void OnModelCreating(ModelBuilder m)
    {
        m.Entity<GeometryEntity>(e =>
        {
            e.ToTable("geo_geometry_test");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.Val).HasColumnName("val").HasColumnType("Geometry");
        });
    }
}

#endregion

#region Fixture

public class GeoTypesFixture : IAsyncLifetime
{
    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        ConnectionString = await SharedContainer.GetConnectionStringAsync();

        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(ConnectionString);
        await connection.OpenAsync();

        // Point table
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE geo_point_test (
                    id Int64,
                    val Point
                ) ENGINE = MergeTree() ORDER BY id
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                INSERT INTO geo_point_test VALUES
                (1, (10.0, 20.0)),
                (2, (-73.9857, 40.7484)),
                (3, (0.0, 0.0))
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        // Ring table
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE geo_ring_test (
                    id Int64,
                    val Ring
                ) ENGINE = MergeTree() ORDER BY id
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                INSERT INTO geo_ring_test VALUES
                (1, [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]),
                (2, [(1.0, 1.0), (2.0, 1.0), (2.0, 2.0), (1.0, 1.0)])
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        // Polygon table (array of rings)
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE geo_polygon_test (
                    id Int64,
                    val Polygon
                ) ENGINE = MergeTree() ORDER BY id
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                INSERT INTO geo_polygon_test VALUES
                (1, [[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]]),
                (2, [[(0.0, 0.0), (20.0, 0.0), (20.0, 20.0), (0.0, 20.0), (0.0, 0.0)], [(5.0, 5.0), (15.0, 5.0), (15.0, 15.0), (5.0, 15.0), (5.0, 5.0)]])
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        // MultiPolygon table
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE geo_multipolygon_test (
                    id Int64,
                    val MultiPolygon
                ) ENGINE = MergeTree() ORDER BY id
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                INSERT INTO geo_multipolygon_test VALUES
                (1, [[[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]]])
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        // Geometry table (variant of geo types)
        // Ring and LineString share the same underlying type, which ClickHouse flags as suspicious
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "SET allow_experimental_geo_types = 1, allow_suspicious_variant_types = 1";
            await cmd.ExecuteNonQueryAsync();
        }

        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE geo_geometry_test (
                    id Int64,
                    val Geometry
                ) ENGINE = MergeTree() ORDER BY id
                SETTINGS allow_experimental_geo_types = 1, allow_suspicious_variant_types = 1
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                INSERT INTO geo_geometry_test VALUES
                (1, (5.0, 5.0)),
                (2, [(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)])
                """;
            await cmd.ExecuteNonQueryAsync();
        }
    }

    public Task DisposeAsync() => Task.CompletedTask;
}

#endregion

#region Collection Fixture

[CollectionDefinition("GeoTypes")]
public class GeoTypesCollection : ICollectionFixture<GeoTypesFixture>;

#endregion

#region Unit Tests

public class GeoTypeMappingSourceTests
{
    [Theory]
    [InlineData("Point")]
    [InlineData("Ring")]
    [InlineData("LineString")]
    [InlineData("Polygon")]
    [InlineData("MultiLineString")]
    [InlineData("MultiPolygon")]
    [InlineData("Geometry")]
    public void FindMapping_GeoStoreTypes_Resolves(string storeType)
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(object), storeType);
        Assert.NotNull(mapping);
    }

    [Fact]
    public void FindMapping_Point_ClrType_Is_ReferenceTuple()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(object), "Point")!;
        Assert.Equal(typeof(Tuple<double, double>), mapping.ClrType);
    }

    [Fact]
    public void FindMapping_Ring_ClrType_Is_ReferenceTupleArray()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(object), "Ring")!;
        Assert.Equal(typeof(Tuple<double, double>[]), mapping.ClrType);
    }

    [Fact]
    public void FindMapping_LineString_ClrType_Is_ReferenceTupleArray()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(object), "LineString")!;
        Assert.Equal(typeof(Tuple<double, double>[]), mapping.ClrType);
    }

    [Fact]
    public void FindMapping_Polygon_ClrType_Is_NestedArray()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(object), "Polygon")!;
        Assert.Equal(typeof(Tuple<double, double>[][]), mapping.ClrType);
    }

    [Fact]
    public void FindMapping_MultiPolygon_ClrType_Is_TripleNestedArray()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(object), "MultiPolygon")!;
        Assert.Equal(typeof(Tuple<double, double>[][][]), mapping.ClrType);
    }

    [Fact]
    public void FindMapping_Geometry_ClrType_Is_Object()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(object), "Geometry")!;
        Assert.Equal(typeof(object), mapping.ClrType);
    }

    [Fact]
    public void FindMapping_NullablePoint_Resolves()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(object), "Nullable(Point)");
        Assert.NotNull(mapping);
        Assert.Equal(typeof(Tuple<double, double>), mapping.ClrType);
    }

    [Fact]
    public void Point_SqlLiteral_GeneratesTupleSyntax()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(object), "Point")!;
        var literal = mapping.GenerateSqlLiteral(Tuple.Create(10.5, 20.5));
        Assert.Equal("(10.5, 20.5)", literal);
    }

    [Fact]
    public void Ring_SqlLiteral_GeneratesArrayOfTupleSyntax()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(object), "Ring")!;
        var ring = new[]
        {
            Tuple.Create(0.5, 0.5),
            Tuple.Create(1.5, 0.5),
            Tuple.Create(1.5, 1.5),
            Tuple.Create(0.5, 0.5),
        };
        var literal = mapping.GenerateSqlLiteral(ring);
        Assert.Equal("[(0.5, 0.5), (1.5, 0.5), (1.5, 1.5), (0.5, 0.5)]", literal);
    }

    [Fact]
    public void FindMapping_Point_StoreType_ShowsComposedType()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(object), "Point")!;
        // Store type shows the composed structural type, not the alias
        Assert.Equal("Tuple(Float64, Float64)", mapping.StoreType);
    }

    [Fact]
    public void FindMapping_Ring_StoreType_ShowsComposedType()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(object), "Ring")!;
        Assert.Equal("Array(Tuple(Float64, Float64))", mapping.StoreType);
    }

    [Fact]
    public void FindMapping_Geometry_StoreType_ShowsVariantComposition()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(object), "Geometry")!;
        Assert.Contains("Variant(", mapping.StoreType);
    }

    private static Microsoft.EntityFrameworkCore.Storage.IRelationalTypeMappingSource GetTypeMappingSource()
    {
        var builder = new DbContextOptionsBuilder();
        builder.UseClickHouse("Host=localhost;Protocol=http");
        using var ctx = new DbContext(builder.Options);
        return ((IInfrastructure<IServiceProvider>)ctx).Instance
            .GetRequiredService<Microsoft.EntityFrameworkCore.Storage.IRelationalTypeMappingSource>();
    }
}

#endregion

#region Integration Tests

[Collection("GeoTypes")]
public class GeoPointTests
{
    private readonly GeoTypesFixture _fixture;
    public GeoPointTests(GeoTypesFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task ReadAll_Points_RoundTrip()
    {
        await using var ctx = new PointDbContext(_fixture.ConnectionString);
        var rows = await ctx.Entities.Where(e => e.Id <= 3).OrderBy(e => e.Id).AsNoTracking().ToListAsync();

        Assert.Equal(3, rows.Count);

        Assert.Equal(10.0, rows[0].Val.Item1);
        Assert.Equal(20.0, rows[0].Val.Item2);

        Assert.Equal(-73.9857, rows[1].Val.Item1, 4);
        Assert.Equal(40.7484, rows[1].Val.Item2, 4);

        Assert.Equal(0.0, rows[2].Val.Item1);
        Assert.Equal(0.0, rows[2].Val.Item2);
    }

    [Fact]
    public async Task Insert_Point_RoundTrip()
    {
        await using var ctx = new PointDbContext(_fixture.ConnectionString);
        ctx.Entities.Add(new PointEntity
        {
            Id = 100,
            Val = Tuple.Create(55.7558, 37.6173)
        });
        await ctx.SaveChangesAsync();

        await using var ctx2 = new PointDbContext(_fixture.ConnectionString);
        var row = await ctx2.Entities.Where(e => e.Id == 100).AsNoTracking().SingleAsync();
        Assert.Equal(55.7558, row.Val.Item1, 4);
        Assert.Equal(37.6173, row.Val.Item2, 4);
    }
}

[Collection("GeoTypes")]
public class GeoRingTests
{
    private readonly GeoTypesFixture _fixture;
    public GeoRingTests(GeoTypesFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task ReadAll_Rings_RoundTrip()
    {
        await using var ctx = new RingDbContext(_fixture.ConnectionString);
        var rows = await ctx.Entities.Where(e => e.Id <= 2).OrderBy(e => e.Id).AsNoTracking().ToListAsync();

        Assert.Equal(2, rows.Count);

        // Row 1: square ring with 5 points
        Assert.Equal(5, rows[0].Val.Length);
        Assert.Equal(0.0, rows[0].Val[0].Item1);
        Assert.Equal(0.0, rows[0].Val[0].Item2);
        Assert.Equal(10.0, rows[0].Val[1].Item1);
        Assert.Equal(0.0, rows[0].Val[1].Item2);

        // Row 2: triangle ring with 4 points
        Assert.Equal(4, rows[1].Val.Length);
    }

    [Fact]
    public async Task Insert_Ring_RoundTrip()
    {
        await using var ctx = new RingDbContext(_fixture.ConnectionString);
        var ring = new[]
        {
            Tuple.Create(0.0, 0.0),
            Tuple.Create(5.0, 0.0),
            Tuple.Create(5.0, 5.0),
            Tuple.Create(0.0, 0.0),
        };
        ctx.Entities.Add(new RingEntity { Id = 100, Val = ring });
        await ctx.SaveChangesAsync();

        await using var ctx2 = new RingDbContext(_fixture.ConnectionString);
        var row = await ctx2.Entities.Where(e => e.Id == 100).AsNoTracking().SingleAsync();
        Assert.Equal(4, row.Val.Length);
        Assert.Equal(5.0, row.Val[1].Item1);
    }
}

[Collection("GeoTypes")]
public class GeoPolygonTests
{
    private readonly GeoTypesFixture _fixture;
    public GeoPolygonTests(GeoTypesFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task ReadAll_Polygons_RoundTrip()
    {
        await using var ctx = new PolygonDbContext(_fixture.ConnectionString);
        var rows = await ctx.Entities.Where(e => e.Id <= 2).OrderBy(e => e.Id).AsNoTracking().ToListAsync();

        Assert.Equal(2, rows.Count);

        // Row 1: single ring (outer boundary)
        Assert.Single(rows[0].Val);
        Assert.Equal(5, rows[0].Val[0].Length);

        // Row 2: two rings (outer + hole)
        Assert.Equal(2, rows[1].Val.Length);
        Assert.Equal(5, rows[1].Val[0].Length);
        Assert.Equal(5, rows[1].Val[1].Length);
    }

    [Fact]
    public async Task Insert_Polygon_RoundTrip()
    {
        await using var ctx = new PolygonDbContext(_fixture.ConnectionString);
        var polygon = new[]
        {
            new[]
            {
                Tuple.Create(0.0, 0.0),
                Tuple.Create(3.0, 0.0),
                Tuple.Create(3.0, 3.0),
                Tuple.Create(0.0, 3.0),
                Tuple.Create(0.0, 0.0),
            }
        };
        ctx.Entities.Add(new PolygonEntity { Id = 100, Val = polygon });
        await ctx.SaveChangesAsync();

        await using var ctx2 = new PolygonDbContext(_fixture.ConnectionString);
        var row = await ctx2.Entities.Where(e => e.Id == 100).AsNoTracking().SingleAsync();
        Assert.Single(row.Val);
        Assert.Equal(5, row.Val[0].Length);
        Assert.Equal(3.0, row.Val[0][1].Item1);
    }
}

[Collection("GeoTypes")]
public class GeoMultiPolygonTests
{
    private readonly GeoTypesFixture _fixture;
    public GeoMultiPolygonTests(GeoTypesFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task ReadAll_MultiPolygons_RoundTrip()
    {
        await using var ctx = new MultiPolygonDbContext(_fixture.ConnectionString);
        var rows = await ctx.Entities.Where(e => e.Id <= 1).OrderBy(e => e.Id).AsNoTracking().ToListAsync();

        Assert.Single(rows);

        // Row 1: one polygon with one ring
        Assert.Single(rows[0].Val);
        Assert.Single(rows[0].Val[0]);
        Assert.Equal(5, rows[0].Val[0][0].Length);
    }
}

[Collection("GeoTypes")]
public class GeoGeometryTests
{
    private readonly GeoTypesFixture _fixture;
    public GeoGeometryTests(GeoTypesFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task ReadAll_Geometry_MixedTypes_RoundTrip()
    {
        await using var ctx = new GeometryDbContext(_fixture.ConnectionString);
        var rows = await ctx.Entities.Where(e => e.Id <= 2).OrderBy(e => e.Id).AsNoTracking().ToListAsync();

        Assert.Equal(2, rows.Count);

        // Row 1: Point (5.0, 5.0) — driver returns Tuple<double, double>
        Assert.NotNull(rows[0].Val);
        Assert.IsType<Tuple<double, double>>(rows[0].Val);
        var point = (Tuple<double, double>)rows[0].Val!;
        Assert.Equal(5.0, point.Item1);
        Assert.Equal(5.0, point.Item2);

        // Row 2: Ring/LineString [(0,0), (1,1), (2,0)] — driver returns Tuple<double,double>[]
        Assert.NotNull(rows[1].Val);
    }
}

#endregion
