using ClickHouse.EntityFrameworkCore.Extensions;
using ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EFCore.ClickHouse.Tests;

/// <summary>
/// The element mapping of a <c>Nullable(T)</c> value-type component of an Array, Map or Tuple has
/// the store type <c>Nullable(T)</c>, and the element mapping of a <c>LowCardinality(Nullable(T))</c>
/// component has the store type <c>LowCardinality(Nullable(T))</c>. The provider uses this store type
/// for the type of a parameter that is compared with an element, and for the column type in the DDL.
/// </summary>
public class NullableElementStoreTypeTests
{
    [Theory]
    [InlineData("Array(Nullable(Int32))", "Nullable(Int32)")]
    [InlineData("Array(LowCardinality(Nullable(Int32)))", "LowCardinality(Nullable(Int32))")]
    [InlineData("Array(Nullable(Decimal(18, 4)))", "Nullable(Decimal(18, 4))")]
    [InlineData("Array(Nullable(DateTime64(3)))", "Nullable(DateTime64(3))")]
    public void Array_NullableValueTypeElement_StoreTypeHasOneNullable(string storeType, string expectedElementStoreType)
    {
        var mapping = Assert.IsType<ClickHouseArrayTypeMapping>(GetTypeMappingSource().FindMapping(storeType));

        var element = Assert.IsType<ClickHouseNullableElementMapping>(mapping.ElementMapping);
        Assert.Equal(expectedElementStoreType, element.StoreType);
        Assert.Equal(storeType, mapping.StoreType);
    }

    [Fact]
    public void NestedArray_NullableValueTypeElement_StoreTypeHasOneNullable()
    {
        var outer = Assert.IsType<ClickHouseArrayTypeMapping>(GetTypeMappingSource().FindMapping("Array(Array(Nullable(Int32)))"));
        var inner = Assert.IsType<ClickHouseArrayTypeMapping>(outer.ElementMapping);

        Assert.Equal("Array(Nullable(Int32))", inner.StoreType);
        Assert.Equal("Nullable(Int32)", inner.ElementMapping.StoreType);
    }

    [Fact]
    public void Map_NullableValueTypeValue_StoreTypeHasOneNullable()
    {
        var mapping = Assert.IsType<ClickHouseMapTypeMapping>(GetTypeMappingSource().FindMapping("Map(String, Nullable(Int32))"));

        Assert.Equal("Nullable(Int32)", mapping.ValueMapping.StoreType);
        Assert.Equal(typeof(int?), mapping.ValueMapping.ClrType);
    }

    [Fact]
    public void Tuple_NullableValueTypeItem_StoreTypeHasOneNullable()
    {
        var mapping = Assert.IsType<ClickHouseTupleTypeMapping>(GetTypeMappingSource().FindMapping("Tuple(Nullable(Int32), String)"));

        Assert.Equal("Nullable(Int32)", mapping.ElementMappings[0].StoreType);
        Assert.Equal(typeof(int?), mapping.ElementMappings[0].ClrType);
    }

    [Fact]
    public void NullableElementMapping_OverNonNullableStoreType_AddsNullable()
    {
        var mapping = new ClickHouseNullableElementMapping(new ClickHouseInt32TypeMapping());

        Assert.Equal("Nullable(Int32)", mapping.StoreType);
        Assert.Equal(typeof(int?), mapping.ClrType);
    }

    [Fact]
    public void GenerateCreateScript_NullableValueTypeElements_HaveOneNullable()
    {
        using var context = new NullableElementsDbContext("Host=localhost;Protocol=http");

        var script = context.Database.GenerateCreateScript();

        Assert.Contains("`nints` Array(Nullable(Int32))", script);
        Assert.Contains("`nint_list` Array(Nullable(Int32))", script);
        Assert.Contains("`lc_nints` Array(LowCardinality(Nullable(Int32)))", script);
        Assert.Contains("`nint_map` Map(String, Nullable(Int32))", script);
        Assert.Contains("`nint_tuple` Tuple(Nullable(Int32), String)", script);
        Assert.DoesNotContain("Nullable(Nullable(", script);
        Assert.DoesNotContain("Nullable(LowCardinality(", script);
    }

    private static IRelationalTypeMappingSource GetTypeMappingSource()
    {
        var builder = new DbContextOptionsBuilder();
        builder.UseClickHouse("Host=localhost;Protocol=http");
        using var ctx = new DbContext(builder.Options);
        return ((IInfrastructure<IServiceProvider>)ctx).Instance.GetRequiredService<IRelationalTypeMappingSource>();
    }
}

/// <summary>
/// Creates a table with <c>EnsureCreated</c> from a model with Nullable value-type elements, writes
/// rows with <c>SaveChanges</c>, and reads and filters them with captured values.
/// </summary>
public class NullableElementEnsureCreatedTests : IAsyncLifetime
{
    private string _connectionString = default!;

    public async Task InitializeAsync()
    {
        _connectionString = await SharedContainer.GetConnectionStringAsync();
    }

    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task EnsureCreated_InsertAndQuery_NullableValueTypeElements()
    {
        await using (var context = new NullableElementsDbContext(_connectionString))
        {
            await context.Database.EnsureCreatedAsync();

            context.Entities.AddRange(
                new NullableElementsEntity
                {
                    Id = 1,
                    NInts = [1, null],
                    NIntList = [1, null],
                    LcNInts = [1, null],
                    NIntMap = new() { ["a"] = 1, ["b"] = null },
                    NIntTuple = (1, "x"),
                },
                new NullableElementsEntity
                {
                    Id = 2,
                    NInts = [null],
                    NIntList = [null],
                    LcNInts = [null],
                    NIntMap = new() { ["a"] = null },
                    NIntTuple = (null, "y"),
                });
            await context.SaveChangesAsync();
        }

        await using var readContext = new NullableElementsDbContext(_connectionString);

        var columnType = await readContext.Database
            .SqlQueryRaw<string>(
                "SELECT type AS Value FROM system.columns WHERE database = currentDatabase() AND table = 'nullable_elements' AND name = 'lc_nints'")
            .SingleAsync();
        Assert.Equal("Array(LowCardinality(Nullable(Int32)))", columnType);

        var rows = await readContext.Entities.AsNoTracking().OrderBy(e => e.Id).ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Equal([1, null], rows[0].NInts);
        Assert.Equal([1, null], rows[0].NIntList);
        Assert.Equal([1, null], rows[0].LcNInts);
        Assert.Null(rows[0].NIntMap["b"]);
        Assert.Equal((null, "y"), rows[1].NIntTuple);

        int? one = 1;
        Assert.Equal([1L], await readContext.Entities.Where(e => e.NInts.Count(n => n == one) == 1).Select(e => e.Id).ToListAsync());
        Assert.Equal([1L], await readContext.Entities.Where(e => e.NIntList.Any(n => n == one)).Select(e => e.Id).ToListAsync());
        Assert.Equal([1L], await readContext.Entities.Where(e => e.LcNInts.Count(n => n == one) == 1).Select(e => e.Id).ToListAsync());
    }
}

public class NullableElementsEntity
{
    public long Id { get; set; }
    public int?[] NInts { get; set; } = [];
    public List<int?> NIntList { get; set; } = [];
    public int?[] LcNInts { get; set; } = [];
    public Dictionary<string, int?> NIntMap { get; set; } = [];
    public (int?, string) NIntTuple { get; set; }
}

public class NullableElementsDbContext : DbContext
{
    public DbSet<NullableElementsEntity> Entities => Set<NullableElementsEntity>();

    private readonly string _connectionString;

    public NullableElementsDbContext(string connectionString) => _connectionString = connectionString;

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseClickHouse(_connectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<NullableElementsEntity>(entity =>
        {
            // LowCardinality of a number needs this setting in CREATE TABLE.
            entity.ToTable("nullable_elements", t => t
                .HasMergeTreeEngine()
                .WithOrderBy("id")
                .WithSetting("allow_suspicious_low_cardinality_types", "1"));
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.NInts).HasColumnName("nints").HasColumnType("Array(Nullable(Int32))");
            entity.Property(e => e.NIntList).HasColumnName("nint_list").HasColumnType("Array(Nullable(Int32))");
            entity.Property(e => e.LcNInts).HasColumnName("lc_nints").HasColumnType("Array(LowCardinality(Nullable(Int32)))");
            entity.Property(e => e.NIntMap).HasColumnName("nint_map").HasColumnType("Map(String, Nullable(Int32))");
            entity.Property(e => e.NIntTuple).HasColumnName("nint_tuple").HasColumnType("Tuple(Nullable(Int32), String)");
        });
    }
}
