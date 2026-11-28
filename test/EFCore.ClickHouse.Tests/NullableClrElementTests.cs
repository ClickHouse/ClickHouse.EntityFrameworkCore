using ClickHouse.EntityFrameworkCore.Extensions;
using ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EFCore.ClickHouse.Tests;

/// <summary>
/// A property without <c>HasColumnType</c> whose CLR type has a <c>Nullable&lt;T&gt;</c> component,
/// such as <c>int?[]</c>, <c>List&lt;int?&gt;</c>, <c>Dictionary&lt;string, int?&gt;</c> or
/// <c>(int?, string)</c>, maps to a ClickHouse type with <c>Nullable(T)</c> for that component.
/// </summary>
public class NullableClrElementTypeMappingTests
{
    private const string NoServer = "Host=localhost;Protocol=http";

    [Theory]
    [InlineData(typeof(int?[]), "Array(Nullable(Int32))")]
    [InlineData(typeof(List<int?>), "Array(Nullable(Int32))")]
    [InlineData(typeof(IEnumerable<int?>), "Array(Nullable(Int32))")]
    [InlineData(typeof(IReadOnlyList<long?>), "Array(Nullable(Int64))")]
    [InlineData(typeof(int?[][]), "Array(Array(Nullable(Int32)))")]
    [InlineData(typeof(Guid?[]), "Array(Nullable(UUID))")]
    [InlineData(typeof(DateTime?[]), "Array(Nullable(DateTime))")]
    [InlineData(typeof(bool?[]), "Array(Nullable(Bool))")]
    [InlineData(typeof(decimal?[]), "Array(Nullable(Decimal(18,2)))")]
    [InlineData(typeof(Dictionary<string, int?>), "Map(String, Nullable(Int32))")]
    [InlineData(typeof(Dictionary<string, int?[]>), "Map(String, Array(Nullable(Int32)))")]
    [InlineData(typeof((int?, string)), "Tuple(Nullable(Int32), String)")]
    [InlineData(typeof(Tuple<int?, string>), "Tuple(Nullable(Int32), String)")]
    public void FindMapping_NullableValueTypeComponent_HasNullableStoreType(Type clrType, string expectedStoreType)
    {
        var mapping = GetTypeMappingSource().FindMapping(clrType);

        Assert.NotNull(mapping);
        Assert.Equal(expectedStoreType, mapping.StoreType);
        Assert.Equal(clrType, mapping.ClrType);
    }

    [Fact]
    public void FindMapping_NullableValueTypeArray_ElementIsNullableElementMapping()
    {
        var mapping = Assert.IsType<ClickHouseArrayTypeMapping>(GetTypeMappingSource().FindMapping(typeof(int?[])));

        var element = Assert.IsType<ClickHouseNullableElementMapping>(mapping.ElementMapping);
        Assert.Equal("Nullable(Int32)", element.StoreType);
        Assert.Equal(typeof(int?), element.ClrType);
        Assert.Null(mapping.Converter);
    }

    [Theory]
    [InlineData(typeof(int[]), "Array(Int32)")]
    [InlineData(typeof(List<int>), "Array(Int32)")]
    [InlineData(typeof(string[]), "Array(String)")]
    [InlineData(typeof(Dictionary<string, int>), "Map(String, Int32)")]
    [InlineData(typeof((int, string)), "Tuple(Int32, String)")]
    // An enum element has a value converter to String. The array mapping does not apply element converters.
    [InlineData(typeof(NullableClrElementColor?[]), "Array(String)")]
    // ClickHouse does not allow Nullable(Tuple(...)).
    [InlineData(typeof((int, int)?[]), "Array(Tuple(Int32, Int32))")]
    [InlineData(typeof(Dictionary<string, (int, string)?>), "Map(String, Tuple(Int32, String))")]
    public void FindMapping_ComponentWithoutNullableScalar_StoreTypeUnchanged(Type clrType, string expectedStoreType)
    {
        var mapping = GetTypeMappingSource().FindMapping(clrType);

        Assert.NotNull(mapping);
        Assert.Equal(expectedStoreType, mapping.StoreType);
    }

    [Fact]
    public void FindMapping_DictionaryWithNullableKey_KeyIsNotNullable()
    {
        // ClickHouse refuses a Nullable Map key (code 36). The type is made at run time because
        // the key of Dictionary<TKey, TValue> has the notnull constraint.
        var clrType = typeof(Dictionary<,>).MakeGenericType(typeof(int?), typeof(string));

        var mapping = GetTypeMappingSource().FindMapping(clrType);

        Assert.NotNull(mapping);
        Assert.Equal("Map(Int32, String)", mapping.StoreType);
    }

    [Fact]
    public void FindMapping_NullableValueTypeArray_ExplicitStoreTypeWins()
    {
        var mapping = Assert.IsType<ClickHouseArrayTypeMapping>(
            GetTypeMappingSource().FindMapping(typeof(int?[]), "Array(Int32)"));

        Assert.Equal("Array(Int32)", mapping.StoreType);
        Assert.IsNotType<ClickHouseNullableElementMapping>(mapping.ElementMapping);
    }

    [Fact]
    public void NullableElementMapping_OverDecimal_KeepsPrecisionAndScale()
    {
        var mapping = new ClickHouseNullableElementMapping(new ClickHouseDecimalTypeMapping(18, 4));

        Assert.Equal("Nullable(Decimal(18,4))", mapping.StoreType);
        Assert.Equal(18, mapping.Precision);
        Assert.Equal(4, mapping.Scale);
    }

    [Fact]
    public void FindMapping_ElementType_ReturnsSameMappingEachTime()
    {
        using var context = new NullableClrElementsDbContext(NoServer);
        var elementType = context.Model.FindEntityType(typeof(NullableClrElementsEntity))!
            .FindProperty(nameof(NullableClrElementsEntity.NInts))!
            .GetElementType()!;
        var source = context.GetService<IRelationalTypeMappingSource>();

        var first = source.FindMapping(elementType);
        var second = source.FindMapping(elementType);

        Assert.IsType<ClickHouseNullableElementMapping>(first);
        Assert.Same(first, second);
    }

    [Fact]
    public void Model_ConventionProperties_HaveNullableElementColumnTypes()
    {
        using var context = new NullableClrElementsDbContext(NoServer);
        var entityType = context.Model.FindEntityType(typeof(NullableClrElementsEntity))!;

        Assert.Equal("Array(Nullable(Int32))", entityType.FindProperty(nameof(NullableClrElementsEntity.NInts))!.GetColumnType());
        Assert.Equal("Array(Nullable(Int32))", entityType.FindProperty(nameof(NullableClrElementsEntity.NIntList))!.GetColumnType());

        var mapping = Assert.IsType<ClickHouseArrayTypeMapping>(
            entityType.FindProperty(nameof(NullableClrElementsEntity.NInts))!.GetTypeMapping());
        Assert.Equal(typeof(int?[]), mapping.ClrType);
        Assert.IsType<ClickHouseNullableElementMapping>(mapping.ElementMapping);
    }

    [Fact]
    public void GenerateCreateScript_ConventionProperties_HaveNullableElements()
    {
        using var context = new NullableClrElementsDbContext(NoServer);

        var script = context.Database.GenerateCreateScript();

        Assert.Contains("`n_ints` Array(Nullable(Int32))", script);
        Assert.Contains("`n_int_list` Array(Nullable(Int32))", script);
        Assert.Contains("`n_int_enumerable` Array(Nullable(Int32))", script);
        Assert.Contains("`n_int_nested` Array(Array(Nullable(Int32)))", script);
        Assert.Contains("`n_int_map` Map(String, Nullable(Int32))", script);
        Assert.Contains("`n_int_tuple` Tuple(Nullable(Int32), String)", script);
        Assert.Contains("`n_guids` Array(Nullable(UUID))", script);
        Assert.Contains("`n_dates` Array(Nullable(DateTime))", script);
        Assert.Contains("`ints` Array(Int32)", script);
        Assert.Contains("`strings` Array(String)", script);
    }

    [Theory]
    [InlineData("UUID", "Array(UUID)")]
    [InlineData("Nullable(UUID)", "Array(Nullable(UUID))")]
    [InlineData("LowCardinality(Nullable(UUID))", "Array(LowCardinality(Nullable(UUID)))")]
    public void GenerateCreateScript_ExplicitElementStoreType_IsKept(string elementStoreType, string expectedColumnType)
    {
        using var context = new NullableClrElementsElementStoreTypeDbContext(NoServer, elementStoreType);

        var script = context.Database.GenerateCreateScript();

        Assert.Contains($"`n_guids` {expectedColumnType}", script);
        Assert.Contains("`n_ints` Array(Nullable(Int32))", script);
    }

    /// <summary>
    /// The model snapshot of a migration has <c>HasColumnType(...)</c> with the column type of each
    /// property. The model that EF Core builds from the snapshot must have the same column types
    /// as the model of the context, or every migration finds pending model changes.
    /// </summary>
    [Fact]
    public void ModelWithSnapshotColumnTypes_HasNoDifferences()
    {
        using var context = new NullableClrElementsDbContext(NoServer);
        using var snapshotContext = new NullableClrElementsColumnTypesDbContext(NoServer, NullableClrElementsColumnTypesDbContext.NewTypes);
        var differ = context.GetService<IMigrationsModelDiffer>();

        var target = context.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var source = snapshotContext.GetService<IDesignTimeModel>().Model.GetRelationalModel();

        Assert.Empty(differ.GetDifferences(source, target));
    }

    [Fact]
    public void ModelWithOldColumnTypes_GivesAlterColumnOperations()
    {
        using var context = new NullableClrElementsDbContext(NoServer);
        using var oldContext = new NullableClrElementsColumnTypesDbContext(NoServer, NullableClrElementsColumnTypesDbContext.OldTypes);
        var differ = context.GetService<IMigrationsModelDiffer>();

        var operations = differ.GetDifferences(
            oldContext.GetService<IDesignTimeModel>().Model.GetRelationalModel(),
            context.GetService<IDesignTimeModel>().Model.GetRelationalModel());

        var alters = operations.Select(o => Assert.IsType<AlterColumnOperation>(o)).ToList();
        Assert.Equal(
            NullableClrElementsColumnTypesDbContext.NewTypes.OrderBy(kv => kv.Key),
            alters.Select(a => KeyValuePair.Create(a.Name, a.ColumnType!)).OrderBy(kv => kv.Key));
    }

    private static IRelationalTypeMappingSource GetTypeMappingSource()
    {
        var builder = new DbContextOptionsBuilder();
        builder.UseClickHouse(NoServer);
        using var ctx = new DbContext(builder.Options);
        return ((IInfrastructure<IServiceProvider>)ctx).Instance.GetRequiredService<IRelationalTypeMappingSource>();
    }
}

public class NullableClrElementFixture : IAsyncLifetime
{
    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        ConnectionString = await SharedContainer.GetConnectionStringAsync();

        await using var context = new NullableClrElementsDbContext(ConnectionString);
        await context.Database.EnsureCreatedAsync();

        context.Entities.AddRange(
            new NullableClrElementsEntity
            {
                Id = 1,
                NInts = [1, null, 3],
                NIntList = [1, null],
                NIntEnumerable = [4, null],
                NIntNested = [[1, null], []],
                NIntMap = new() { ["a"] = 1, ["b"] = null },
                NIntTuple = (1, "x"),
                NGuids = [Guid.Parse("11111111-2222-3333-4444-555555555555"), null],
                NDates = [new DateTime(2026, 1, 2, 3, 4, 5), null],
                Ints = [1],
                Strings = ["a"],
            },
            new NullableClrElementsEntity
            {
                Id = 2,
                NInts = [2],
                NIntList = [2],
                NIntEnumerable = [],
                NIntNested = [],
                NIntMap = new() { ["a"] = 2 },
                NIntTuple = (null, "y"),
                NGuids = [],
                NDates = [null],
                Ints = [],
                Strings = [],
            });
        await context.SaveChangesAsync();
    }

    public Task DisposeAsync() => Task.CompletedTask;
}

/// <summary>
/// Creates a table with <c>EnsureCreated</c> from a model without <c>HasColumnType</c>, writes rows
/// with NULL components with <c>SaveChanges</c>, and reads and filters them.
/// </summary>
public class NullableClrElementIntegrationTests : IClassFixture<NullableClrElementFixture>
{
    private readonly NullableClrElementFixture _fixture;

    public NullableClrElementIntegrationTests(NullableClrElementFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task EnsureCreated_ColumnTypes_HaveNullableElements()
    {
        await using var context = new NullableClrElementsDbContext(_fixture.ConnectionString);

        var types = await ReadColumnTypes(context);

        Assert.Equal(NullableClrElementsColumnTypesDbContext.NewTypes, types.Where(kv => kv.Key.StartsWith("n_")).ToDictionary());
        Assert.Equal("Array(Int32)", types["ints"]);
        Assert.Equal("Array(String)", types["strings"]);
    }

    [Fact]
    public async Task SaveChanges_ThenRead_KeepsNullComponents()
    {
        await using var context = new NullableClrElementsDbContext(_fixture.ConnectionString);

        var rows = await context.Entities.AsNoTracking().OrderBy(e => e.Id).ToListAsync();

        Assert.Equal(2, rows.Count);
        Assert.Equal([1, null, 3], rows[0].NInts);
        Assert.Equal([1, null], rows[0].NIntList);
        Assert.Equal([4, null], rows[0].NIntEnumerable);
        Assert.Equal(2, rows[0].NIntNested.Length);
        Assert.Equal([1, null], rows[0].NIntNested[0]);
        Assert.Empty(rows[0].NIntNested[1]);
        Assert.Equal(1, rows[0].NIntMap["a"]);
        Assert.True(rows[0].NIntMap.ContainsKey("b"));
        Assert.Null(rows[0].NIntMap["b"]);
        Assert.Equal((1, "x"), rows[0].NIntTuple);
        Assert.Equal([Guid.Parse("11111111-2222-3333-4444-555555555555"), null], rows[0].NGuids);
        Assert.Equal([new DateTime(2026, 1, 2, 3, 4, 5), null], rows[0].NDates);
        Assert.Equal([1], rows[0].Ints);

        Assert.Equal([2], rows[1].NInts);
        Assert.Equal((null, "y"), rows[1].NIntTuple);
        Assert.Equal([null], rows[1].NDates);
    }

    [Fact]
    public async Task ChangeTracking_ElementSetToNull_MarksEntityModified()
    {
        await using var context = new NullableClrElementsDbContext(_fixture.ConnectionString);
        var row = await context.Entities.SingleAsync(e => e.Id == 2);

        row.NInts[0] = null;
        row.NIntList.Add(null);
        context.ChangeTracker.DetectChanges();

        var entry = context.Entry(row);
        Assert.Equal(EntityState.Modified, entry.State);
        Assert.True(entry.Property(e => e.NInts).IsModified);
        Assert.True(entry.Property(e => e.NIntList).IsModified);
        Assert.False(entry.Property(e => e.NIntMap).IsModified);
    }

    [Fact]
    public async Task Lambda_CapturedNullableValue_UsesOneNullable()
    {
        await using var context = new NullableClrElementsDbContext(_fixture.ConnectionString);
        int? one = 1;

        var query = context.Entities.Where(e => e.NInts.Count(n => n == one) == 1).Select(e => e.Id);

        Assert.Contains("{one:Nullable(Int32)}", query.ToQueryString());
        Assert.Equal([1L], await query.ToListAsync());
    }

    [Fact]
    public async Task Lambda_NullElement_GivesDotNetResults()
    {
        await using var context = new NullableClrElementsDbContext(_fixture.ConnectionString);

        var nullCounts = await context.Entities.OrderBy(e => e.Id).Select(e => e.NInts.Count(n => n == null)).ToListAsync();
        Assert.Equal([1, 0], nullCounts);

        var notNullCounts = await context.Entities.OrderBy(e => e.Id).Select(e => e.NIntList.Count(n => n != null)).ToListAsync();
        Assert.Equal([1, 1], notNullCounts);

        var anyNull = await context.Entities.Where(e => e.NIntList.Any(n => n == null)).Select(e => e.Id).ToListAsync();
        Assert.Equal([1L], anyNull);

        var containsNull = await context.Entities.Where(e => e.NInts.Contains(null)).Select(e => e.Id).ToListAsync();
        Assert.Equal([1L], containsNull);
    }

    [Fact]
    public async Task Lambda_CapturedCollectionWithNull_MatchesNullElement()
    {
        await using var context = new NullableClrElementsDbContext(_fixture.ConnectionString);
        var ids = new int?[] { 3, null };

        var rows = await context.Entities
            .Where(e => e.NIntList.Any(n => ids.Contains(n)))
            .OrderBy(e => e.Id)
            .Select(e => e.Id)
            .ToListAsync();

        Assert.Equal([1L], rows);
    }

    [Fact]
    public async Task Equals_CapturedArrayWithNull_UsesColumnType()
    {
        await using var context = new NullableClrElementsDbContext(_fixture.ConnectionString);
        var value = new int?[] { 1, null, 3 };

        var query = context.Entities.Where(e => e.NInts == value).Select(e => e.Id);

        Assert.Contains("{value:Array(Nullable(Int32))}", query.ToQueryString());
        Assert.Equal([1L], await query.ToListAsync());
    }

    internal static async Task<Dictionary<string, string>> ReadColumnTypes(DbContext context)
        => (await context.Database
                .SqlQueryRaw<string>(
                    "SELECT concat(name, '|', type) AS Value FROM system.columns WHERE database = currentDatabase() AND table = 'nullable_clr_elements'")
                .ToListAsync())
            .Select(s => s.Split('|', 2))
            .ToDictionary(p => p[0], p => p[1]);
}

/// <summary>
/// A table made with the column types of the CLR-type mapping before <c>Nullable(T)</c>
/// components, for example <c>Array(Int32)</c> for <c>int?[]</c>, gets
/// <c>ALTER TABLE ... MODIFY COLUMN</c> from a migration, and ClickHouse keeps the data.
/// </summary>
public class NullableClrElementMigrationTests : IAsyncLifetime
{
    private string _connectionString = default!;

    public async Task InitializeAsync()
    {
        _connectionString = await SharedContainer.GetConnectionStringAsync();
    }

    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task AlterColumn_FromOldColumnTypes_KeepsData()
    {
        await using (var oldContext = new NullableClrElementsColumnTypesDbContext(
                         _connectionString, NullableClrElementsColumnTypesDbContext.OldTypes))
        {
            await oldContext.Database.EnsureCreatedAsync();
            await oldContext.Database.ExecuteSqlRawAsync(
                "INSERT INTO nullable_clr_elements VALUES (1, [1, 2], [3], [4], [[5]], {{'a': 6}}, (7, 'x'), " +
                "['11111111-2222-3333-4444-555555555555'], ['2026-01-02 03:04:05'], [8], ['s'])");

            await using var context = new NullableClrElementsDbContext(_connectionString);
            var operations = context.GetService<IMigrationsModelDiffer>().GetDifferences(
                oldContext.GetService<IDesignTimeModel>().Model.GetRelationalModel(),
                context.GetService<IDesignTimeModel>().Model.GetRelationalModel());
            var commands = context.GetService<IMigrationsSqlGenerator>().Generate(operations, context.Model);
            Assert.All(commands, c => Assert.Contains("MODIFY COLUMN", c.CommandText));
            await context.GetService<IMigrationCommandExecutor>()
                .ExecuteNonQueryAsync(commands, context.GetService<IRelationalConnection>());
        }

        await using (var context = new NullableClrElementsDbContext(_connectionString))
        {
            var types = await NullableClrElementIntegrationTests.ReadColumnTypes(context);
            Assert.Equal(NullableClrElementsColumnTypesDbContext.NewTypes, types.Where(kv => kv.Key.StartsWith("n_")).ToDictionary());

            context.Entities.Add(new NullableClrElementsEntity { Id = 2, NInts = [null], NIntTuple = (null, "y") });
            await context.SaveChangesAsync();
        }

        await using var readContext = new NullableClrElementsDbContext(_connectionString);
        var rows = await readContext.Entities.AsNoTracking().OrderBy(e => e.Id).ToListAsync();
        Assert.Equal([1, 2], rows[0].NInts);
        Assert.Equal([3], rows[0].NIntList);
        Assert.Equal([4], rows[0].NIntEnumerable);
        Assert.Equal([5], rows[0].NIntNested[0]);
        Assert.Equal(6, rows[0].NIntMap["a"]);
        Assert.Equal((7, "x"), rows[0].NIntTuple);
        Assert.Equal([Guid.Parse("11111111-2222-3333-4444-555555555555")], rows[0].NGuids);
        Assert.Equal([new DateTime(2026, 1, 2, 3, 4, 5)], rows[0].NDates);
        Assert.Equal([null], rows[1].NInts);
        Assert.Equal((null, "y"), rows[1].NIntTuple);
    }
}

public enum NullableClrElementColor
{
    Red,
    Green,
}

public class NullableClrElementsEntity
{
    public long Id { get; set; }
    public int?[] NInts { get; set; } = [];
    public List<int?> NIntList { get; set; } = [];
    public IEnumerable<int?> NIntEnumerable { get; set; } = [];
    public int?[][] NIntNested { get; set; } = [];
    public Dictionary<string, int?> NIntMap { get; set; } = [];
    public (int?, string) NIntTuple { get; set; }
    public Guid?[] NGuids { get; set; } = [];
    public DateTime?[] NDates { get; set; } = [];
    public int[] Ints { get; set; } = [];
    public string[] Strings { get; set; } = [];
}

/// <summary>
/// The model has no <c>HasColumnType</c>: the column types come from the CLR types.
/// </summary>
public class NullableClrElementsDbContext : DbContext
{
    public DbSet<NullableClrElementsEntity> Entities => Set<NullableClrElementsEntity>();

    private readonly string _connectionString;

    public NullableClrElementsDbContext(string connectionString) => _connectionString = connectionString;

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseClickHouse(_connectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<NullableClrElementsEntity>(entity =>
        {
            entity.ToTable("nullable_clr_elements", t => t.HasMergeTreeEngine().WithOrderBy("id"));
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.NInts).HasColumnName("n_ints");
            entity.Property(e => e.NIntList).HasColumnName("n_int_list");
            entity.Property(e => e.NIntEnumerable).HasColumnName("n_int_enumerable");
            entity.Property(e => e.NIntNested).HasColumnName("n_int_nested");
            entity.Property(e => e.NIntMap).HasColumnName("n_int_map");
            entity.Property(e => e.NIntTuple).HasColumnName("n_int_tuple");
            entity.Property(e => e.NGuids).HasColumnName("n_guids");
            entity.Property(e => e.NDates).HasColumnName("n_dates");
            entity.Property(e => e.Ints).HasColumnName("ints");
            entity.Property(e => e.Strings).HasColumnName("strings");
        });
    }
}

/// <summary>
/// The element of <c>NGuids</c> has an explicit store type.
/// </summary>
public class NullableClrElementsElementStoreTypeDbContext : NullableClrElementsDbContext
{
    private readonly string _elementStoreType;

    public NullableClrElementsElementStoreTypeDbContext(string connectionString, string elementStoreType)
        : base(connectionString)
    {
        _elementStoreType = elementStoreType;
    }

    internal string ModelKey => _elementStoreType;

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        base.OnConfiguring(optionsBuilder);
        optionsBuilder.ReplaceService<IModelCacheKeyFactory, NullableClrElementsModelCacheKeyFactory>();
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);
        modelBuilder.Entity<NullableClrElementsEntity>()
            .PrimitiveCollection(e => e.NGuids)
            .ElementType(t => t.HasStoreType(_elementStoreType));
    }
}

/// <summary>
/// Sets <c>HasColumnType</c> on the properties with Nullable components, as a model snapshot does.
/// </summary>
public class NullableClrElementsColumnTypesDbContext : NullableClrElementsDbContext
{
    public static readonly IReadOnlyDictionary<string, string> NewTypes = new Dictionary<string, string>
    {
        ["n_ints"] = "Array(Nullable(Int32))",
        ["n_int_list"] = "Array(Nullable(Int32))",
        ["n_int_enumerable"] = "Array(Nullable(Int32))",
        ["n_int_nested"] = "Array(Array(Nullable(Int32)))",
        ["n_int_map"] = "Map(String, Nullable(Int32))",
        ["n_int_tuple"] = "Tuple(Nullable(Int32), String)",
        ["n_guids"] = "Array(Nullable(UUID))",
        ["n_dates"] = "Array(Nullable(DateTime))",
    };

    /// <summary>The column types that the provider gave these properties before Nullable(T) components.</summary>
    public static readonly IReadOnlyDictionary<string, string> OldTypes = new Dictionary<string, string>
    {
        ["n_ints"] = "Array(Int32)",
        ["n_int_list"] = "Array(Int32)",
        ["n_int_enumerable"] = "Array(Int32)",
        ["n_int_nested"] = "Array(Array(Int32))",
        ["n_int_map"] = "Map(String, Int32)",
        ["n_int_tuple"] = "Tuple(Int32, String)",
        ["n_guids"] = "Array(UUID)",
        ["n_dates"] = "Array(DateTime)",
    };

    private readonly IReadOnlyDictionary<string, string> _columnTypes;

    public NullableClrElementsColumnTypesDbContext(string connectionString, IReadOnlyDictionary<string, string> columnTypes)
        : base(connectionString)
    {
        _columnTypes = columnTypes;
    }

    internal string ModelKey => string.Join(";", _columnTypes.Values);

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        base.OnConfiguring(optionsBuilder);
        optionsBuilder.ReplaceService<IModelCacheKeyFactory, NullableClrElementsModelCacheKeyFactory>();
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);
        modelBuilder.Entity<NullableClrElementsEntity>(entity =>
        {
            foreach (var property in entity.Metadata.GetProperties().ToList())
            {
                if (_columnTypes.TryGetValue(property.GetColumnName(), out var columnType))
                    entity.Property(property.Name).HasColumnType(columnType);
            }
        });
    }
}

/// <summary>
/// Gives each configuration of the test contexts its own model.
/// </summary>
internal sealed class NullableClrElementsModelCacheKeyFactory : IModelCacheKeyFactory
{
    public object Create(DbContext context, bool designTime)
        => context switch
        {
            NullableClrElementsElementStoreTypeDbContext c => (c.GetType(), c.ModelKey, designTime),
            NullableClrElementsColumnTypesDbContext c => (c.GetType(), c.ModelKey, designTime),
            _ => (context.GetType(), designTime),
        };
}
