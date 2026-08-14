using System.Linq.Expressions;
using ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Xunit;

namespace EFCore.ClickHouse.Tests;

/// <summary>
/// A composite mapping reads its whole column with <c>GetValue</c>, so the component mappings'
/// read conversions have to be applied per component. Both <see cref="DateTimeOffset"/> and
/// <see cref="DateOnly"/> arrive from the driver as <see cref="DateTime"/>, so without that the
/// whole composite fails to cast.
/// </summary>
public class CompositeElementEntity
{
    public long Id { get; set; }
    public DateTimeOffset[] Offsets { get; set; } = [];
    public DateOnly[] Dates { get; set; } = [];
    public List<DateTimeOffset> OffsetList { get; set; } = [];
    public DateTimeOffset[][] NestedOffsets { get; set; } = [];
    public Dictionary<string, DateTimeOffset> OffsetsByName { get; set; } = [];
    public Dictionary<DateOnly, string> NamesByDate { get; set; } = [];
    public Tuple<DateTimeOffset, string>? OffsetTuple { get; set; }
    public (DateTimeOffset When, int Count) OffsetValueTuple { get; set; }
    public int[] Ints { get; set; } = [];
    public Dictionary<string, int> Counts { get; set; } = [];
}

public class NullableCompositeElementEntity
{
    public long Id { get; set; }
    public DateTimeOffset?[] Offsets { get; set; } = [];
    public DateOnly?[] Dates { get; set; } = [];
}

public enum CompositeColour
{
    Red,
    Green,
    Blue
}

/// <summary>
/// Components that carry a <c>ValueConverter</c> rather than a data-reader conversion: an enum
/// converts through <c>EnumToStringConverter</c>, and a <c>List&lt;T&gt;</c> component through
/// <c>ListToArrayConverter</c>. The composite has to apply both, or the column becomes writable but
/// unreadable.
/// </summary>
public class ConvertedCompositeEntity
{
    public long Id { get; set; }
    public CompositeColour[] Colours { get; set; } = [];
    public Tuple<CompositeColour, int>? ColourTuple { get; set; }
    public Dictionary<string, CompositeColour> ColourByName { get; set; } = [];
    public Dictionary<string, List<int>> Buckets { get; set; } = [];
    public List<List<int>> Nested { get; set; } = [];
    public List<int>[] ListArray { get; set; } = [];
    public IList<DateTimeOffset> Interfaced { get; set; } = new List<DateTimeOffset>();
    public IReadOnlyList<DateOnly> ReadOnlyDates { get; set; } = [];

    // Components that need no conversion at all — these must keep the direct cast.
    public string[] Names { get; set; } = [];
    public double[] Ratios { get; set; } = [];
    public Dictionary<string, string> Labels { get; set; } = [];
}

public class CompositeElementDbContext : DbContext
{
    private readonly string _connectionString;

    public CompositeElementDbContext(string connectionString) => _connectionString = connectionString;

    public DbSet<CompositeElementEntity> Entities => Set<CompositeElementEntity>();
    public DbSet<NullableCompositeElementEntity> NullableEntities => Set<NullableCompositeElementEntity>();
    public DbSet<ConvertedCompositeEntity> ConvertedEntities => Set<ConvertedCompositeEntity>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseClickHouse(_connectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<CompositeElementEntity>(e =>
        {
            e.ToTable("composite_elements");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.Offsets).HasColumnName("offsets");
            e.Property(x => x.Dates).HasColumnName("dates");
            e.Property(x => x.OffsetList).HasColumnName("offset_list");
            e.Property(x => x.NestedOffsets).HasColumnName("nested_offsets");
            e.Property(x => x.OffsetsByName).HasColumnName("offsets_by_name");
            e.Property(x => x.NamesByDate).HasColumnName("names_by_date");
            e.Property(x => x.OffsetTuple).HasColumnName("offset_tuple");
            e.Property(x => x.OffsetValueTuple).HasColumnName("offset_value_tuple");
            e.Property(x => x.Ints).HasColumnName("ints");
            e.Property(x => x.Counts).HasColumnName("counts");
        });

        modelBuilder.Entity<NullableCompositeElementEntity>(e =>
        {
            e.ToTable("nullable_composite_elements");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            // The resolver takes element nullability from the store type. EF Core also models it on
            // IElementType.IsNullable for a primitive collection, which the resolver does not use yet.
            e.Property(x => x.Offsets).HasColumnName("offsets")
                .HasColumnType("Array(Nullable(DateTime64(7, 'UTC')))");
            e.Property(x => x.Dates).HasColumnName("dates")
                .HasColumnType("Array(Nullable(Date32))");
        });

        modelBuilder.Entity<ConvertedCompositeEntity>(e =>
        {
            e.ToTable("converted_composites");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.Colours).HasColumnName("colours")
                .HasColumnType("Array(Enum8('Red' = 1, 'Green' = 2, 'Blue' = 3))");
            e.Property(x => x.ColourTuple).HasColumnName("colour_tuple")
                .HasColumnType("Tuple(Enum8('Red' = 1, 'Green' = 2, 'Blue' = 3), Int32)");
            e.Property(x => x.ColourByName).HasColumnName("colour_by_name")
                .HasColumnType("Map(String, Enum8('Red' = 1, 'Green' = 2, 'Blue' = 3))");
            e.Property(x => x.Buckets).HasColumnName("buckets");
            e.Property(x => x.Nested).HasColumnName("nested");
            e.Property(x => x.ListArray).HasColumnName("list_array");
            e.Property(x => x.Interfaced).HasColumnName("interfaced");
            e.Property(x => x.ReadOnlyDates).HasColumnName("readonly_dates");
            e.Property(x => x.Names).HasColumnName("names");
            e.Property(x => x.Ratios).HasColumnName("ratios");
            e.Property(x => x.Labels).HasColumnName("labels");
        });
    }
}

public class ElementConverterEntity
{
    public long Id { get; set; }
    public string[] Tags { get; set; } = [];
}

/// <summary>
/// An element converter that keeps the CLR type, set through EF Core's public
/// <c>ElementType().HasConversion(...)</c> API.
/// </summary>
public class ElementConverterDbContext : DbContext
{
    private readonly string _connectionString;

    public ElementConverterDbContext(string connectionString) => _connectionString = connectionString;

    public DbSet<ElementConverterEntity> Entities => Set<ElementConverterEntity>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseClickHouse(_connectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
        => modelBuilder.Entity<ElementConverterEntity>(e =>
        {
            e.ToTable("element_converted");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.PrimitiveCollection(x => x.Tags).HasColumnName("tags")
                .ElementType(el => el.HasConversion(
                    new ValueConverter<string, string>(v => v, v => v + "!")));
        });
}

public class CompositeElementConversionFixture : IAsyncLifetime
{
    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        ConnectionString = await SharedContainer.GetConnectionStringAsync();
        using var ctx = new CompositeElementDbContext(ConnectionString);
        await ctx.Database.EnsureCreatedAsync();
    }

    public Task DisposeAsync() => Task.CompletedTask;
}

public class CompositeElementConversionTests : IClassFixture<CompositeElementConversionFixture>
{
    private readonly CompositeElementConversionFixture _fixture;

    public CompositeElementConversionTests(CompositeElementConversionFixture fixture)
        => _fixture = fixture;

    private static readonly DateTimeOffset Instant = new(2026, 1, 15, 5, 0, 0, TimeSpan.Zero);

    private static CompositeElementEntity NewRow(long id) => new()
    {
        Id = id,
        Offsets = [Instant, Instant.AddDays(1)],
        Dates = [new DateOnly(2026, 1, 15), new DateOnly(2026, 2, 20)],
        OffsetList = [Instant, Instant.AddHours(3)],
        NestedOffsets = [[Instant], [Instant.AddDays(2), Instant.AddDays(3)]],
        OffsetsByName = new Dictionary<string, DateTimeOffset> { ["start"] = Instant, ["end"] = Instant.AddDays(5) },
        NamesByDate = new Dictionary<DateOnly, string> { [new DateOnly(2026, 3, 1)] = "march" },
        OffsetTuple = Tuple.Create(Instant, "reference"),
        OffsetValueTuple = (Instant.AddDays(7), 42),
        Ints = [1, 2, 3],
        Counts = new Dictionary<string, int> { ["a"] = 1, ["b"] = 2 },
    };

    [Fact]
    public async Task Store_types_are_the_expected_composites()
    {
        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "DESCRIBE TABLE composite_elements";

        var columns = new Dictionary<string, string>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            columns[(string)reader.GetValue(0)] = (string)reader.GetValue(1);

        Assert.Equal("Array(DateTime64(7, 'UTC'))", columns["offsets"]);
        Assert.Equal("Array(Date32)", columns["dates"]);
        Assert.Equal("Array(Array(DateTime64(7, 'UTC')))", columns["nested_offsets"]);
        Assert.Equal("Map(String, DateTime64(7, 'UTC'))", columns["offsets_by_name"]);
        Assert.Equal("Map(Date32, String)", columns["names_by_date"]);
        Assert.Equal("Tuple(DateTime64(7, 'UTC'), String)", columns["offset_tuple"]);
    }

    [Fact]
    public async Task Every_composite_shape_round_trips()
    {
        using var writeContext = new CompositeElementDbContext(_fixture.ConnectionString);
        writeContext.Entities.Add(NewRow(1));
        await writeContext.SaveChangesAsync();

        using var readContext = new CompositeElementDbContext(_fixture.ConnectionString);
        var row = await readContext.Entities.SingleAsync(e => e.Id == 1);
        var expected = NewRow(1);

        Assert.Equal(expected.Offsets, row.Offsets);
        Assert.Equal(expected.Dates, row.Dates);
        Assert.Equal(expected.OffsetList, row.OffsetList);
        Assert.Equal(expected.OffsetsByName, row.OffsetsByName);
        Assert.Equal(expected.NamesByDate, row.NamesByDate);
        Assert.Equal(expected.OffsetTuple, row.OffsetTuple);
        Assert.Equal(expected.OffsetValueTuple, row.OffsetValueTuple);
        Assert.Equal(expected.Ints, row.Ints);
        Assert.Equal(expected.Counts, row.Counts);

        // Nested arrays compose: the element mapping is itself an array mapping.
        Assert.Equal(expected.NestedOffsets.Length, row.NestedOffsets.Length);
        for (var i = 0; i < expected.NestedOffsets.Length; i++)
            Assert.Equal(expected.NestedOffsets[i], row.NestedOffsets[i]);
    }

    /// <summary>Projecting the column alone exercises the mapping without entity materialization.</summary>
    [Fact]
    public async Task Projecting_a_composite_column_converts_its_elements()
    {
        using var writeContext = new CompositeElementDbContext(_fixture.ConnectionString);
        writeContext.Entities.Add(NewRow(2));
        await writeContext.SaveChangesAsync();

        using var ctx = new CompositeElementDbContext(_fixture.ConnectionString);

        Assert.Equal([Instant, Instant.AddDays(1)], await ctx.Entities.Where(e => e.Id == 2).Select(e => e.Offsets).SingleAsync());
        Assert.Equal([new DateOnly(2026, 1, 15), new DateOnly(2026, 2, 20)], await ctx.Entities.Where(e => e.Id == 2).Select(e => e.Dates).SingleAsync());
        Assert.Equal(Tuple.Create(Instant, "reference"), await ctx.Entities.Where(e => e.Id == 2).Select(e => e.OffsetTuple).SingleAsync());
    }

    [Fact]
    public async Task Empty_composites_round_trip()
    {
        using var writeContext = new CompositeElementDbContext(_fixture.ConnectionString);
        writeContext.Entities.Add(new CompositeElementEntity
        {
            Id = 3,
            Offsets = [],
            Dates = [],
            OffsetList = [],
            NestedOffsets = [],
            OffsetsByName = [],
            NamesByDate = [],
            OffsetTuple = Tuple.Create(Instant, "only"),
            OffsetValueTuple = (Instant, 0),
            Ints = [],
            Counts = [],
        });
        await writeContext.SaveChangesAsync();

        using var readContext = new CompositeElementDbContext(_fixture.ConnectionString);
        var row = await readContext.Entities.SingleAsync(e => e.Id == 3);

        Assert.Empty(row.Offsets);
        Assert.Empty(row.Dates);
        Assert.Empty(row.OffsetList);
        Assert.Empty(row.NestedOffsets);
        Assert.Empty(row.OffsetsByName);
    }

    /// <summary>
    /// The nullable wrapper must add exactly one <c>Nullable(...)</c>. The inner mapping is resolved
    /// from a store type that already carries the wrapper, and that text is preserved verbatim, so
    /// wrapping again gave <c>Array(Nullable(Nullable(T)))</c> — DDL that ClickHouse rejects.
    /// </summary>
    [Fact]
    public async Task Nullable_element_store_type_is_not_double_wrapped()
    {
        using var ctx = new CompositeElementDbContext(_fixture.ConnectionString);
        var entityType = ctx.Model.FindEntityType(typeof(NullableCompositeElementEntity))!;

        Assert.Equal(
            "Array(Nullable(DateTime64(7, 'UTC')))",
            entityType.FindProperty(nameof(NullableCompositeElementEntity.Offsets))!.GetColumnType());
        Assert.Equal(
            "Array(Nullable(Date32))",
            entityType.FindProperty(nameof(NullableCompositeElementEntity.Dates))!.GetColumnType());

        // And the table really exists with that shape, so EnsureCreated accepted the DDL.
        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "DESCRIBE TABLE nullable_composite_elements";

        var columns = new Dictionary<string, string>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            columns[(string)reader.GetValue(0)] = (string)reader.GetValue(1);

        Assert.Equal("Array(Nullable(DateTime64(7, 'UTC')))", columns["offsets"]);
        Assert.Equal("Array(Nullable(Date32))", columns["dates"]);
    }

    /// <summary>
    /// <c>Array(Nullable(T))</c> goes through <c>ClickHouseNullableElementMapping</c>, which must
    /// delegate the read conversion to the inner mapping while nulls pass straight through.
    /// </summary>
    [Fact]
    public async Task Nullable_elements_round_trip_with_nulls()
    {
        using var writeContext = new CompositeElementDbContext(_fixture.ConnectionString);
        writeContext.NullableEntities.Add(new NullableCompositeElementEntity
        {
            Id = 1,
            Offsets = [Instant, null, Instant.AddDays(1)],
            Dates = [new DateOnly(2026, 1, 15), null],
        });
        await writeContext.SaveChangesAsync();

        using var readContext = new CompositeElementDbContext(_fixture.ConnectionString);
        var row = await readContext.NullableEntities.SingleAsync(e => e.Id == 1);

        Assert.Equal([Instant, null, Instant.AddDays(1)], row.Offsets);
        Assert.Equal([new DateOnly(2026, 1, 15), null], row.Dates);
    }

    /// <summary>
    /// A component that needs no conversion must keep the direct cast, so the common case pays
    /// nothing. Asserted on the shape of the read expression rather than on a round trip, because a
    /// round trip passes either way. <c>String</c> and <c>Float64</c> convert nothing; note that the
    /// integer mappings <em>do</em> (they widen with <c>Convert.ToInt32</c> for aggregates), so
    /// <c>int[]</c> is not a valid example here.
    /// </summary>
    [Theory]
    [InlineData(nameof(ConvertedCompositeEntity.Names))]
    [InlineData(nameof(ConvertedCompositeEntity.Ratios))]
    [InlineData(nameof(ConvertedCompositeEntity.Labels))]
    public void Components_needing_no_conversion_keep_the_direct_cast(string propertyName)
    {
        using var ctx = new CompositeElementDbContext(_fixture.ConnectionString);
        var mapping = ctx.Model
            .FindEntityType(typeof(ConvertedCompositeEntity))!
            .FindProperty(propertyName)!
            .GetRelationalTypeMapping();

        var read = mapping.CustomizeDataReaderExpression(Expression.Parameter(typeof(object), "v"));

        // A direct cast is a UnaryExpression; the rebuild path emits a Call to a Convert* helper.
        Assert.IsAssignableFrom<UnaryExpression>(read);
    }

    /// <summary>The counterpart: a converting component must take the rebuild path.</summary>
    [Theory]
    [InlineData(nameof(CompositeElementEntity.Offsets))]
    [InlineData(nameof(CompositeElementEntity.Dates))]
    [InlineData(nameof(CompositeElementEntity.OffsetsByName))]
    [InlineData(nameof(CompositeElementEntity.Ints))]
    public void Converting_components_take_the_rebuild_path(string propertyName)
    {
        using var ctx = new CompositeElementDbContext(_fixture.ConnectionString);
        var mapping = ctx.Model
            .FindEntityType(typeof(CompositeElementEntity))!
            .FindProperty(propertyName)!
            .GetRelationalTypeMapping();

        var read = mapping.CustomizeDataReaderExpression(Expression.Parameter(typeof(object), "v"));

        Assert.IsAssignableFrom<MethodCallExpression>(read);
    }

    /// <summary>
    /// The per-component converter must be embedded as a constant, not as an inline lambda. An inline
    /// lambda is rebuilt on every materialization, which allocates a delegate per row for every
    /// composite column that converts — including ones where the runtime fast path then returns the
    /// driver's array untouched.
    /// </summary>
    [Theory]
    [InlineData(nameof(CompositeElementEntity.Offsets))]
    [InlineData(nameof(CompositeElementEntity.Ints))]
    [InlineData(nameof(CompositeElementEntity.OffsetsByName))]
    public void Component_converters_are_embedded_as_constants(string propertyName)
    {
        using var ctx = new CompositeElementDbContext(_fixture.ConnectionString);
        var mapping = ctx.Model
            .FindEntityType(typeof(CompositeElementEntity))!
            .FindProperty(propertyName)!
            .GetRelationalTypeMapping();

        var read = (MethodCallExpression)mapping.CustomizeDataReaderExpression(
            Expression.Parameter(typeof(object), "v"));

        // Argument 0 is the raw value; every argument after it is a component converter.
        Assert.All(
            read.Arguments.Skip(1),
            argument => Assert.Equal(ExpressionType.Constant, argument.NodeType));
    }

    // --- components carrying a ValueConverter -------------------------------

    private static ConvertedCompositeEntity NewConvertedRow(long id) => new()
    {
        Id = id,
        Colours = [CompositeColour.Red, CompositeColour.Blue],
        ColourTuple = Tuple.Create(CompositeColour.Blue, 7),
        ColourByName = new Dictionary<string, CompositeColour> { ["primary"] = CompositeColour.Green },
        Buckets = new Dictionary<string, List<int>> { ["low"] = [1, 2], ["high"] = [9] },
        Nested = [[1, 2], [3]],
        ListArray = [[4, 5], []],
        Interfaced = new List<DateTimeOffset> { Instant, Instant.AddDays(1) },
        ReadOnlyDates = [new DateOnly(2026, 4, 1)],
        Names = ["a", "b"],
        Ratios = [1.5, 2.5],
        Labels = new Dictionary<string, string> { ["k"] = "v" },
    };

    /// <summary>
    /// A component mapping can convert through a <c>ValueConverter</c> instead of a data-reader
    /// conversion. The composite must apply that too — otherwise the column is writable but not
    /// readable, which is worse than refusing the model up front.
    /// </summary>
    /// <remarks>
    /// Seeded with raw SQL on purpose. Writing a converter-bearing component through
    /// <c>SaveChanges</c> does not work yet: the bulk insert path passes model values straight to the
    /// driver without applying the converter, so an enum component is written as its raw ordinal
    /// (issue #54). This test covers the read direction, which is what the composite conversion
    /// fixes.
    /// </remarks>
    [Fact]
    public async Task Components_with_a_value_converter_read_correctly()
    {
        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        using var insert = connection.CreateCommand();
        insert.CommandText = """
            INSERT INTO converted_composites
                (id, colours, colour_tuple, colour_by_name, buckets, nested, list_array,
                 interfaced, readonly_dates, names, ratios, labels)
            VALUES
                (1, ['Red', 'Blue'], ('Blue', 7), {'primary': 'Green'},
                 {'low': [1, 2], 'high': [9]}, [[1, 2], [3]], [[4, 5], []],
                 ['2026-01-15 05:00:00.0000000', '2026-01-16 05:00:00.0000000'],
                 ['2026-04-01'], ['a', 'b'], [1.5, 2.5], {'k': 'v'})
            """;
        await insert.ExecuteNonQueryAsync();

        using var readContext = new CompositeElementDbContext(_fixture.ConnectionString);
        var row = await readContext.ConvertedEntities.SingleAsync(e => e.Id == 1);
        var expected = NewConvertedRow(1);

        // EnumToStringConverter components.
        Assert.Equal(expected.Colours, row.Colours);
        Assert.Equal(expected.ColourTuple, row.ColourTuple);
        Assert.Equal(expected.ColourByName, row.ColourByName);

        // ListToArrayConverter components, including inside a Map and nested one level.
        Assert.Equal(expected.Buckets, row.Buckets);
        Assert.Equal(expected.Nested, row.Nested);
        Assert.Equal(expected.ListArray.Length, row.ListArray.Length);
        for (var i = 0; i < expected.ListArray.Length; i++)
            Assert.Equal(expected.ListArray[i], row.ListArray[i]);

        // Collection-interface components go through EnumerableToArrayConverter.
        Assert.Equal(expected.Interfaced, row.Interfaced);
        Assert.Equal(expected.ReadOnlyDates, row.ReadOnlyDates);

        // And the no-conversion components still work.
        Assert.Equal(expected.Names, row.Names);
        Assert.Equal(expected.Ratios, row.Ratios);
        Assert.Equal(expected.Labels, row.Labels);
    }

    [Fact]
    public async Task Enum_component_store_types_are_as_configured()
    {
        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "DESCRIBE TABLE converted_composites";

        var columns = new Dictionary<string, string>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            columns[(string)reader.GetValue(0)] = (string)reader.GetValue(1);

        Assert.Equal("Array(Enum8('Red' = 1, 'Green' = 2, 'Blue' = 3))", columns["colours"]);
        Assert.Equal("Map(String, Enum8('Red' = 1, 'Green' = 2, 'Blue' = 3))", columns["colour_by_name"]);
        Assert.Equal("Map(String, Array(Int32))", columns["buckets"]);
        Assert.Equal("Array(Array(Int32))", columns["nested"]);
    }

    // --- the already-typed pass-through -------------------------------------

    /// <summary>
    /// Reading a composite keeps a fast path for a driver value that already has the target CLR
    /// type. That is sound only where matching types prove there is no work left, which a
    /// <c>ValueConverter</c> can break: it may change the value and keep the CLR type, so the
    /// driver's array is already <c>string[]</c> while <c>ConvertFromProvider</c> still has to run.
    /// </summary>
    /// <remarks>
    /// No mapping the provider resolves on its own is shaped this way — every converter it uses also
    /// changes the CLR type — but the shape is reachable from the public API through
    /// <c>ElementType().HasConversion(...)</c>, which the model test below covers. This one drives
    /// the mapping directly so the fast path is exercised without a model.
    /// </remarks>
    [Fact]
    public void A_same_clr_type_component_converter_is_not_skipped_by_the_fast_path()
    {
        using var ctx = new CompositeElementDbContext(_fixture.ConnectionString);
        var source = ctx.GetService<IRelationalTypeMappingSource>();
        var stringMapping = source.FindMapping(typeof(string), "String")!;

        // A converter that keeps the CLR type but changes the value.
        var elementMapping = (RelationalTypeMapping)stringMapping.WithComposedConverter(
            new ValueConverter<string, string>(v => v, v => v + "!"));
        var arrayMapping = new ClickHouseArrayTypeMapping(elementMapping);

        Assert.Equal(typeof(string[]), arrayMapping.ClrType);
        Assert.NotNull(elementMapping.Converter);

        // The driver hands back string[], which is already the target type.
        var read = Read<string[]>(arrayMapping, new[] { "a", "b" });

        Assert.Equal(["a!", "b!"], read);
    }

    /// <summary>
    /// The fast path must survive for the case it exists to serve: a component whose read is a cast
    /// and nothing more, as in a nested array. Here the driver's value is returned as it stands.
    /// </summary>
    [Fact]
    public void An_already_typed_component_with_no_converter_passes_straight_through()
    {
        using var ctx = new CompositeElementDbContext(_fixture.ConnectionString);
        var source = ctx.GetService<IRelationalTypeMappingSource>();
        var innerArray = source.FindMapping(typeof(int[]), "Array(Int32)")!;
        var outerArray = new ClickHouseArrayTypeMapping(innerArray);

        // No converter, so a matching CLR type is proof enough that nothing is left to do.
        Assert.Null(innerArray.Converter);

        var driverValue = new[] { new[] { 1, 2 }, new[] { 3 } };
        var read = Read<int[][]>(outerArray, driverValue);

        Assert.Same(driverValue, read);
    }

    /// <summary>
    /// The same hazard for a <c>Map</c>. Both the key and the value mapping must be checked, so this
    /// puts the converter on the value and leaves the key alone.
    /// </summary>
    [Fact]
    public void A_same_clr_type_map_value_converter_is_not_skipped_by_the_fast_path()
    {
        using var ctx = new CompositeElementDbContext(_fixture.ConnectionString);
        var source = ctx.GetService<IRelationalTypeMappingSource>();
        var stringMapping = source.FindMapping(typeof(string), "String")!;

        var valueMapping = (RelationalTypeMapping)stringMapping.WithComposedConverter(
            new ValueConverter<string, string>(v => v, v => v + "!"));
        var mapMapping = new ClickHouseMapTypeMapping(stringMapping, valueMapping);

        // The driver hands back the target dictionary type already.
        var read = Read<Dictionary<string, string>>(
            mapMapping,
            new Dictionary<string, string> { ["k"] = "a" });

        Assert.Equal("a!", read["k"]);
    }

    /// <summary>
    /// The same hazard for a <c>Tuple</c>. A reference tuple is used, because a <c>ValueTuple</c>
    /// target never reaches the fast path — the driver returns <c>System.Tuple&lt;&gt;</c>.
    /// </summary>
    [Fact]
    public void A_same_clr_type_tuple_component_converter_is_not_skipped_by_the_fast_path()
    {
        using var ctx = new CompositeElementDbContext(_fixture.ConnectionString);
        var source = ctx.GetService<IRelationalTypeMappingSource>();
        var stringMapping = source.FindMapping(typeof(string), "String")!;

        var componentMapping = (RelationalTypeMapping)stringMapping.WithComposedConverter(
            new ValueConverter<string, string>(v => v, v => v + "!"));
        var tupleMapping = new ClickHouseTupleTypeMapping(
            [componentMapping, componentMapping],
            useValueTuple: false);

        Assert.Equal(typeof(Tuple<string, string>), tupleMapping.ClrType);

        var read = Read<Tuple<string, string>>(tupleMapping, Tuple.Create("a", "b"));

        Assert.Equal(Tuple.Create("a!", "b!"), read);
    }

    /// <summary>
    /// The reachable route to the same shape: an element converter set through the public API. This
    /// is why the gate matters rather than being defence against a shape nobody can build.
    /// </summary>
    [Fact]
    public async Task An_element_converter_set_on_the_model_is_applied_on_read()
    {
        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // The fixture already created the database, so EnsureCreated would add nothing. Written
        // outside EF anyway, because SaveChanges does not apply converters yet (#54).
        using var create = connection.CreateCommand();
        create.CommandText =
            "CREATE TABLE IF NOT EXISTS element_converted (id Int64, tags Array(String)) "
            + "ENGINE = MergeTree ORDER BY id";
        await create.ExecuteNonQueryAsync();

        using var command = connection.CreateCommand();
        command.CommandText = "INSERT INTO element_converted VALUES (1, ['a', 'b'])";
        await command.ExecuteNonQueryAsync();

        using var readContext = new ElementConverterDbContext(_fixture.ConnectionString);
        var row = await readContext.Entities.SingleAsync(e => e.Id == 1);

        // Without the gate the driver's raw string[] would come straight through as "a", "b".
        Assert.Equal(["a!", "b!"], row.Tags);
    }

    /// <summary>Compiles and runs a mapping's data-reader expression over one driver value.</summary>
    private static T Read<T>(RelationalTypeMapping mapping, object driverValue)
    {
        var parameter = Expression.Parameter(typeof(object), "value");
        var body = mapping.CustomizeDataReaderExpression(parameter);

        return Expression.Lambda<Func<object, T>>(Expression.Convert(body, typeof(T)), parameter)
            .Compile()(driverValue);
    }
}
