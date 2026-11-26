using ClickHouse.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Xunit;

namespace EFCore.ClickHouse.Tests;

public enum ComponentColour
{
    Red = 0,
    Green = 1
}

/// <summary>
/// Components whose CLR type a composite must take from the model rather than from the store type's
/// default. Each property here resolved to the wrong element type before, and the query then failed
/// to compile with a coercion error naming a type the user never wrote.
/// </summary>
public class ComponentClrTypeEntity
{
    public long Id { get; set; }

    /// <summary>A converter-backed component: the enum mapping's converter used to erase the
    /// <c>Nullable&lt;&gt;</c>, so this resolved as <c>ComponentColour[]</c>.</summary>
    public ComponentColour?[] Colours { get; set; } = [];

    /// <summary><c>Date32</c> serves both <see cref="DateOnly"/> and <see cref="DateTime"/>, and the
    /// store type's default won.</summary>
    public DateTime[] Timestamps { get; set; } = [];

    /// <summary>ClickHouse accepts and normalizes this spelling; the provider's parser did not.</summary>
    public DateOnly?[] Spaced { get; set; } = [];

    /// <summary>A column made outside EF Core can have an enum label that the C# enum does not
    /// have, and the label can hold parentheses and an escaped quote.</summary>
    public ComponentColour?[] Labelled { get; set; } = [];
}

public class ComponentClrTypeDbContext(string connectionString) : DbContext
{
    public DbSet<ComponentClrTypeEntity> Entities => Set<ComponentClrTypeEntity>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseClickHouse(connectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
        => modelBuilder.Entity<ComponentClrTypeEntity>(e =>
        {
            e.ToTable("component_clr_types", t => t.HasMergeTreeEngine().WithOrderBy("id"));
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.Colours).HasColumnName("colours")
                .HasColumnType("Array(Nullable(Enum8('Red' = 0, 'Green' = 1)))");
            e.Property(x => x.Timestamps).HasColumnName("timestamps").HasColumnType("Array(Date32)");
            e.Property(x => x.Spaced).HasColumnName("spaced").HasColumnType("Array( Nullable ( Date32 ) )");
            e.Property(x => x.Labelled).HasColumnName("labelled")
                .HasColumnType(LabelledStoreType);
        });

    /// <summary>A quote inside a label in both spellings that ClickHouse accepts: <c>\'</c>, which
    /// the server itself writes, and <c>''</c>.</summary>
    public const string LabelledStoreType =
        "Array(Nullable(Enum8('Red' = 0, 'Green' = 1, 'Gone (it\\'s old)' = 2, 'Kept (it''s new)' = 3)))";
}

public class ComponentClrTypeFixture : IAsyncLifetime
{
    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        ConnectionString = await SharedContainer.GetConnectionStringAsync();
        using var ctx = new ComponentClrTypeDbContext(ConnectionString);
        await ctx.Database.EnsureCreatedAsync();
    }

    public Task DisposeAsync() => Task.CompletedTask;
}

public class CompositeComponentClrTypeTests(ComponentClrTypeFixture fixture)
    : IClassFixture<ComponentClrTypeFixture>
{
    private Type MappedClrTypeOf(string propertyName)
    {
        using var ctx = new ComponentClrTypeDbContext(fixture.ConnectionString);
        return ctx.Model.FindEntityType(typeof(ComponentClrTypeEntity))!
            .FindProperty(propertyName)!
            .FindRelationalTypeMapping()!
            .ClrType;
    }

    /// <summary>
    /// A converter on the component mapping made EF Core take the mapping's CLR type from the
    /// converter's model type, discarding the <c>Nullable&lt;&gt;</c> the wrapper asked for.
    /// </summary>
    [Fact]
    public void A_converter_backed_component_keeps_its_nullable_clr_type()
        => Assert.Equal(typeof(ComponentColour?[]), MappedClrTypeOf(nameof(ComponentClrTypeEntity.Colours)));

    /// <summary>One store type serves several CLR types, so the model's choice has to win.</summary>
    [Fact]
    public void A_component_resolves_as_the_clr_type_the_model_declares()
        => Assert.Equal(typeof(DateTime[]), MappedClrTypeOf(nameof(ComponentClrTypeEntity.Timestamps)));

    /// <summary>
    /// ClickHouse tolerates whitespace before an argument list and normalizes it away, so the
    /// provider's parsers must agree with each other on such a spelling.
    /// </summary>
    [Fact]
    public void Whitespace_in_a_store_type_still_yields_a_nullable_element()
        => Assert.Equal(typeof(DateOnly?[]), MappedClrTypeOf(nameof(ComponentClrTypeEntity.Spaced)));

    /// <summary>
    /// A parenthesis inside a quoted enum label is not part of the type structure. Counting it made
    /// the provider miss the <c>Nullable(...)</c> wrapper, so the element lost its nullable CLR type.
    /// </summary>
    [Fact]
    public void A_parenthesis_in_an_enum_label_keeps_the_nullable_element()
        => Assert.Equal(typeof(ComponentColour?[]), MappedClrTypeOf(nameof(ComponentClrTypeEntity.Labelled)));

    /// <summary>The same column must keep its DDL, which ClickHouse accepted in the fixture.</summary>
    [Fact]
    public void A_parenthesis_in_an_enum_label_keeps_the_store_type()
    {
        using var ctx = new ComponentClrTypeDbContext(fixture.ConnectionString);
        var property = ctx.Model.FindEntityType(typeof(ComponentClrTypeEntity))!
            .FindProperty(nameof(ComponentClrTypeEntity.Labelled))!;

        Assert.Equal(ComponentClrTypeDbContext.LabelledStoreType, property.GetColumnType());
        Assert.Equal(ComponentClrTypeDbContext.LabelledStoreType, property.FindRelationalTypeMapping()!.StoreType);
    }

    /// <summary>Every property above must also survive query compilation and materialization.</summary>
    [Fact]
    public async Task Components_round_trip_through_the_database()
    {
        var timestamps = new[] { new DateTime(2026, 1, 15), new DateTime(2026, 6, 30) };
        var spaced = new DateOnly?[] { new DateOnly(2026, 2, 1), null };

        using (var write = new ComponentClrTypeDbContext(fixture.ConnectionString))
        {
            write.Entities.Add(new ComponentClrTypeEntity
            {
                Id = 1,
                // An enum inside a composite is still written as its raw ordinal (#54), so the
                // round trip asserted here is on the components that need no converter.
                Colours = [ComponentColour.Red, null],
                Timestamps = timestamps,
                Spaced = spaced,
                Labelled = [ComponentColour.Green, null]
            });
            await write.SaveChangesAsync();
        }

        using var read = new ComponentClrTypeDbContext(fixture.ConnectionString);
        var row = await read.Entities.SingleAsync(x => x.Id == 1);

        Assert.Equal(timestamps, row.Timestamps);
        Assert.Equal(spaced, row.Spaced);
    }

}
