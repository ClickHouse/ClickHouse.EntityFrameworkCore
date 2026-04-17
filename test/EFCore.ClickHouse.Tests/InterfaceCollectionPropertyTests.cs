using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.ClickHouse.Tests;

/// <summary>
/// Regression tests for PR #10 review finding 3: interface collection CLR types
/// (<see cref="IEnumerable{T}"/>, <see cref="IList{T}"/>, <see cref="IReadOnlyList{T}"/>, …)
/// used as entity properties must round-trip correctly end-to-end. Before the fix, only
/// <c>T[]</c> and <c>List&lt;T&gt;</c> had proper converter support; interface types were
/// recognized but fell through to a mapping whose CLR type was <c>T[]</c>, which would
/// not bind to an interface-typed property.
/// </summary>
public class InterfaceCollectionPropertyTests : IClassFixture<InterfaceCollectionFixture>
{
    private readonly InterfaceCollectionFixture _fixture;

    public InterfaceCollectionPropertyTests(InterfaceCollectionFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task IList_Property_RoundTrips()
    {
        await using var ctx = new InterfaceCollectionContext(_fixture.ConnectionString);
        var row = await ctx.Entities.FirstAsync(e => e.Id == 1);

        Assert.Equal(new[] { 1, 2, 3 }, row.IListTags);
        Assert.IsAssignableFrom<IList<int>>(row.IListTags);
    }

    [Fact]
    public async Task IReadOnlyList_Property_RoundTrips()
    {
        await using var ctx = new InterfaceCollectionContext(_fixture.ConnectionString);
        var row = await ctx.Entities.FirstAsync(e => e.Id == 1);

        Assert.Equal(new[] { 4, 5 }, row.IReadOnlyListTags);
        Assert.IsAssignableFrom<IReadOnlyList<int>>(row.IReadOnlyListTags);
    }

    [Fact]
    public async Task ICollection_Property_RoundTrips()
    {
        await using var ctx = new InterfaceCollectionContext(_fixture.ConnectionString);
        var row = await ctx.Entities.FirstAsync(e => e.Id == 1);

        Assert.Equal(new[] { 6, 7, 8 }, row.ICollectionTags);
        Assert.IsAssignableFrom<ICollection<int>>(row.ICollectionTags);
    }

    [Fact]
    public async Task IEnumerable_Property_RoundTrips()
    {
        await using var ctx = new InterfaceCollectionContext(_fixture.ConnectionString);
        var row = await ctx.Entities.FirstAsync(e => e.Id == 1);

        Assert.Equal(new[] { 9, 10 }, row.IEnumerableTags);
    }
}

public class InterfaceCollectionEntity
{
    public long Id { get; set; }
    public IList<int> IListTags { get; set; } = new List<int>();
    public IReadOnlyList<int> IReadOnlyListTags { get; set; } = Array.Empty<int>();
    public ICollection<int> ICollectionTags { get; set; } = new List<int>();
    public IEnumerable<int> IEnumerableTags { get; set; } = Array.Empty<int>();
}

public class InterfaceCollectionContext : DbContext
{
    public DbSet<InterfaceCollectionEntity> Entities => Set<InterfaceCollectionEntity>();

    private readonly string _connectionString;
    public InterfaceCollectionContext(string connectionString) => _connectionString = connectionString;

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseClickHouse(_connectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<InterfaceCollectionEntity>(e =>
        {
            e.ToTable("interface_collection_test");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.IListTags).HasColumnName("ilist_tags").HasColumnType("Array(Int32)");
            e.Property(x => x.IReadOnlyListTags).HasColumnName("ireadonlylist_tags").HasColumnType("Array(Int32)");
            e.Property(x => x.ICollectionTags).HasColumnName("icollection_tags").HasColumnType("Array(Int32)");
            e.Property(x => x.IEnumerableTags).HasColumnName("ienumerable_tags").HasColumnType("Array(Int32)");
        });
    }
}

public class InterfaceCollectionFixture : IAsyncLifetime
{
    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        ConnectionString = await SharedContainer.GetConnectionStringAsync();

        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(ConnectionString);
        await connection.OpenAsync();

        using var create = connection.CreateCommand();
        create.CommandText = """
            CREATE TABLE interface_collection_test (
                id Int64,
                ilist_tags Array(Int32),
                ireadonlylist_tags Array(Int32),
                icollection_tags Array(Int32),
                ienumerable_tags Array(Int32)
            ) ENGINE = MergeTree() ORDER BY id
            """;
        await create.ExecuteNonQueryAsync();

        using var seed = connection.CreateCommand();
        seed.CommandText = """
            INSERT INTO interface_collection_test VALUES
            (1, [1,2,3], [4,5], [6,7,8], [9,10])
            """;
        await seed.ExecuteNonQueryAsync();
    }

    public Task DisposeAsync() => Task.CompletedTask;
}
