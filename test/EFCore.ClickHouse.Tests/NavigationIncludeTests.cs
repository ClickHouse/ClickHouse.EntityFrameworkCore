using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.ClickHouse.Tests;

#region Entities

public class IncludeParent
{
    public long Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public List<IncludeChild> Children { get; set; } = [];
}

public class IncludeChild
{
    public long Id { get; set; }
    public long ParentId { get; set; }
    public string Name { get; set; } = string.Empty;
    public IncludeParent? Parent { get; set; }
    public List<IncludeGrandChild> GrandChildren { get; set; } = [];
}

public class IncludeGrandChild
{
    public long Id { get; set; }
    public long ChildId { get; set; }
    public string Name { get; set; } = string.Empty;
    public List<IncludeGreatGrandChild> GreatGrandChildren { get; set; } = [];
}

public class IncludeGreatGrandChild
{
    public long Id { get; set; }
    public long GrandChildId { get; set; }
    public string Name { get; set; } = string.Empty;
}

public class IncludeDbContext : DbContext
{
    public DbSet<IncludeParent> Parents => Set<IncludeParent>();
    public DbSet<IncludeChild> Children => Set<IncludeChild>();

    private readonly string _connectionString;

    public IncludeDbContext(string connectionString)
    {
        _connectionString = connectionString;
    }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseClickHouse(_connectionString);
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<IncludeParent>(entity =>
        {
            entity.ToTable("include_parents");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.Name).HasColumnName("name");
            entity.HasMany(e => e.Children).WithOne(e => e.Parent).HasForeignKey(e => e.ParentId);
        });

        modelBuilder.Entity<IncludeChild>(entity =>
        {
            entity.ToTable("include_children");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.ParentId).HasColumnName("parent_id");
            entity.Property(e => e.Name).HasColumnName("name");
            entity.HasMany(e => e.GrandChildren).WithOne().HasForeignKey(e => e.ChildId);
        });

        modelBuilder.Entity<IncludeGrandChild>(entity =>
        {
            entity.ToTable("include_grandchildren");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.ChildId).HasColumnName("child_id");
            entity.Property(e => e.Name).HasColumnName("name");
            entity.HasMany(e => e.GreatGrandChildren).WithOne().HasForeignKey(e => e.GrandChildId);
        });

        modelBuilder.Entity<IncludeGreatGrandChild>(entity =>
        {
            entity.ToTable("include_greatgrandchildren");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.GrandChildId).HasColumnName("grandchild_id");
            entity.Property(e => e.Name).HasColumnName("name");
        });
    }
}

#endregion

public class IncludeFixture : IAsyncLifetime
{
    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        ConnectionString = await SharedContainer.GetConnectionStringAsync();

        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(ConnectionString);
        await connection.OpenAsync();

        string[] statements =
        [
            "CREATE TABLE include_parents (id Int64, name String) ENGINE = MergeTree() ORDER BY id",
            "CREATE TABLE include_children (id Int64, parent_id Int64, name String) ENGINE = MergeTree() ORDER BY id",
            "CREATE TABLE include_grandchildren (id Int64, child_id Int64, name String) ENGINE = MergeTree() ORDER BY id",
            "CREATE TABLE include_greatgrandchildren (id Int64, grandchild_id Int64, name String) ENGINE = MergeTree() ORDER BY id",
            "INSERT INTO include_parents VALUES (1, 'P1'), (2, 'P2'), (3, 'P3')",
            // Parent 3 has no children. Child 12 has no grandchildren.
            "INSERT INTO include_children VALUES (11, 1, 'C11'), (12, 1, 'C12'), (21, 2, 'C21')",
            // Grandchild 113 has no great-grandchildren.
            "INSERT INTO include_grandchildren VALUES (111, 11, 'G111'), (112, 11, 'G112'), (113, 11, 'G113'), (211, 21, 'G211')",
            "INSERT INTO include_greatgrandchildren VALUES (1111, 111, 'GG1111'), (1112, 111, 'GG1112'), (1121, 112, 'GG1121'), (2111, 211, 'GG2111')",
        ];

        foreach (var statement in statements)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = statement;
            await cmd.ExecuteNonQueryAsync();
        }
    }

    public Task DisposeAsync() => Task.CompletedTask;
}

/// <summary>
/// Include / ThenInclude over collection navigations. EF Core loads these with
/// LEFT JOINs to subqueries, and the outer query reads the subquery columns by name.
/// </summary>
public class NavigationIncludeTests : IClassFixture<IncludeFixture>
{
    private readonly IncludeFixture _fixture;

    public NavigationIncludeTests(IncludeFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Include_ThenInclude_SingleOrDefault()
    {
        await using var ctx = new IncludeDbContext(_fixture.ConnectionString);

        var parent = await ctx.Parents
            .Where(p => p.Id == 1L)
            .Include(p => p.Children)
            .ThenInclude(c => c.GrandChildren)
            .SingleOrDefaultAsync();

        AssertParent1(parent);
    }

    [Fact]
    public async Task Include_ThenInclude_ThenInclude_ToList()
    {
        await using var ctx = new IncludeDbContext(_fixture.ConnectionString);

        var parents = await ctx.Parents
            .Include(p => p.Children)
            .ThenInclude(c => c.GrandChildren)
            .ThenInclude(g => g.GreatGrandChildren)
            .OrderBy(p => p.Id)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal([1L, 2L, 3L], parents.Select(p => p.Id));
        Assert.Equal([11L, 12L], parents[0].Children.Select(c => c.Id).Order());
        Assert.Equal([21L], parents[1].Children.Select(c => c.Id));
        Assert.Empty(parents[2].Children);

        var c11 = parents[0].Children.Single(c => c.Id == 11);
        Assert.Equal([111L, 112L, 113L], c11.GrandChildren.Select(g => g.Id).Order());
        Assert.Equal([1111L, 1112L], c11.GrandChildren.Single(g => g.Id == 111).GreatGrandChildren.Select(x => x.Id).Order());
        Assert.Equal([1121L], c11.GrandChildren.Single(g => g.Id == 112).GreatGrandChildren.Select(x => x.Id));
        Assert.Empty(c11.GrandChildren.Single(g => g.Id == 113).GreatGrandChildren);
    }

    // The parent comes from the second table of a join (children INNER JOIN parents).
    // SingleOrDefault moves that join into a LIMIT 2 subquery that projects `id` and
    // `name` of the second table. The first table also has `id` and `name`, so ClickHouse
    // names an unaliased `p.id` as `p.id`. The outer query reads the column as `id`.
    [Fact]
    public async Task Include_ThenInclude_on_entity_from_joined_table_SingleOrDefault()
    {
        await using var ctx = new IncludeDbContext(_fixture.ConnectionString);

        var parent = await ctx.Children
            .Where(c => c.Id == 11L)
            .Join(ctx.Parents, c => c.ParentId, p => p.Id, (c, p) => p)
            .Include(p => p.Children)
            .ThenInclude(c => c.GrandChildren)
            .SingleOrDefaultAsync();

        AssertParent1(parent);
    }

    // SelectMany makes the children the second table of a join (parents INNER JOIN children).
    // Take moves that join into a subquery that projects `id` and `name` of the children.
    [Fact]
    public async Task Include_on_entity_from_joined_table_with_Take()
    {
        await using var ctx = new IncludeDbContext(_fixture.ConnectionString);

        var query = ctx.Parents
            .Where(p => p.Id == 1L)
            .SelectMany(p => p.Children)
            .Include(c => c.GrandChildren)
            .OrderBy(c => c.Id)
            .Take(1);

        var children = await query.AsNoTracking().ToListAsync();

        var child = Assert.Single(children);
        Assert.Equal(11L, child.Id);
        Assert.Equal("C11", child.Name);
        Assert.Equal([111L, 112L, 113L], child.GrandChildren.Select(g => g.Id).Order());

        // The subquery writes the alias of each column, also when it is equal to the column name.
        Assert.Matches(@"SELECT `\w+`\.`id` AS `id`, `\w+`\.`name` AS `name`", query.ToQueryString());
    }

    [Fact]
    public void Top_level_projection_does_not_alias_columns()
    {
        using var ctx = new IncludeDbContext(_fixture.ConnectionString);

        var sql = ctx.Parents
            .Include(p => p.Children)
            .ToQueryString();

        // The top-level SELECT reads `id` from two tables. It is read by position, and
        // ClickHouse refuses two expressions with the same alias in one SELECT.
        var topLevel = sql.Split('\n')[0];
        Assert.DoesNotContain(" AS ", topLevel);
    }

    private static void AssertParent1(IncludeParent? parent)
    {
        Assert.NotNull(parent);
        Assert.Equal(1L, parent.Id);
        Assert.Equal("P1", parent.Name);
        Assert.Equal([11L, 12L], parent.Children.Select(c => c.Id).Order());
        Assert.Equal([111L, 112L, 113L], parent.Children.Single(c => c.Id == 11).GrandChildren.Select(g => g.Id).Order());
        Assert.Empty(parent.Children.Single(c => c.Id == 12).GrandChildren);
    }
}
