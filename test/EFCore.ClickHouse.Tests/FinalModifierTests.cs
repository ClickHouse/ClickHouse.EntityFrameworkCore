using System.Text.RegularExpressions;
using ClickHouse.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Xunit;

namespace EFCore.ClickHouse.Tests;

#region Entities

public class FinalEvent
{
    public long Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public ulong Version { get; set; }
    public List<FinalLabel> Labels { get; set; } = [];
}

public class FinalLabel
{
    public long Id { get; set; }
    public long EventId { get; set; }
    public string Text { get; set; } = string.Empty;
    public ulong Version { get; set; }
}

public class FinalOwner
{
    public long Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public ulong Version { get; set; }
    public FinalOwnerAddress Address { get; set; } = new();
    public List<FinalOwnerPart> Parts { get; set; } = [];
}

public class FinalOwnerAddress
{
    public string City { get; set; } = string.Empty;
}

public class FinalOwnerPart
{
    public int Number { get; set; }
    public string Text { get; set; } = string.Empty;
}

public class FinalPlainRow
{
    public long Id { get; set; }
}

public abstract class FinalTptVehicle
{
    public long Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public ulong Version { get; set; }
}

public class FinalTptCar : FinalTptVehicle
{
    public int Doors { get; set; }
}

public abstract class FinalTpcShape
{
    public long Id { get; set; }
    public ulong Version { get; set; }
}

public class FinalTpcCircle : FinalTpcShape
{
    public double Radius { get; set; }
}

public class FinalTpcSquare : FinalTpcShape
{
    public double Side { get; set; }
}

public class FinalSqlQueryRow
{
    public long Id { get; set; }
}

public class FinalFunctionRow
{
    public long Id { get; set; }
}

public class FinalDbContext : DbContext
{
    public DbSet<FinalEvent> Events => Set<FinalEvent>();
    public DbSet<FinalLabel> Labels => Set<FinalLabel>();
    public DbSet<FinalOwner> Owners => Set<FinalOwner>();
    public DbSet<FinalPlainRow> PlainRows => Set<FinalPlainRow>();
    public DbSet<FinalTptVehicle> Vehicles => Set<FinalTptVehicle>();
    public DbSet<FinalTpcShape> Shapes => Set<FinalTpcShape>();
    public DbSet<FinalSqlQueryRow> SqlQueryRows => Set<FinalSqlQueryRow>();
    public DbSet<FinalFunctionRow> FunctionRows => Set<FinalFunctionRow>();

    private readonly string _connectionString;
    private readonly Action<string>? _logCommand;

    public FinalDbContext(string connectionString, Action<string>? logCommand = null)
    {
        _connectionString = connectionString;
        _logCommand = logCommand;
    }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseClickHouse(_connectionString);

        if (_logCommand is not null)
        {
            optionsBuilder.LogTo(_logCommand, [RelationalEventId.CommandExecuted]);
        }
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<FinalEvent>(entity =>
        {
            entity.ToTable("final_events");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.Name).HasColumnName("name");
            entity.Property(e => e.Version).HasColumnName("version");
            entity.HasMany(e => e.Labels).WithOne().HasForeignKey(l => l.EventId);
        });

        modelBuilder.Entity<FinalLabel>(entity =>
        {
            entity.ToTable("final_labels");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.EventId).HasColumnName("event_id");
            entity.Property(e => e.Text).HasColumnName("text");
            entity.Property(e => e.Version).HasColumnName("version");
        });

        modelBuilder.Entity<FinalOwner>(entity =>
        {
            entity.ToTable("final_owners");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.Name).HasColumnName("name");
            entity.Property(e => e.Version).HasColumnName("version");
            entity.OwnsOne(e => e.Address, address =>
            {
                address.ToTable("final_owner_addresses");
                address.WithOwner().HasForeignKey("OwnerId");
                address.Property<long>("OwnerId").HasColumnName("owner_id");
                address.Property(a => a.City).HasColumnName("city");
            });
            entity.OwnsMany(e => e.Parts, part =>
            {
                part.ToTable("final_owner_parts");
                part.WithOwner().HasForeignKey("OwnerId");
                part.Property<long>("OwnerId").HasColumnName("owner_id");
                part.Property(p => p.Number).HasColumnName("number");
                part.Property(p => p.Text).HasColumnName("text");
                part.HasKey("OwnerId", nameof(FinalOwnerPart.Number));
            });
        });

        modelBuilder.Entity<FinalPlainRow>(entity =>
        {
            entity.ToTable("final_plain_rows");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
        });

        modelBuilder.Entity<FinalTptVehicle>(entity =>
        {
            entity.UseTptMappingStrategy();
            entity.ToTable("final_tpt_vehicles");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.Name).HasColumnName("name");
            entity.Property(e => e.Version).HasColumnName("version");
        });

        modelBuilder.Entity<FinalTptCar>(entity =>
        {
            entity.ToTable("final_tpt_cars");
            entity.Property(e => e.Doors).HasColumnName("doors");
        });

        modelBuilder.Entity<FinalTpcShape>(entity =>
        {
            entity.UseTpcMappingStrategy();
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.Version).HasColumnName("version");
        });

        modelBuilder.Entity<FinalTpcCircle>(entity =>
        {
            entity.ToTable("final_tpc_circles");
            entity.Property(e => e.Radius).HasColumnName("radius");
        });

        modelBuilder.Entity<FinalTpcSquare>(entity =>
        {
            entity.ToTable("final_tpc_squares");
            entity.Property(e => e.Side).HasColumnName("side");
        });

        modelBuilder.Entity<FinalSqlQueryRow>(entity =>
        {
            entity.HasNoKey();
            entity.ToSqlQuery("SELECT 1 AS Id");
        });

        modelBuilder.Entity<FinalFunctionRow>(entity =>
        {
            entity.HasNoKey();
            entity.ToFunction("final_rows_function");
        });
    }
}

public class FinalFilteredDbContext : FinalDbContext
{
    public FinalFilteredDbContext(string connectionString)
        : base(connectionString)
    {
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);
        modelBuilder.Entity<FinalEvent>().HasQueryFilter(e => e.Id != 2);
    }
}

public class FinalLabelFilterDbContext : FinalDbContext
{
    public FinalLabelFilterDbContext(string connectionString)
        : base(connectionString)
    {
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        // Only an old version of event 1 has the name 'a'.
        modelBuilder.Entity<FinalLabel>()
            .HasQueryFilter(l => !Events.Final().Any(e => e.Id == l.EventId && e.Name == "a"));
    }
}

#endregion

public class FinalFixture : IAsyncLifetime
{
    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        ConnectionString = await SharedContainer.GetConnectionStringAsync();

        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(ConnectionString);
        await connection.OpenAsync();

        async Task Execute(string sql)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync();
        }

        await Execute("CREATE TABLE final_events (id Int64, name String, version UInt64) ENGINE = ReplacingMergeTree(version) ORDER BY id");
        await Execute("CREATE TABLE final_labels (id Int64, event_id Int64, text String, version UInt64) ENGINE = ReplacingMergeTree(version) ORDER BY event_id");
        await Execute("CREATE TABLE final_owners (id Int64, name String, version UInt64) ENGINE = ReplacingMergeTree(version) ORDER BY id");
        await Execute("CREATE TABLE final_owner_addresses (owner_id Int64, city String) ENGINE = MergeTree ORDER BY owner_id");
        await Execute("CREATE TABLE final_owner_parts (owner_id Int64, number Int32, text String) ENGINE = MergeTree ORDER BY (owner_id, number)");
        await Execute("CREATE TABLE final_plain_rows (id Int64) ENGINE = MergeTree ORDER BY id");
        await Execute("CREATE TABLE final_tpt_vehicles (id Int64, name String, version UInt64) ENGINE = ReplacingMergeTree(version) ORDER BY id");
        await Execute("CREATE TABLE final_tpt_cars (id Int64, doors Int32, version UInt64) ENGINE = ReplacingMergeTree(version) ORDER BY id");
        await Execute("CREATE TABLE final_tpc_circles (id Int64, version UInt64, radius Float64) ENGINE = ReplacingMergeTree(version) ORDER BY id");
        await Execute("CREATE TABLE final_tpc_squares (id Int64, version UInt64, side Float64) ENGINE = ReplacingMergeTree(version) ORDER BY id");

        // Stop background merges so that the rows stay unmerged and only FINAL merges them.
        foreach (var table in new[]
                 {
                     "final_events", "final_labels", "final_owners", "final_tpt_vehicles", "final_tpt_cars",
                     "final_tpc_circles", "final_tpc_squares"
                 })
        {
            await Execute($"SYSTEM STOP MERGES {table}");
        }

        // One INSERT for each row puts each row in a separate part.
        // Merged rows: 1 -> 'b', 2 -> 'c', 3 -> 'e'.
        await Execute("INSERT INTO final_events VALUES (1, 'a', 1)");
        await Execute("INSERT INTO final_events VALUES (1, 'b', 2)");
        await Execute("INSERT INTO final_events VALUES (2, 'c', 1)");
        await Execute("INSERT INTO final_events VALUES (3, 'd', 1)");
        await Execute("INSERT INTO final_events VALUES (3, 'e', 5)");

        // The sorting key is event_id, so FINAL keeps one label for each event.
        // Merged rows: event 1 -> (12, 'x2'), event 2 -> (11, 'y').
        await Execute("INSERT INTO final_labels VALUES (10, 1, 'x1', 1)");
        await Execute("INSERT INTO final_labels VALUES (12, 1, 'x2', 2)");
        await Execute("INSERT INTO final_labels VALUES (11, 2, 'y', 1)");

        // Merged rows: 1 -> 'new'. The owned types use MergeTree, which rejects FINAL.
        await Execute("INSERT INTO final_owners VALUES (1, 'old', 1)");
        await Execute("INSERT INTO final_owners VALUES (1, 'new', 2)");
        await Execute("INSERT INTO final_owner_addresses VALUES (1, 'Oslo')");
        await Execute("INSERT INTO final_owner_parts VALUES (1, 1, 'p1'), (1, 2, 'p2')");

        await Execute("INSERT INTO final_plain_rows VALUES (1)");

        // Merged rows: 1 -> ('car-new', 4 doors).
        await Execute("INSERT INTO final_tpt_vehicles VALUES (1, 'car-old', 1)");
        await Execute("INSERT INTO final_tpt_vehicles VALUES (1, 'car-new', 2)");
        await Execute("INSERT INTO final_tpt_cars VALUES (1, 2, 1)");
        await Execute("INSERT INTO final_tpt_cars VALUES (1, 4, 2)");

        // Merged rows: circle 1 -> 2.0, square 2 -> 3.0.
        await Execute("INSERT INTO final_tpc_circles VALUES (1, 1, 1.0)");
        await Execute("INSERT INTO final_tpc_circles VALUES (1, 2, 2.0)");
        await Execute("INSERT INTO final_tpc_squares VALUES (2, 1, 1.5)");
        await Execute("INSERT INTO final_tpc_squares VALUES (2, 2, 3.0)");
    }

    public Task DisposeAsync() => Task.CompletedTask;
}

public class FinalModifierTests : IClassFixture<FinalFixture>
{
    private readonly FinalFixture _fixture;

    public FinalModifierTests(FinalFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task WithoutFinal_ReadsUnmergedRows()
    {
        await using var ctx = new FinalDbContext(_fixture.ConnectionString);

        Assert.Equal(5, await ctx.Events.CountAsync());
    }

    [Fact]
    public async Task Final_ReadsMergedRows()
    {
        await using var ctx = new FinalDbContext(_fixture.ConnectionString);

        var query = ctx.Events.Final().OrderBy(e => e.Id);

        Assert.Contains("FROM `final_events` AS `f` FINAL", query.ToQueryString());

        var events = await query.ToListAsync();

        Assert.Equal(new[] { (1L, "b"), (2L, "c"), (3L, "e") }, events.Select(e => (e.Id, e.Name)));
    }

    [Fact]
    public async Task Final_ComposesWithOtherOperators()
    {
        await using var ctx = new FinalDbContext(_fixture.ConnectionString);

        // Without FINAL: a, b, d, e.
        var names = await ctx.Events.Final()
            .Where(e => e.Name != "c")
            .OrderBy(e => e.Name)
            .Select(e => e.Name)
            .ToListAsync();

        Assert.Equal(["b", "e"], names);
        Assert.Equal(6L, await ctx.Events.Final().SumAsync(e => e.Id));
    }

    [Fact]
    public async Task Final_AndPlainQueries_UseSeparateCompiledQueries()
    {
        await using var ctx = new FinalDbContext(_fixture.ConnectionString);

        Assert.Equal(5, await ctx.Events.CountAsync());
        Assert.Equal(3, await ctx.Events.Final().CountAsync());
        Assert.Equal(5, await ctx.Events.CountAsync());
    }

    [Fact]
    public async Task Final_InCompiledQuery()
    {
        var query = EF.CompileAsyncQuery((FinalDbContext ctx) => ctx.Events.Final().Count());

        await using var ctx = new FinalDbContext(_fixture.ConnectionString);

        Assert.Equal(3, await query(ctx));
    }

    [Fact]
    public async Task Final_InJoin_AppliesOnlyToMarkedSource()
    {
        await using var ctx = new FinalDbContext(_fixture.ConnectionString);

        var finalEventsOnly =
            from e in ctx.Events.Final()
            join l in ctx.Labels on e.Id equals l.EventId
            select new { e.Name, l.Text };

        var sql = finalEventsOnly.ToQueryString();
        Assert.Contains("FROM `final_events` AS `f` FINAL", sql);
        Assert.Contains("INNER JOIN `final_labels` AS `f0` ON", sql);

        // 3 merged events joined with 3 unmerged labels.
        Assert.Equal(3, (await finalEventsOnly.ToListAsync()).Count);

        var bothFinal =
            from e in ctx.Events.Final()
            join l in ctx.Labels.Final() on e.Id equals l.EventId
            orderby e.Id
            select new { e.Name, l.Text };

        Assert.Contains("INNER JOIN `final_labels` AS `f0` FINAL ON", bothFinal.ToQueryString());
        Assert.Equal(new[] { ("b", "x2"), ("c", "y") }, (await bothFinal.ToListAsync()).Select(x => (x.Name, x.Text)));
    }

    [Fact]
    public async Task Final_InSubquery()
    {
        await using var ctx = new FinalDbContext(_fixture.ConnectionString);

        // Without FINAL, event 1 still has the old name 'a'.
        var query = ctx.Labels.Final()
            .Where(l => ctx.Events.Final().Any(e => e.Id == l.EventId && e.Name == "a"));

        Assert.Contains("FROM `final_events` AS `f0` FINAL", query.ToQueryString());
        Assert.Empty(await query.ToListAsync());

        var plainSubquery = ctx.Labels.Final()
            .Where(l => ctx.Events.Any(e => e.Id == l.EventId && e.Name == "a"));

        Assert.Equal([12L], await plainSubquery.Select(l => l.Id).ToListAsync());
    }

    [Fact]
    public async Task Final_InSetOperation()
    {
        await using var ctx = new FinalDbContext(_fixture.ConnectionString);

        var ids = await ctx.Events.Final().Select(e => e.Id)
            .Concat(ctx.Events.Select(e => e.Id + 100))
            .OrderBy(id => id)
            .ToListAsync();

        Assert.Equal([1L, 2L, 3L, 101L, 101L, 102L, 103L, 103L], ids);
    }

    [Fact]
    public async Task Final_DoesNotApplyToIncludedNavigation()
    {
        await using var ctx = new FinalDbContext(_fixture.ConnectionString);

        var query = ctx.Events.Final().Include(e => e.Labels).Where(e => e.Id == 1);

        var sql = query.ToQueryString();
        Assert.Contains("FROM `final_events` AS `f` FINAL", sql);
        Assert.Contains("LEFT JOIN `final_labels` AS `f0` ON", sql);

        var single = Assert.Single(await query.AsNoTracking().ToListAsync());
        Assert.Equal(2, single.Labels.Count);
    }

    [Fact]
    public async Task Final_InSplitQuery_AppliesToParentTableInEveryCommand()
    {
        var commands = new List<string>();
        await using var ctx = new FinalDbContext(_fixture.ConnectionString, commands.Add);

        var events = await ctx.Events.Final()
            .Include(e => e.Labels)
            .AsSplitQuery()
            .AsNoTracking()
            .OrderBy(e => e.Id)
            .ToListAsync();

        Assert.Equal(new[] { 1L, 2L, 3L }, events.Select(e => e.Id));
        Assert.Equal(2, commands.Count);
        Assert.All(commands, c => Assert.Contains("FROM `final_events` AS `f` FINAL", c));
        Assert.Contains("`final_labels` AS `f0` ON", commands[1]);
    }

    [Fact]
    public async Task Final_DoesNotApplyToOwnedTypesInOtherTables()
    {
        await using var ctx = new FinalDbContext(_fixture.ConnectionString);

        var query = ctx.Owners.Final().Include(o => o.Address).Include(o => o.Parts);

        var sql = query.ToQueryString();
        Assert.Contains("FROM `final_owners` AS `f` FINAL", sql);
        Assert.Contains("`final_owner_addresses`", sql);
        Assert.Contains("`final_owner_parts`", sql);
        Assert.Single(Regex.Matches(sql, "FINAL"));

        var owner = Assert.Single(await query.AsNoTracking().ToListAsync());
        Assert.Equal("new", owner.Name);
        Assert.Equal("Oslo", owner.Address.City);
        Assert.Equal(new[] { "p1", "p2" }, owner.Parts.OrderBy(p => p.Number).Select(p => p.Text));
    }

    [Fact]
    public async Task Final_InQueryFilter()
    {
        await using var ctx = new FinalLabelFilterDbContext(_fixture.ConnectionString);

        Assert.Matches("`final_events` AS `\\w+` FINAL", ctx.Labels.ToQueryString());

        // Without FINAL, the filter removes the two labels of event 1.
        Assert.Equal(3, await ctx.Labels.CountAsync());
    }

    [Fact]
    public async Task Final_WithQueryFilter_KeepsFinal()
    {
        await using var ctx = new FinalFilteredDbContext(_fixture.ConnectionString);

        var ids = await ctx.Events.Final().OrderBy(e => e.Id).Select(e => e.Id).ToListAsync();

        Assert.Equal([1L, 3L], ids);
    }

    [Fact]
    public async Task Final_TptHierarchy_AppliesToAllTables()
    {
        await using var ctx = new FinalDbContext(_fixture.ConnectionString);

        var query = ctx.Vehicles.Final();

        var sql = query.ToQueryString();
        Assert.Contains("FROM `final_tpt_vehicles` AS `f` FINAL", sql);
        Assert.Contains("`final_tpt_cars` AS `f0` FINAL", sql);

        var car = Assert.IsType<FinalTptCar>(Assert.Single(await query.ToListAsync()));
        Assert.Equal("car-new", car.Name);
        Assert.Equal(4, car.Doors);
    }

    [Fact]
    public async Task Final_TpcHierarchy_AppliesToAllTables()
    {
        await using var ctx = new FinalDbContext(_fixture.ConnectionString);

        var query = ctx.Shapes.Final().OrderBy(s => s.Id);

        var sql = query.ToQueryString();
        Assert.Matches("FROM `final_tpc_circles` AS `\\w+` FINAL", sql);
        Assert.Matches("FROM `final_tpc_squares` AS `\\w+` FINAL", sql);

        var shapes = await query.ToListAsync();
        Assert.Equal(2, shapes.Count);
        Assert.Equal(2.0, Assert.IsType<FinalTpcCircle>(shapes[0]).Radius);
        Assert.Equal(3.0, Assert.IsType<FinalTpcSquare>(shapes[1]).Side);
    }

    [Fact]
    public async Task FinalSetting_AppliesFinalToAllTablesThatSupportIt()
    {
        await using var ctx = new FinalDbContext(_fixture.ConnectionString + ";set_final=1");

        Assert.Equal(3, await ctx.Events.CountAsync());
        Assert.Equal(1, await ctx.PlainRows.CountAsync());
    }

    [Fact]
    public async Task Final_OnEngineWithoutFinalSupport_ServerRejectsQuery()
    {
        await using var ctx = new FinalDbContext(_fixture.ConnectionString);

        var ex = await Assert.ThrowsAnyAsync<Exception>(() => ctx.PlainRows.Final().ToListAsync());

        Assert.Contains("ILLEGAL_FINAL", ex.ToString());
    }

    [Fact]
    public void Final_OnEntityWithoutTable_Throws()
    {
        using var ctx = new FinalDbContext(_fixture.ConnectionString);

        var sqlQueryException = Assert.Throws<InvalidOperationException>(() => ctx.SqlQueryRows.Final().ToQueryString());
        Assert.Contains("Final() requires the entity type 'FinalSqlQueryRow'", sqlQueryException.Message);

        var functionException = Assert.Throws<InvalidOperationException>(() => ctx.FunctionRows.Final().ToQueryString());
        Assert.Contains("Final() requires the entity type 'FinalFunctionRow'", functionException.Message);
    }
}

public class ClickHouseFinalQueryRootExpressionTests
{
    [Fact]
    public void FinalRoot_IsNotEqualToPlainRoot()
    {
        using var ctx = new FinalDbContext("Host=localhost");
        var entityType = ctx.Model.FindEntityType(typeof(FinalEvent))!;

        var plain = new EntityQueryRootExpression(entityType);
        var final = new ClickHouseFinalQueryRootExpression(entityType);
        var otherFinal = new ClickHouseFinalQueryRootExpression(entityType);

        Assert.False(final.Equals(plain));
        Assert.NotEqual(plain.GetHashCode(), final.GetHashCode());
        Assert.True(final.Equals(otherFinal));
        Assert.Equal(final.GetHashCode(), otherFinal.GetHashCode());
    }

    [Fact]
    public void FinalRoot_KeepsFinalWhenRebuilt()
    {
        using var ctx = new FinalDbContext("Host=localhost");
        var entityType = ctx.Model.FindEntityType(typeof(FinalEvent))!;
        var otherEntityType = ctx.Model.FindEntityType(typeof(FinalLabel))!;

        var final = (ClickHouseFinalQueryRootExpression)ctx.Events.Final().Expression;

        Assert.Equal("DbSet<FinalEvent>().Final()", ExpressionPrinter.Print(final));
        Assert.IsType<ClickHouseFinalQueryRootExpression>(final.DetachQueryProvider());
        Assert.IsType<ClickHouseFinalQueryRootExpression>(final.UpdateEntityType(entityType));
        Assert.Throws<InvalidOperationException>(() => final.UpdateEntityType(otherEntityType));
    }
}
