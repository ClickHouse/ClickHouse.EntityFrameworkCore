using ClickHouse.EntityFrameworkCore.Extensions;
using ClickHouse.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Xunit;

namespace EFCore.ClickHouse.Tests;

public class ProjectionDifferTests
{
    [Fact]
    public void Adding_FromRaw_projection_emits_Add()
    {
        var ops = Diff(
            previous: DefineEvents,
            current: b =>
            {
                DefineEvents(b);
                b.Entity<Event>().HasProjection("p_by_cat")
                    .FromRaw("SELECT Category, count() GROUP BY Category");
            });

        var add = Assert.Single(ops.OfType<ClickHouseAddProjectionOperation>());
        Assert.Equal("p_by_cat", add.ProjectionName);
        Assert.Equal("events", add.Table);
        Assert.Contains("SELECT", add.SelectQuery);
        Assert.True(add.Materialize);
        Assert.Empty(ops.OfType<ClickHouseDropProjectionOperation>());
    }

    [Fact]
    public void Removing_projection_emits_Drop()
    {
        var ops = Diff(
            previous: b =>
            {
                DefineEvents(b);
                b.Entity<Event>().HasProjection("p").FromRaw("SELECT Category GROUP BY Category");
            },
            current: DefineEvents);

        var drop = Assert.Single(ops.OfType<ClickHouseDropProjectionOperation>());
        Assert.Equal("p", drop.ProjectionName);
        Assert.Equal("events", drop.Table);
        Assert.Empty(ops.OfType<ClickHouseAddProjectionOperation>());
    }

    [Fact]
    public void Changing_SelectSql_emits_Drop_then_Add()
    {
        var ops = Diff(
            previous: b =>
            {
                DefineEvents(b);
                b.Entity<Event>().HasProjection("p").FromRaw("SELECT Category GROUP BY Category");
            },
            current: b =>
            {
                DefineEvents(b);
                b.Entity<Event>().HasProjection("p").FromRaw("SELECT Category, sum(Amount) GROUP BY Category");
            });

        var drop = Assert.Single(ops.OfType<ClickHouseDropProjectionOperation>());
        var add = Assert.Single(ops.OfType<ClickHouseAddProjectionOperation>());
        Assert.True(ops.IndexOf(drop) < ops.IndexOf(add));
    }

    [Fact]
    public void WithoutMaterialize_sets_Materialize_false_on_operation()
    {
        var ops = Diff(
            previous: DefineEvents,
            current: b =>
            {
                DefineEvents(b);
                b.Entity<Event>().HasProjection("p")
                    .FromRaw("SELECT Category GROUP BY Category")
                    .WithoutMaterialize();
            });

        var add = Assert.Single(ops.OfType<ClickHouseAddProjectionOperation>());
        Assert.False(add.Materialize);
    }

    [Fact]
    public void Changing_OnCluster_emits_Drop_then_Add()
    {
        var ops = Diff(
            previous: b =>
            {
                DefineEvents(b);
                b.Entity<Event>().HasProjection("p").FromRaw("SELECT Category GROUP BY Category");
            },
            current: b =>
            {
                DefineEvents(b);
                b.Entity<Event>().HasProjection("p").FromRaw("SELECT Category GROUP BY Category").OnCluster("prod");
            });

        Assert.Single(ops.OfType<ClickHouseDropProjectionOperation>());
        var add = Assert.Single(ops.OfType<ClickHouseAddProjectionOperation>());
        Assert.Equal("prod", add.Cluster);
    }

    [Fact]
    public void Unchanged_projection_produces_no_projection_operations()
    {
        Action<ModelBuilder> setup = b =>
        {
            DefineEvents(b);
            b.Entity<Event>().HasProjection("p").FromRaw("SELECT Category GROUP BY Category").OnCluster("prod");
        };

        var ops = Diff(setup, setup);

        Assert.Empty(ops.OfType<ClickHouseAddProjectionOperation>());
        Assert.Empty(ops.OfType<ClickHouseDropProjectionOperation>());
    }

    [Fact]
    public void Linq_projection_translates_and_strips_FROM_at_differ_time()
    {
        var ops = Diff(
            previous: DefineEvents,
            current: b =>
            {
                DefineEvents(b);
                b.Entity<Event>().HasProjection("p_agg")
                    .Select(q => q.GroupBy(e => e.Category)
                        .Select(g => new { Category = g.Key, Total = g.Sum(e => e.Amount) }));
            });

        var add = Assert.Single(ops.OfType<ClickHouseAddProjectionOperation>());
        Assert.Contains("SELECT", add.SelectQuery, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("GROUP BY", add.SelectQuery, StringComparison.OrdinalIgnoreCase);
        // A projection's SELECT has no FROM clause — the parent table is implicit.
        Assert.DoesNotContain("FROM", add.SelectQuery, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Projection_add_follows_table_create()
    {
        var ops = Diff(
            previous: _ => { },
            current: b =>
            {
                DefineEvents(b);
                b.Entity<Event>().HasProjection("p").FromRaw("SELECT Category GROUP BY Category");
            });

        var createTable = ops.OfType<CreateTableOperation>().FirstOrDefault(o => o.Name == "events");
        var addProjection = ops.OfType<ClickHouseAddProjectionOperation>().Single();

        Assert.NotNull(createTable);
        Assert.True(ops.IndexOf(createTable!) < ops.IndexOf(addProjection));
    }

    [Fact]
    public void Projection_drop_precedes_table_drop()
    {
        var ops = Diff(
            previous: b =>
            {
                DefineEvents(b);
                b.Entity<Event>().HasProjection("p").FromRaw("SELECT Category GROUP BY Category");
            },
            current: _ => { });

        var dropProjection = ops.OfType<ClickHouseDropProjectionOperation>().Single();
        var dropTable = ops.OfType<DropTableOperation>().FirstOrDefault(o => o.Name == "events");

        Assert.NotNull(dropTable);
        Assert.True(ops.IndexOf(dropProjection) < ops.IndexOf(dropTable!));
    }

    private static void DefineEvents(ModelBuilder b)
        => b.Entity<Event>(e => { e.HasKey(x => x.Id); e.ToTable("events"); });

    private static IList<MigrationOperation> Diff(
        Action<ModelBuilder> previous,
        Action<ModelBuilder> current)
    {
        using var prevCtx = new TestContext(previous);
        using var currCtx = new TestContext(current);
        var prevModel = prevCtx.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var currModel = currCtx.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var differ = currCtx.GetService<IMigrationsModelDiffer>();
        return differ.GetDifferences(prevModel, currModel).ToList();
    }

    public class Event
    {
        public long Id { get; set; }
        public string Category { get; set; } = "";
        public decimal Amount { get; set; }
    }

    private sealed class TestContext : DbContext
    {
        private readonly Action<ModelBuilder> _configure;
        public TestContext(Action<ModelBuilder> configure) => _configure = configure;
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.UseClickHouse("Host=localhost;Database=test").EnableServiceProviderCaching(false);
        protected override void OnModelCreating(ModelBuilder modelBuilder) => _configure(modelBuilder);
    }
}
