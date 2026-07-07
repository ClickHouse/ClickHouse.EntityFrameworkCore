using ClickHouse.EntityFrameworkCore.Extensions;
using ClickHouse.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Xunit;

namespace EFCore.ClickHouse.Tests;

public class MaterializedViewDifferTests
{
    [Fact]
    public void Adding_FromRaw_view_emits_Create()
    {
        var ops = Diff(
            previous: b => DefineTargetAndSource(b),
            current: b =>
            {
                DefineTargetAndSource(b);
                b.HasMaterializedView<Target>("mv1")
                    .FromRaw("SELECT Id AS Hour, count() AS Hits FROM source GROUP BY Id");
            });

        var create = Assert.Single(ops.OfType<ClickHouseCreateMaterializedViewOperation>());
        Assert.Equal("mv1", create.ViewName);
        Assert.Equal("target", create.TargetTable);
        Assert.Contains("SELECT", create.SelectQuery);
        Assert.Empty(ops.OfType<ClickHouseDropMaterializedViewOperation>());
    }

    [Fact]
    public void Removing_view_emits_Drop()
    {
        var ops = Diff(
            previous: b =>
            {
                DefineTargetAndSource(b);
                b.HasMaterializedView<Target>("mv1").FromRaw("SELECT * FROM source");
            },
            current: b => DefineTargetAndSource(b));

        var drop = Assert.Single(ops.OfType<ClickHouseDropMaterializedViewOperation>());
        Assert.Equal("mv1", drop.ViewName);
        Assert.Empty(ops.OfType<ClickHouseCreateMaterializedViewOperation>());
    }

    [Fact]
    public void Changing_SelectSql_emits_Drop_then_Create()
    {
        var ops = Diff(
            previous: b =>
            {
                DefineTargetAndSource(b);
                b.HasMaterializedView<Target>("mv1").FromRaw("SELECT * FROM source");
            },
            current: b =>
            {
                DefineTargetAndSource(b);
                b.HasMaterializedView<Target>("mv1").FromRaw("SELECT Id AS Hour FROM source");
            });

        var drop = Assert.Single(ops.OfType<ClickHouseDropMaterializedViewOperation>());
        var create = Assert.Single(ops.OfType<ClickHouseCreateMaterializedViewOperation>());

        // Drop comes before Create
        var dropIdx = ops.IndexOf(drop);
        var createIdx = ops.IndexOf(create);
        Assert.True(dropIdx < createIdx);
    }

    [Fact]
    public void Changing_OnCluster_emits_Drop_then_Create()
    {
        var ops = Diff(
            previous: b =>
            {
                DefineTargetAndSource(b);
                b.HasMaterializedView<Target>("mv1").FromRaw("SELECT * FROM source");
            },
            current: b =>
            {
                DefineTargetAndSource(b);
                b.HasMaterializedView<Target>("mv1").FromRaw("SELECT * FROM source").OnCluster("prod");
            });

        Assert.Single(ops.OfType<ClickHouseDropMaterializedViewOperation>());
        var create = Assert.Single(ops.OfType<ClickHouseCreateMaterializedViewOperation>());
        Assert.Equal("prod", create.Cluster);
    }

    [Fact]
    public void Unchanged_view_produces_no_view_operations()
    {
        Action<ModelBuilder> setup = b =>
        {
            DefineTargetAndSource(b);
            b.HasMaterializedView<Target>("mv1").FromRaw("SELECT * FROM source").OnCluster("prod");
        };

        var ops = Diff(setup, setup);

        Assert.Empty(ops.OfType<ClickHouseCreateMaterializedViewOperation>());
        Assert.Empty(ops.OfType<ClickHouseDropMaterializedViewOperation>());
    }

    [Fact]
    public void Multiple_views_only_changed_one_is_diffed()
    {
        var ops = Diff(
            previous: b =>
            {
                DefineTargetAndSource(b);
                b.HasMaterializedView<Target>("mv1").FromRaw("SELECT * FROM source");
                b.HasMaterializedView<Target>("mv2").FromRaw("SELECT 1 FROM source");
            },
            current: b =>
            {
                DefineTargetAndSource(b);
                b.HasMaterializedView<Target>("mv1").FromRaw("SELECT * FROM source");          // unchanged
                b.HasMaterializedView<Target>("mv2").FromRaw("SELECT 2 FROM source");          // changed
            });

        var create = Assert.Single(ops.OfType<ClickHouseCreateMaterializedViewOperation>());
        var drop = Assert.Single(ops.OfType<ClickHouseDropMaterializedViewOperation>());
        Assert.Equal("mv2", create.ViewName);
        Assert.Equal("mv2", drop.ViewName);
    }

    [Fact]
    public void View_create_follows_target_table_create()
    {
        var ops = Diff(
            previous: _ => { },                                     // empty model
            current: b =>
            {
                DefineTargetAndSource(b);
                b.HasMaterializedView<Target>("mv1").FromRaw("SELECT * FROM source");
            });

        var createTable = ops.OfType<CreateTableOperation>().FirstOrDefault(o => o.Name == "target");
        var createView = ops.OfType<ClickHouseCreateMaterializedViewOperation>().Single();

        Assert.NotNull(createTable);
        Assert.True(ops.IndexOf(createTable!) < ops.IndexOf(createView));
    }

    [Fact]
    public void View_drop_precedes_target_table_drop()
    {
        var ops = Diff(
            previous: b =>
            {
                DefineTargetAndSource(b);
                b.HasMaterializedView<Target>("mv1").FromRaw("SELECT * FROM source");
            },
            current: _ => { });

        var dropView = ops.OfType<ClickHouseDropMaterializedViewOperation>().Single();
        var dropTable = ops.OfType<DropTableOperation>().FirstOrDefault(o => o.Name == "target");

        Assert.NotNull(dropTable);
        Assert.True(ops.IndexOf(dropView) < ops.IndexOf(dropTable!));
    }

    [Fact]
    public void Linq_view_translates_to_SQL_at_differ_time()
    {
        var ops = Diff(
            previous: _ => { },
            current: b =>
            {
                DefineTargetAndSource(b);
                b.HasMaterializedView<Target>("mv1")
                    .From<Source>()
                    .Select(q => q.GroupBy(s => s.Id).Select(g => new Target { Hour = g.Key, Hits = g.Count() }));
            });

        var create = Assert.Single(ops.OfType<ClickHouseCreateMaterializedViewOperation>());
        Assert.Contains("SELECT", create.SelectQuery, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("source", create.SelectQuery, StringComparison.OrdinalIgnoreCase);
    }

    private static void DefineTargetAndSource(ModelBuilder b)
    {
        b.Entity<Source>(e => { e.HasKey(x => x.Id); e.ToTable("source"); });
        b.Entity<Target>(e => { e.HasKey(x => x.Hour); e.ToTable("target"); });
    }

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

    public class Target
    {
        public long Hour { get; set; }
        public int Hits { get; set; }
    }

    public class Source
    {
        public long Id { get; set; }
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
