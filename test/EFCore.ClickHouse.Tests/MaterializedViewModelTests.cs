using ClickHouse.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Xunit;

namespace EFCore.ClickHouse.Tests;

public class MaterializedViewModelTests
{
    [Fact]
    public void FromRaw_stores_select_sql_verbatim()
    {
        using var ctx = new TestContext(b =>
        {
            b.Entity<TargetEntity>(e =>
            {
                e.HasKey(x => x.Hour);
                e.ToTable("target");
            });

            b.HasMaterializedView<TargetEntity>("mv_raw")
                .FromRaw("SELECT toStartOfHour(ts) AS Hour, count() AS Hits FROM raw GROUP BY Hour");
        });

        var views = ctx.GetService<IDesignTimeModel>().Model.GetMaterializedViews();
        var view = Assert.Single(views);
        Assert.Equal("mv_raw", view.Name);
        Assert.Equal("SELECT toStartOfHour(ts) AS Hour, count() AS Hits FROM raw GROUP BY Hour", view.SelectSql);
        Assert.False(view.Populate);
        Assert.Null(view.Cluster);
    }

    [Fact]
    public void Linq_select_captures_view_without_eagerly_translating()
    {
        // LINQ-defined views are translated lazily at differ time.
        // At model-build time the view is registered (target type + metadata) but SelectSql is null.
        using var ctx = new TestContext(b =>
        {
            b.Entity<SourceOrder>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("source_orders");
            });
            b.Entity<TargetEntity>(e =>
            {
                e.HasKey(x => x.Hour);
                e.ToTable("target");
            });

            b.HasMaterializedView<TargetEntity>("mv_linq")
                .From<SourceOrder>()
                .Select(orders => orders
                    .GroupBy(o => o.CreatedAt)
                    .Select(g => new TargetEntity
                    {
                        Hour = g.Key,
                        Hits = g.Count(),
                    }));
        });

        var view = Assert.Single(ctx.GetService<IDesignTimeModel>().Model.GetMaterializedViews());
        Assert.Equal("mv_linq", view.Name);
        Assert.Contains(typeof(TargetEntity).FullName!, view.TargetTypeName);
    }

    [Fact]
    public void OnCluster_and_InDatabase_are_recorded()
    {
        using var ctx = new TestContext(b =>
        {
            b.Entity<TargetEntity>(e =>
            {
                e.HasKey(x => x.Hour);
                e.ToTable("target");
            });

            b.HasMaterializedView<TargetEntity>("mv_full")
                .FromRaw("SELECT * FROM source")
                .OnCluster("prod")
                .InDatabase("analytics");
        });

        var view = Assert.Single(ctx.GetService<IDesignTimeModel>().Model.GetMaterializedViews());
        Assert.Equal("prod", view.Cluster);
        Assert.Equal("analytics", view.Database);
    }

    [Fact]
    public void Validator_rejects_view_targeting_unregistered_entity()
    {
        // TargetEntity is NOT added to the model — should fail validation.
        var ex = Assert.Throws<InvalidOperationException>(() =>
        {
            using var ctx = new TestContext(b =>
            {
                b.HasMaterializedView<TargetEntity>("mv_orphan").FromRaw("SELECT 1");
            });
            _ = ctx.GetService<IDesignTimeModel>().Model;
        });

        Assert.Contains("not a registered entity", ex.Message);
    }

    [Fact]
    public void Validator_rejects_view_without_select_body()
    {
        // Builder leaves the view in a partial state (target set, no Select/FromRaw).
        var ex = Assert.Throws<InvalidOperationException>(() =>
        {
            using var ctx = new TestContext(b =>
            {
                b.Entity<TargetEntity>(e => { e.HasKey(x => x.Hour); e.ToTable("target"); });
                _ = b.HasMaterializedView<TargetEntity>("mv_empty");  // no .Select or .FromRaw
            });
            _ = ctx.GetService<IDesignTimeModel>().Model;
        });

        Assert.Contains("no SELECT body", ex.Message);
    }

    public class TargetEntity
    {
        public DateTime Hour { get; set; }
        public int Hits { get; set; }
    }

    public class SourceOrder
    {
        public long Id { get; set; }
        public DateTime CreatedAt { get; set; }
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
