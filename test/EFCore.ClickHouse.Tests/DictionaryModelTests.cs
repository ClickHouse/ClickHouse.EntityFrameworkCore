using ClickHouse.EntityFrameworkCore.Extensions;
using ClickHouse.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Xunit;

namespace EFCore.ClickHouse.Tests;

/// <summary>
/// Model-level (no database) tests for dictionary configuration capture and differ validation.
/// </summary>
public class DictionaryModelTests
{
    [Fact]
    public void Composite_key_is_captured_from_anonymous_type()
    {
        using var ctx = new TestContext(b => b
            .HasDictionary<Composite>("d").FromTable<Composite>().HasKey(c => new { c.A, c.B }));

        var def = Assert.Single(ctx.GetService<IDesignTimeModel>().Model.GetDictionaries());
        Assert.Equal(["A", "B"], def.KeyColumns);
    }

    [Fact]
    public void Layout_lifetime_cluster_and_database_are_captured()
    {
        using var ctx = new TestContext(b => b
            .HasDictionary<Composite>("d").FromTable<Composite>().HasKey(c => c.A)
            .Layout(ClickHouseDictionaryLayout.ComplexKeyHashed)
            .Lifetime(60, 120)
            .OnCluster("prod")
            .InDatabase("dicts"));

        var def = Assert.Single(ctx.GetService<IDesignTimeModel>().Model.GetDictionaries());
        Assert.Equal("COMPLEX_KEY_HASHED", def.Layout);
        Assert.Equal(60, def.LifetimeMin);
        Assert.Equal(120, def.LifetimeMax);
        Assert.Equal("prod", def.Cluster);
        Assert.Equal("dicts", def.Database);
        Assert.Equal(["A", "B", "Val"], def.ColumnNames);
    }

    [Fact]
    public void Dictionary_without_source_throws_when_diffed()
    {
        using var empty = new TestContext(_ => { });
        using var ctx = new TestContext(b => b.HasDictionary<Composite>("d").HasKey(c => c.A)); // no FromTable

        var emptyModel = empty.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var targetModel = ctx.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var differ = ctx.GetService<IMigrationsModelDiffer>();

        var ex = Assert.Throws<InvalidOperationException>(() => differ.GetDifferences(emptyModel, targetModel));
        Assert.Contains("no source table", ex.Message);
    }

    private sealed class TestContext : DbContext
    {
        private readonly Action<ModelBuilder> _configure;
        public TestContext(Action<ModelBuilder> configure) => _configure = configure;

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.UseClickHouse("Host=localhost;Database=test").EnableServiceProviderCaching(false);

        protected override void OnModelCreating(ModelBuilder modelBuilder) => _configure(modelBuilder);
    }

    private class Composite
    {
        public long A { get; set; }
        public long B { get; set; }
        public long Val { get; set; }
    }
}
