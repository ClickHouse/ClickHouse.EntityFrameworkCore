using ClickHouse.EntityFrameworkCore.Extensions;
using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.ClickHouse.Tests;

public class EngineConfigurationTests
{
    [Fact]
    public void HasMergeTreeEngine_sets_engine_annotation()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test", t => t.HasMergeTreeEngine());
            });
        });

        var entityType = model.FindEntityType(typeof(TestEntity))!;
        Assert.Equal(ClickHouseAnnotationNames.MergeTree, entityType.GetEngine());
    }

    [Fact]
    public void HasReplacingMergeTreeEngine_sets_version_and_isDeleted()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test", t => t.HasReplacingMergeTreeEngine("Version", "IsDeleted"));
            });
        });

        var entityType = model.FindEntityType(typeof(TestEntity))!;
        Assert.Equal(ClickHouseAnnotationNames.ReplacingMergeTree, entityType.GetEngine());
        Assert.Equal("Version", entityType.GetReplacingMergeTreeVersion());
        Assert.Equal("IsDeleted", entityType.GetReplacingMergeTreeIsDeleted());
    }

    [Fact]
    public void HasCollapsingMergeTreeEngine_sets_sign()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test", t => t.HasCollapsingMergeTreeEngine("Sign"));
            });
        });

        var entityType = model.FindEntityType(typeof(TestEntity))!;
        Assert.Equal(ClickHouseAnnotationNames.CollapsingMergeTree, entityType.GetEngine());
        Assert.Equal("Sign", entityType.GetCollapsingMergeTreeSign());
    }

    [Fact]
    public void HasVersionedCollapsingMergeTreeEngine_sets_sign_and_version()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test", t => t.HasVersionedCollapsingMergeTreeEngine("Sign", "Ver"));
            });
        });

        var entityType = model.FindEntityType(typeof(TestEntity))!;
        Assert.Equal(ClickHouseAnnotationNames.VersionedCollapsingMergeTree, entityType.GetEngine());
        Assert.Equal("Sign", entityType.GetVersionedCollapsingMergeTreeSign());
        Assert.Equal("Ver", entityType.GetVersionedCollapsingMergeTreeVersion());
    }

    [Fact]
    public void HasSummingMergeTreeEngine_sets_columns()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test", t => t.HasSummingMergeTreeEngine("Amount", "Count"));
            });
        });

        var entityType = model.FindEntityType(typeof(TestEntity))!;
        Assert.Equal(ClickHouseAnnotationNames.SummingMergeTree, entityType.GetEngine());
        Assert.Equal(["Amount", "Count"], entityType.GetSummingMergeTreeColumns());
    }

    [Fact]
    public void HasGraphiteMergeTreeEngine_sets_config_section()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test", t => t.HasGraphiteMergeTreeEngine("graphite_rollup"));
            });
        });

        var entityType = model.FindEntityType(typeof(TestEntity))!;
        Assert.Equal(ClickHouseAnnotationNames.GraphiteMergeTree, entityType.GetEngine());
        Assert.Equal("graphite_rollup", entityType.GetGraphiteMergeTreeConfigSection());
    }

    [Fact]
    public void WithOrderBy_stores_column_array()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test", t => t.HasMergeTreeEngine().WithOrderBy("Id", "Name"));
            });
        });

        var entityType = model.FindEntityType(typeof(TestEntity))!;
        Assert.Equal(["Id", "Name"], entityType.GetOrderBy());
    }

    [Fact]
    public void WithPartitionBy_stores_expression()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test", t => t.HasMergeTreeEngine()
                    .WithOrderBy("Id")
                    .WithPartitionBy("toYYYYMM(CreatedAt)"));
            });
        });

        var entityType = model.FindEntityType(typeof(TestEntity))!;
        Assert.Equal(["toYYYYMM(CreatedAt)"], entityType.GetPartitionBy());
    }

    [Fact]
    public void WithSetting_stores_prefix_based_annotations()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test", t => t.HasMergeTreeEngine()
                    .WithOrderBy("Id")
                    .WithSetting("index_granularity", "4096")
                    .WithSetting("storage_policy", "'hot_cold'"));
            });
        });

        var entityType = model.FindEntityType(typeof(TestEntity))!;
        var settings = entityType.GetSettings();
        Assert.Equal(2, settings.Count);
        Assert.Equal("4096", settings["index_granularity"]);
        Assert.Equal("'hot_cold'", settings["storage_policy"]);
    }

    [Fact]
    public void WithTtl_stores_ttl_expression()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test", t => t.HasMergeTreeEngine()
                    .WithOrderBy("Id")
                    .WithTtl("CreatedAt + INTERVAL 30 DAY"));
            });
        });

        var entityType = model.FindEntityType(typeof(TestEntity))!;
        Assert.Equal("CreatedAt + INTERVAL 30 DAY", entityType.GetTtl());
    }

    [Fact]
    public void HasMemoryEngine_sets_engine()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test", t => t.HasMemoryEngine());
            });
        });

        var entityType = model.FindEntityType(typeof(TestEntity))!;
        Assert.Equal(ClickHouseAnnotationNames.Memory, entityType.GetEngine());
    }

    [Fact]
    public void SimpleEngine_WithOrderBy_throws()
    {
        Assert.Throws<InvalidOperationException>(() =>
        {
            BuildModel(b =>
            {
                b.Entity<TestEntity>(e =>
                {
                    e.HasKey(x => x.Id);
                    e.ToTable("test", t => t.HasMemoryEngine().WithOrderBy("Id"));
                });
            });
        });
    }

    [Fact]
    public void Index_HasSkippingIndexType_stores_annotation()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.HasIndex(x => x.Name)
                    .HasSkippingIndexType("minmax")
                    .HasGranularity(4);
                e.ToTable("test", t => t.HasMergeTreeEngine().WithOrderBy("Id"));
            });
        });

        var index = model.FindEntityType(typeof(TestEntity))!.GetIndexes().First();
        Assert.Equal("minmax", index.GetSkippingIndexType());
        Assert.Equal(4, index.GetGranularity());
    }

    [Fact]
    public void Property_HasCodec_stores_annotation()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.Property(x => x.Id).HasCodec("Delta, ZSTD");
                e.ToTable("test", t => t.HasMergeTreeEngine().WithOrderBy("Id"));
            });
        });

        var property = model.FindEntityType(typeof(TestEntity))!.FindProperty("Id")!;
        Assert.Equal("Delta, ZSTD", property.GetCodec());
    }

    [Fact]
    public void Property_HasColumnTtl_stores_annotation()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.Property(x => x.Name).HasColumnTtl("CreatedAt + INTERVAL 1 DAY");
                e.ToTable("test", t => t.HasMergeTreeEngine().WithOrderBy("Id"));
            });
        });

        var property = model.FindEntityType(typeof(TestEntity))!.FindProperty("Name")!;
        Assert.Equal("CreatedAt + INTERVAL 1 DAY", property.GetColumnTtl());
    }

    [Fact]
    public void Property_HasColumnComment_stores_annotation()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.Property(x => x.Name).HasColumnComment("User's display name");
                e.ToTable("test", t => t.HasMergeTreeEngine().WithOrderBy("Id"));
            });
        });

        var property = model.FindEntityType(typeof(TestEntity))!.FindProperty("Name")!;
        Assert.Equal("User's display name", property.GetColumnComment());
    }

    [Fact]
    public void Default_convention_sets_MergeTree_with_PK_as_OrderBy()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test");
            });
        });

        var entityType = model.FindEntityType(typeof(TestEntity))!;
        Assert.Equal(ClickHouseAnnotationNames.MergeTree, entityType.GetEngine());
        Assert.Equal(["Id"], entityType.GetOrderBy());
    }

    [Fact]
    public void Default_convention_sets_tuple_OrderBy_when_no_PK()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasNoKey();
                e.ToTable("test");
            });
        });

        var entityType = model.FindEntityType(typeof(TestEntity))!;
        Assert.Equal(ClickHouseAnnotationNames.MergeTree, entityType.GetEngine());
        Assert.Equal(["tuple()"], entityType.GetOrderBy());
    }

    [Fact]
    public void Default_convention_does_not_override_explicit_engine()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test", t => t.HasStripeLogEngine());
            });
        });

        var entityType = model.FindEntityType(typeof(TestEntity))!;
        Assert.Equal(ClickHouseAnnotationNames.StripeLog, entityType.GetEngine());
        Assert.Null(entityType.GetOrderBy());
    }

    private static Microsoft.EntityFrameworkCore.Metadata.IModel BuildModel(Action<ModelBuilder> configure)
    {
        var optionsBuilder = new DbContextOptionsBuilder()
            .UseClickHouse("Host=localhost;Database=test")
            .EnableServiceProviderCaching(false);
        using var context = new TestDbContext(optionsBuilder.Options, configure);
        return context.Model;
    }

    private class TestDbContext : DbContext
    {
        private readonly Action<ModelBuilder> _configure;

        public TestDbContext(DbContextOptions options, Action<ModelBuilder> configure)
            : base(options)
        {
            _configure = configure;
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder) => _configure(modelBuilder);
    }

    private class TestEntity
    {
        public long Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
