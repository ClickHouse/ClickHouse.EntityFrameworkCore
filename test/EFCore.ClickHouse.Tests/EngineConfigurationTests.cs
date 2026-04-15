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

    // --- Coverage gap tests: engine builders, fluent methods, entry points ---

    [Fact]
    public void HasAggregatingMergeTreeEngine_sets_engine()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test", t => t.HasAggregatingMergeTreeEngine()
                    .WithOrderBy("Id"));
            });
        });

        var entityType = model.FindEntityType(typeof(TestEntity))!;
        Assert.Equal(ClickHouseAnnotationNames.AggregatingMergeTree, entityType.GetEngine());
        Assert.Equal(["Id"], entityType.GetOrderBy());
    }

    [Fact]
    public void HasTinyLogEngine_sets_engine()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test", t => t.HasTinyLogEngine());
            });
        });

        var entityType = model.FindEntityType(typeof(TestEntity))!;
        Assert.Equal(ClickHouseAnnotationNames.TinyLog, entityType.GetEngine());
    }

    [Fact]
    public void HasLogEngine_sets_engine()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test", t => t.HasLogEngine());
            });
        });

        var entityType = model.FindEntityType(typeof(TestEntity))!;
        Assert.Equal(ClickHouseAnnotationNames.Log, entityType.GetEngine());
    }

    [Fact]
    public void WithPrimaryKey_stores_columns()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test", t => t.HasMergeTreeEngine()
                    .WithOrderBy("Id", "Name")
                    .WithPrimaryKey("Id"));
            });
        });

        var entityType = model.FindEntityType(typeof(TestEntity))!;
        Assert.Equal(["Id"], entityType.GetClickHousePrimaryKey());
    }

    [Fact]
    public void WithSampleBy_stores_columns()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test", t => t.HasMergeTreeEngine()
                    .WithOrderBy("Id")
                    .WithSampleBy("Id"));
            });
        });

        var entityType = model.FindEntityType(typeof(TestEntity))!;
        Assert.Equal(["Id"], entityType.GetSampleBy());
    }

    [Fact]
    public void MergeTreeEngine_full_fluent_chain()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test", t => t.HasMergeTreeEngine()
                    .WithOrderBy("Id")
                    .WithPartitionBy("toYYYYMM(Name)")
                    .WithPrimaryKey("Id")
                    .WithSampleBy("Id")
                    .WithTtl("Name + INTERVAL 1 DAY")
                    .WithSetting("index_granularity", "4096"));
            });
        });

        var entityType = model.FindEntityType(typeof(TestEntity))!;
        Assert.Equal(ClickHouseAnnotationNames.MergeTree, entityType.GetEngine());
        Assert.Equal(["Id"], entityType.GetOrderBy());
        Assert.Equal(["toYYYYMM(Name)"], entityType.GetPartitionBy());
        Assert.Equal(["Id"], entityType.GetClickHousePrimaryKey());
        Assert.Equal(["Id"], entityType.GetSampleBy());
        Assert.Equal("Name + INTERVAL 1 DAY", entityType.GetTtl());
        Assert.Equal("4096", entityType.GetSettings()["index_granularity"]);
    }

    [Fact]
    public void CollapsingMergeTree_full_fluent_chain()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test", t => t.HasCollapsingMergeTreeEngine("Sign")
                    .WithOrderBy("Id")
                    .WithPartitionBy("toYYYYMM(Name)")
                    .WithPrimaryKey("Id")
                    .WithSampleBy("Id")
                    .WithTtl("Name + INTERVAL 1 DAY")
                    .WithSetting("index_granularity", "8192"));
            });
        });

        var entityType = model.FindEntityType(typeof(TestEntity))!;
        Assert.Equal(ClickHouseAnnotationNames.CollapsingMergeTree, entityType.GetEngine());
        Assert.Equal("Sign", entityType.GetCollapsingMergeTreeSign());
        Assert.Equal(["Id"], entityType.GetOrderBy());
        Assert.Equal("8192", entityType.GetSettings()["index_granularity"]);
    }

    [Fact]
    public void ReplacingMergeTree_full_fluent_chain()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test", t => t.HasReplacingMergeTreeEngine("Ver", "IsDeleted")
                    .WithOrderBy("Id")
                    .WithPartitionBy("Name")
                    .WithPrimaryKey("Id")
                    .WithSampleBy("Id")
                    .WithTtl("Name + INTERVAL 1 DAY")
                    .WithSetting("index_granularity", "4096"));
            });
        });

        var et = model.FindEntityType(typeof(TestEntity))!;
        Assert.Equal(ClickHouseAnnotationNames.ReplacingMergeTree, et.GetEngine());
        Assert.Equal("Ver", et.GetReplacingMergeTreeVersion());
        Assert.Equal(["Id"], et.GetOrderBy());
        Assert.Equal(["Id"], et.GetClickHousePrimaryKey());
        Assert.Equal(["Id"], et.GetSampleBy());
    }

    [Fact]
    public void SummingMergeTree_full_fluent_chain()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test", t => t.HasSummingMergeTreeEngine("Name")
                    .WithOrderBy("Id")
                    .WithPartitionBy("Name")
                    .WithPrimaryKey("Id")
                    .WithSampleBy("Id")
                    .WithTtl("Name + INTERVAL 1 DAY")
                    .WithSetting("index_granularity", "4096"));
            });
        });

        var et = model.FindEntityType(typeof(TestEntity))!;
        Assert.Equal(ClickHouseAnnotationNames.SummingMergeTree, et.GetEngine());
        Assert.Equal(["Id"], et.GetOrderBy());
        Assert.Equal("4096", et.GetSettings()["index_granularity"]);
    }

    [Fact]
    public void VersionedCollapsingMergeTree_full_fluent_chain()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test", t => t.HasVersionedCollapsingMergeTreeEngine("Sign", "Ver")
                    .WithOrderBy("Id")
                    .WithPartitionBy("Name")
                    .WithPrimaryKey("Id")
                    .WithSampleBy("Id")
                    .WithTtl("Name + INTERVAL 1 DAY")
                    .WithSetting("index_granularity", "4096"));
            });
        });

        var et = model.FindEntityType(typeof(TestEntity))!;
        Assert.Equal(ClickHouseAnnotationNames.VersionedCollapsingMergeTree, et.GetEngine());
        Assert.Equal("Sign", et.GetVersionedCollapsingMergeTreeSign());
        Assert.Equal(["Id"], et.GetOrderBy());
    }

    [Fact]
    public void GraphiteMergeTree_full_fluent_chain()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test", t => t.HasGraphiteMergeTreeEngine("graphite_rollup")
                    .WithOrderBy("Id")
                    .WithPartitionBy("Name")
                    .WithPrimaryKey("Id")
                    .WithSampleBy("Id")
                    .WithTtl("Name + INTERVAL 1 DAY")
                    .WithSetting("index_granularity", "4096"));
            });
        });

        var et = model.FindEntityType(typeof(TestEntity))!;
        Assert.Equal(ClickHouseAnnotationNames.GraphiteMergeTree, et.GetEngine());
        Assert.Equal("graphite_rollup", et.GetGraphiteMergeTreeConfigSection());
        Assert.Equal(["Id"], et.GetOrderBy());
    }

    [Fact]
    public void AggregatingMergeTree_full_fluent_chain()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test", t => t.HasAggregatingMergeTreeEngine()
                    .WithOrderBy("Id")
                    .WithPartitionBy("Name")
                    .WithPrimaryKey("Id")
                    .WithSampleBy("Id")
                    .WithTtl("Name + INTERVAL 1 DAY")
                    .WithSetting("index_granularity", "4096"));
            });
        });

        var et = model.FindEntityType(typeof(TestEntity))!;
        Assert.Equal(ClickHouseAnnotationNames.AggregatingMergeTree, et.GetEngine());
        Assert.Equal(["Id"], et.GetOrderBy());
        Assert.Equal(["Name"], et.GetPartitionBy());
        Assert.Equal(["Id"], et.GetClickHousePrimaryKey());
        Assert.Equal(["Id"], et.GetSampleBy());
        Assert.Equal("Name + INTERVAL 1 DAY", et.GetTtl());
        Assert.Equal("4096", et.GetSettings()["index_granularity"]);
    }

    [Fact]
    public void Property_generic_HasCodec_works()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.Property<long>(x => x.Id).HasCodec("LZ4");
                e.ToTable("test");
            });
        });

        var property = model.FindEntityType(typeof(TestEntity))!.FindProperty("Id")!;
        Assert.Equal("LZ4", property.GetCodec());
    }

    [Fact]
    public void Property_generic_HasColumnTtl_works()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.Property<string>(x => x.Name).HasColumnTtl("ts + INTERVAL 1 DAY");
                e.ToTable("test");
            });
        });

        var property = model.FindEntityType(typeof(TestEntity))!.FindProperty("Name")!;
        Assert.Equal("ts + INTERVAL 1 DAY", property.GetColumnTtl());
    }

    [Fact]
    public void Property_generic_HasColumnComment_works()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.Property<string>(x => x.Name).HasColumnComment("test comment");
                e.ToTable("test");
            });
        });

        var property = model.FindEntityType(typeof(TestEntity))!.FindProperty("Name")!;
        Assert.Equal("test comment", property.GetColumnComment());
    }

    [Fact]
    public void Index_HasSkippingIndexParams_stores_annotation()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.HasIndex(x => x.Name)
                    .HasSkippingIndexType("set")
                    .HasGranularity(2)
                    .HasSkippingIndexParams("100");
                e.ToTable("test", t => t.HasMergeTreeEngine().WithOrderBy("Id"));
            });
        });

        var index = model.FindEntityType(typeof(TestEntity))!.GetIndexes().First();
        Assert.Equal("set", index.GetSkippingIndexType());
        Assert.Equal(2, index.GetGranularity());
        Assert.Equal("100", index.GetSkippingIndexParams());
    }

    [Fact]
    public void SimpleEngine_WithPartitionBy_throws()
    {
        Assert.Throws<InvalidOperationException>(() =>
        {
            BuildModel(b =>
            {
                b.Entity<TestEntity>(e =>
                {
                    e.HasKey(x => x.Id);
                    e.ToTable("test", t => t.HasTinyLogEngine().WithPartitionBy("Id"));
                });
            });
        });
    }

    [Fact]
    public void SimpleEngine_WithPrimaryKey_throws()
    {
        Assert.Throws<InvalidOperationException>(() =>
        {
            BuildModel(b =>
            {
                b.Entity<TestEntity>(e =>
                {
                    e.HasKey(x => x.Id);
                    e.ToTable("test", t => t.HasLogEngine().WithPrimaryKey("Id"));
                });
            });
        });
    }

    [Fact]
    public void SimpleEngine_WithSampleBy_throws()
    {
        Assert.Throws<InvalidOperationException>(() =>
        {
            BuildModel(b =>
            {
                b.Entity<TestEntity>(e =>
                {
                    e.HasKey(x => x.Id);
                    e.ToTable("test", t => t.HasStripeLogEngine().WithSampleBy("Id"));
                });
            });
        });
    }

    [Fact]
    public void SimpleEngine_WithTtl_throws()
    {
        Assert.Throws<InvalidOperationException>(() =>
        {
            BuildModel(b =>
            {
                b.Entity<TestEntity>(e =>
                {
                    e.HasKey(x => x.Id);
                    e.ToTable("test", t => t.HasMemoryEngine().WithTtl("Id + INTERVAL 1 DAY"));
                });
            });
        });
    }

    [Fact]
    public void SimpleEngine_WithSetting_throws()
    {
        Assert.Throws<InvalidOperationException>(() =>
        {
            BuildModel(b =>
            {
                b.Entity<TestEntity>(e =>
                {
                    e.HasKey(x => x.Id);
                    e.ToTable("test", t => t.HasMemoryEngine().WithSetting("k", "v"));
                });
            });
        });
    }

    // ── Lambda-based overloads ────────────────────────────────────────────

    [Fact]
    public void Lambda_ReplacingMergeTree_sets_version_and_isDeleted()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test", t => t.HasReplacingMergeTreeEngine<TestEntity>(
                    x => x.Version, x => x.IsDeleted));
            });
        });

        var et = model.FindEntityType(typeof(TestEntity))!;
        Assert.Equal(ClickHouseAnnotationNames.ReplacingMergeTree, et.GetEngine());
        Assert.Equal("Version", et.GetReplacingMergeTreeVersion());
        Assert.Equal("IsDeleted", et.GetReplacingMergeTreeIsDeleted());
    }

    [Fact]
    public void Lambda_ReplacingMergeTree_version_only()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test", t => t.HasReplacingMergeTreeEngine<TestEntity>(
                    version: x => x.Version));
            });
        });

        var et = model.FindEntityType(typeof(TestEntity))!;
        Assert.Equal("Version", et.GetReplacingMergeTreeVersion());
        Assert.Null(et.GetReplacingMergeTreeIsDeleted());
    }

    [Fact]
    public void Lambda_CollapsingMergeTree_sets_sign()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test", t => t.HasCollapsingMergeTreeEngine<TestEntity>(
                    x => x.Sign));
            });
        });

        var et = model.FindEntityType(typeof(TestEntity))!;
        Assert.Equal("Sign", et.GetCollapsingMergeTreeSign());
    }

    [Fact]
    public void Lambda_VersionedCollapsingMergeTree_sets_sign_and_version()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test", t => t.HasVersionedCollapsingMergeTreeEngine<TestEntity>(
                    x => x.Sign, x => x.Version));
            });
        });

        var et = model.FindEntityType(typeof(TestEntity))!;
        Assert.Equal("Sign", et.GetVersionedCollapsingMergeTreeSign());
        Assert.Equal("Version", et.GetVersionedCollapsingMergeTreeVersion());
    }

    [Fact]
    public void Lambda_SummingMergeTree_sets_columns()
    {
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test", t => t.HasSummingMergeTreeEngine<TestEntity>(
                    x => x.Amount, x => x.Count));
            });
        });

        var et = model.FindEntityType(typeof(TestEntity))!;
        Assert.Equal(["Amount", "Count"], et.GetSummingMergeTreeColumns());
    }

    // ── Validator: column-not-found for engine parameters ────────────────

    [Fact]
    public void VersionedCollapsingMergeTree_warns_on_missing_sign_column()
    {
        var ex = Assert.ThrowsAny<Exception>(() =>
        {
            BuildModel(b =>
            {
                b.Entity<TestEntity>(e =>
                {
                    e.HasKey(x => x.Id);
                    e.ToTable("test", t => t
                        .HasVersionedCollapsingMergeTreeEngine("NonExistentSign", "NonExistentVersion")
                        .WithOrderBy("Id"));
                });
            });
        });
        Assert.Contains("NonExistentSign", ex.Message);
    }

    [Fact]
    public void VersionedCollapsingMergeTree_warns_on_missing_version_column()
    {
        var ex = Assert.ThrowsAny<Exception>(() =>
        {
            BuildModel(b =>
            {
                b.Entity<VersionedEntity>(e =>
                {
                    e.HasKey(x => x.Id);
                    e.ToTable("test", t => t
                        .HasVersionedCollapsingMergeTreeEngine("Sign", "NonExistentVersion")
                        .WithOrderBy("Id"));
                });
            });
        });
        Assert.Contains("NonExistentVersion", ex.Message);
    }

    [Fact]
    public void SummingMergeTree_warns_on_missing_sum_columns()
    {
        var ex = Assert.ThrowsAny<Exception>(() =>
        {
            BuildModel(b =>
            {
                b.Entity<TestEntity>(e =>
                {
                    e.HasKey(x => x.Id);
                    e.ToTable("test", t => t
                        .HasSummingMergeTreeEngine("NonExistentColumn")
                        .WithOrderBy("Id"));
                });
            });
        });
        Assert.Contains("NonExistentColumn", ex.Message);
    }

    [Fact]
    public void CollapsingMergeTree_warns_on_missing_sign_column()
    {
        var ex = Assert.ThrowsAny<Exception>(() =>
        {
            BuildModel(b =>
            {
                b.Entity<TestEntity>(e =>
                {
                    e.HasKey(x => x.Id);
                    e.ToTable("test", t => t
                        .HasCollapsingMergeTreeEngine("NonExistentSign")
                        .WithOrderBy("Id"));
                });
            });
        });
        Assert.Contains("NonExistentSign", ex.Message);
    }

    [Fact]
    public void ReplacingMergeTree_warns_on_missing_version_column()
    {
        var ex = Assert.ThrowsAny<Exception>(() =>
        {
            BuildModel(b =>
            {
                b.Entity<TestEntity>(e =>
                {
                    e.HasKey(x => x.Id);
                    e.ToTable("test", t => t
                        .HasReplacingMergeTreeEngine("NonExistentVersion")
                        .WithOrderBy("Id"));
                });
            });
        });
        Assert.Contains("NonExistentVersion", ex.Message);
    }

    [Fact]
    public void ReplacingMergeTree_warns_on_missing_isDeleted_column()
    {
        var ex = Assert.ThrowsAny<Exception>(() =>
        {
            BuildModel(b =>
            {
                b.Entity<VersionedEntity>(e =>
                {
                    e.HasKey(x => x.Id);
                    e.ToTable("test", t => t
                        .HasReplacingMergeTreeEngine("Version", "NonExistentIsDeleted")
                        .WithOrderBy("Id"));
                });
            });
        });
        Assert.Contains("NonExistentIsDeleted", ex.Message);
    }

    // ── Validator: wrong CLR type for engine parameters ──────────────────

    [Fact]
    public void CollapsingMergeTree_rejects_non_Int8_sign_column()
    {
        // Sign is int → maps to Int32, but ClickHouse requires Int8
        var ex = Assert.ThrowsAny<Exception>(() =>
        {
            BuildModel(b =>
            {
                b.Entity<BadSignEntity>(e =>
                {
                    e.HasKey(x => x.Id);
                    e.ToTable("test", t => t
                        .HasCollapsingMergeTreeEngine("Sign")
                        .WithOrderBy("Id"));
                });
            });
        });
        Assert.Contains("Sign", ex.Message);
        Assert.Contains("Int8", ex.Message);
    }

    [Fact]
    public void VersionedCollapsingMergeTree_rejects_non_Int8_sign_column()
    {
        var ex = Assert.ThrowsAny<Exception>(() =>
        {
            BuildModel(b =>
            {
                b.Entity<BadSignEntity>(e =>
                {
                    e.HasKey(x => x.Id);
                    e.ToTable("test", t => t
                        .HasVersionedCollapsingMergeTreeEngine("Sign", "Version")
                        .WithOrderBy("Id"));
                });
            });
        });
        Assert.Contains("Sign", ex.Message);
        Assert.Contains("Int8", ex.Message);
    }

    [Fact]
    public void ReplacingMergeTree_rejects_non_UInt8_isDeleted_column()
    {
        // IsDeleted is int → maps to Int32, but ClickHouse requires UInt8
        var ex = Assert.ThrowsAny<Exception>(() =>
        {
            BuildModel(b =>
            {
                b.Entity<BadIsDeletedEntity>(e =>
                {
                    e.HasKey(x => x.Id);
                    e.ToTable("test", t => t
                        .HasReplacingMergeTreeEngine("Version", "IsDeleted")
                        .WithOrderBy("Id"));
                });
            });
        });
        Assert.Contains("IsDeleted", ex.Message);
        Assert.Contains("UInt8", ex.Message);
    }

    [Fact]
    public void CollapsingMergeTree_accepts_sbyte_sign_column()
    {
        // sbyte maps to Int8 — should pass
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test", t => t
                    .HasCollapsingMergeTreeEngine("Sign")
                    .WithOrderBy("Id"));
            });
        });
        Assert.Equal("Sign", model.FindEntityType(typeof(TestEntity))!.GetCollapsingMergeTreeSign());
    }

    [Fact]
    public void ReplacingMergeTree_accepts_byte_isDeleted_column()
    {
        // byte maps to UInt8 — should pass
        var model = BuildModel(b =>
        {
            b.Entity<TestEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("test", t => t
                    .HasReplacingMergeTreeEngine("Version", "IsDeleted")
                    .WithOrderBy("Id"));
            });
        });
        Assert.Equal("IsDeleted", model.FindEntityType(typeof(TestEntity))!.GetReplacingMergeTreeIsDeleted());
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
        public sbyte Sign { get; set; }
        public ulong Version { get; set; }
        public byte IsDeleted { get; set; }
        public ulong Ver { get; set; }
        public decimal Amount { get; set; }
        public long Count { get; set; }
    }

    private class VersionedEntity
    {
        public long Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public sbyte Sign { get; set; }
        public ulong Version { get; set; }
    }

    // Sign is int (should be sbyte/Int8)
    private class BadSignEntity
    {
        public long Id { get; set; }
        public int Sign { get; set; }
        public ulong Version { get; set; }
    }

    // IsDeleted is int (should be byte/UInt8)
    private class BadIsDeletedEntity
    {
        public long Id { get; set; }
        public ulong Version { get; set; }
        public int IsDeleted { get; set; }
    }
}
