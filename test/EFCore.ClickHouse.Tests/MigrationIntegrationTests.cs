using ClickHouse.EntityFrameworkCore.Extensions;
using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using ClickHouse.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Xunit;

namespace EFCore.ClickHouse.Tests;

/// <summary>
/// Integration tests that verify migration SQL against a real ClickHouse instance.
/// Each test creates tables, applies migration operations, and checks the resulting
/// database state via system tables — not just the emitted SQL text.
/// </summary>
public class MigrationIntegrationTests : IAsyncLifetime
{
    private string _connectionString = default!;
    private string _databaseName = default!;

    public async Task InitializeAsync()
    {
        _connectionString = await SharedContainer.GetConnectionStringAsync();
        _databaseName = System.Text.RegularExpressions.Regex.Match(
            _connectionString, @"Database=([^;]+)").Groups[1].Value;
    }

    public Task DisposeAsync() => Task.CompletedTask;

    // ── Finding 3: Expression quoting ───────────────────────────────────────

    [Fact]
    public async Task Expression_orderBy_creates_valid_table()
    {
        await using var ctx = CreateContext(b =>
        {
            b.Entity<IdEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("expr_ob_test", t => t
                    .HasMergeTreeEngine()
                    .WithOrderBy("Id", "Id % 8"));
            });
        });

        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var sortingKey = await QueryScalar(ctx,
            $"SELECT sorting_key FROM system.tables WHERE database = '{_databaseName}' AND name = 'expr_ob_test'");
        Assert.Contains("Id % 8", sortingKey!);
    }

    [Fact]
    public async Task Expression_partitionBy_creates_valid_table()
    {
        await using var ctx = CreateContext(b =>
        {
            b.Entity<TimestampEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("expr_pb_test", t => t
                    .HasMergeTreeEngine()
                    .WithOrderBy("Id")
                    .WithPartitionBy("Id % 4"));
            });
        });

        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var partitionKey = await QueryScalar(ctx,
            $"SELECT partition_key FROM system.tables WHERE database = '{_databaseName}' AND name = 'expr_pb_test'");
        Assert.Contains("Id % 4", partitionKey!);
    }

    [Fact]
    public async Task Multi_column_partitionBy_creates_valid_table()
    {
        await using var ctx = CreateContext(b =>
        {
            b.Entity<EventEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("multi_part_test", t => t
                    .HasMergeTreeEngine()
                    .WithOrderBy("Id")
                    .WithPartitionBy("Region", "toYYYYMM(CreatedAt)"));
            });
        });
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var partitionKey = await QueryScalar(ctx,
            $"SELECT partition_key FROM system.tables WHERE database = '{_databaseName}' AND name = 'multi_part_test'");
        Assert.Contains("Region", partitionKey!);
        Assert.Contains("toYYYYMM(CreatedAt)", partitionKey);
    }

    [Fact]
    public async Task SampleBy_with_expression_creates_valid_table()
    {
        // SAMPLE BY requires unsigned integer — use cityHash64() which returns UInt64
        await using var ctx = CreateContext(b =>
        {
            b.Entity<EventEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("sample_expr_test", t => t
                    .HasMergeTreeEngine()
                    .WithOrderBy("cityHash64(Id)", "Id")
                    .WithSampleBy("cityHash64(Id)"));
            });
        });
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var samplingKey = await QueryScalar(ctx,
            $"SELECT sampling_key FROM system.tables WHERE database = '{_databaseName}' AND name = 'sample_expr_test'");
        Assert.Contains("cityHash64(Id)", samplingKey!);
    }

    [Fact]
    public async Task PartitionBy_sampleBy_and_primaryKey_together()
    {
        await using var ctx = CreateContext(b =>
        {
            b.Entity<EventEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("combo_clause_test", t => t
                    .HasMergeTreeEngine()
                    .WithOrderBy("cityHash64(Id)", "Region", "Id")
                    .WithPartitionBy("Region", "toYYYYMM(CreatedAt)")
                    .WithPrimaryKey("cityHash64(Id)", "Region")
                    .WithSampleBy("cityHash64(Id)"));
            });
        });
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var partitionKey = await QueryScalar(ctx,
            $"SELECT partition_key FROM system.tables WHERE database = '{_databaseName}' AND name = 'combo_clause_test'");
        Assert.Contains("Region", partitionKey!);
        Assert.Contains("toYYYYMM(CreatedAt)", partitionKey);

        var primaryKey = await QueryScalar(ctx,
            $"SELECT primary_key FROM system.tables WHERE database = '{_databaseName}' AND name = 'combo_clause_test'");
        Assert.Contains("cityHash64(Id)", primaryKey!);
        Assert.Contains("Region", primaryKey);

        var samplingKey = await QueryScalar(ctx,
            $"SELECT sampling_key FROM system.tables WHERE database = '{_databaseName}' AND name = 'combo_clause_test'");
        Assert.Contains("cityHash64(Id)", samplingKey!);
    }

    // ── Finding 2: Column annotation removal ────────────────────────────────

    [Fact]
    public async Task Removing_column_codec_clears_codec_in_database()
    {
        // Create table with CODEC on a column
        await using var ctx = CreateContext(b =>
        {
            b.Entity<IdValueEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.Property(x => x.Value).HasCodec("ZSTD");
                e.ToTable("codec_rm_test", t => t.HasMergeTreeEngine().WithOrderBy("Id"));
            });
        });
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        // Verify codec is present
        var codecBefore = await QueryScalar(ctx,
            $"SELECT compression_codec FROM system.columns WHERE database = '{_databaseName}' AND table = 'codec_rm_test' AND name = 'Value'");
        Assert.Contains("ZSTD", codecBefore!);

        // Generate and execute AlterColumn that removes codec
        var op = new AlterColumnOperation
        {
            Table = "codec_rm_test", Name = "Value", ColumnType = "String", ClrType = typeof(string)
        };
        op.OldColumn.AddAnnotation(ClickHouseAnnotationNames.ColumnCodec, "ZSTD");
        await ApplyMigrationAsync(op);

        // Verify codec is gone
        var codecAfter = await QueryScalar(ctx,
            $"SELECT compression_codec FROM system.columns WHERE database = '{_databaseName}' AND table = 'codec_rm_test' AND name = 'Value'");
        Assert.DoesNotContain("ZSTD", codecAfter ?? "");
    }

    [Fact]
    public async Task Removing_column_comment_clears_comment_in_database()
    {
        await using var ctx = CreateContext(b =>
        {
            b.Entity<IdValueEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.Property(x => x.Value).HasColumnComment("test comment");
                e.ToTable("comment_rm_test", t => t.HasMergeTreeEngine().WithOrderBy("Id"));
            });
        });
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var commentBefore = await QueryScalar(ctx,
            $"SELECT comment FROM system.columns WHERE database = '{_databaseName}' AND table = 'comment_rm_test' AND name = 'Value'");
        Assert.Equal("test comment", commentBefore);

        var op = new AlterColumnOperation
        {
            Table = "comment_rm_test", Name = "Value", ColumnType = "String", ClrType = typeof(string)
        };
        op.OldColumn.AddAnnotation(ClickHouseAnnotationNames.ColumnComment, "test comment");
        await ApplyMigrationAsync(op);

        var commentAfter = await QueryScalar(ctx,
            $"SELECT comment FROM system.columns WHERE database = '{_databaseName}' AND table = 'comment_rm_test' AND name = 'Value'");
        Assert.True(string.IsNullOrEmpty(commentAfter), $"Expected empty comment, got: '{commentAfter}'");
    }

    [Fact]
    public async Task Removing_column_ttl_clears_ttl_in_database()
    {
        await using var ctx = CreateContext(b =>
        {
            b.Entity<TimestampEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.Property(x => x.Timestamp).HasColumnTtl("Timestamp + INTERVAL 1 DAY");
                e.ToTable("ttl_rm_test", t => t.HasMergeTreeEngine().WithOrderBy("Id"));
            });
        });
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        // Verify TTL is present in CREATE TABLE output
        var createBefore = await QueryScalar(ctx,
            $"SELECT create_table_query FROM system.tables WHERE database = '{_databaseName}' AND name = 'ttl_rm_test'");
        Assert.Contains("TTL", createBefore!);

        var op = new AlterColumnOperation
        {
            Table = "ttl_rm_test", Name = "Timestamp", ColumnType = "DateTime", ClrType = typeof(DateTime)
        };
        op.OldColumn.AddAnnotation(ClickHouseAnnotationNames.ColumnTtl, "Timestamp + INTERVAL 1 DAY");
        await ApplyMigrationAsync(op);

        var createAfter = await QueryScalar(ctx,
            $"SELECT create_table_query FROM system.tables WHERE database = '{_databaseName}' AND name = 'ttl_rm_test'");
        Assert.DoesNotContain("TTL", createAfter ?? "");
    }

    // ── Finding 1: Skipping index drop ──────────────────────────────────────

    [Fact]
    public async Task Drop_skipping_index_removes_index_from_database()
    {
        // Create table with skipping index via EnsureCreated
        await using var ctx = CreateContext(b =>
        {
            b.Entity<IdValueEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.HasIndex(x => x.Value)
                    .HasDatabaseName("idx_value")
                    .HasSkippingIndexType("set")
                    .HasGranularity(4)
                    .HasSkippingIndexParams("100");
                e.ToTable("idx_drop_test", t => t.HasMergeTreeEngine().WithOrderBy("Id"));
            });
        });
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        // Verify index exists
        var countBefore = await QueryScalar(ctx,
            $"SELECT count() FROM system.data_skipping_indices WHERE database = '{_databaseName}' AND table = 'idx_drop_test' AND name = 'idx_value'");
        Assert.Equal("1", countBefore);

        // Drop the index via migration operation (annotation simulates what ForRemove provides)
        var dropOp = new DropIndexOperation { Table = "idx_drop_test", Name = "idx_value" };
        dropOp.AddAnnotation(ClickHouseAnnotationNames.SkippingIndexType, "set");
        await ApplyMigrationAsync(dropOp);

        // Verify index is gone
        var countAfter = await QueryScalar(ctx,
            $"SELECT count() FROM system.data_skipping_indices WHERE database = '{_databaseName}' AND table = 'idx_drop_test' AND name = 'idx_value'");
        Assert.Equal("0", countAfter);
    }

    [Fact]
    public async Task Model_differ_carries_skipping_index_annotations_to_DropIndexOperation()
    {
        // Model v1: table WITH skipping index
        await using var ctxWithIndex = CreateContext(b =>
        {
            b.Entity<IdValueEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.HasIndex(x => x.Value)
                    .HasDatabaseName("idx_val")
                    .HasSkippingIndexType("set")
                    .HasGranularity(4);
                e.ToTable("differ_idx_test", t => t.HasMergeTreeEngine().WithOrderBy("Id"));
            });
        });

        // Model v2: same table WITHOUT index
        await using var ctxNoIndex = CreateContext(b =>
        {
            b.Entity<IdValueEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("differ_idx_test", t => t.HasMergeTreeEngine().WithOrderBy("Id"));
            });
        });

        var v1Model = ctxWithIndex.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var v2Model = ctxNoIndex.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var differ = ctxNoIndex.GetService<IMigrationsModelDiffer>();

        // Diff: v1 → v2 should produce a DropIndexOperation
        var operations = differ.GetDifferences(v1Model, v2Model);
        var dropOp = Assert.Single(operations.OfType<DropIndexOperation>());

        // The SkippingIndexType annotation must be present (our ForRemove fix)
        var typeAnnotation = dropOp.FindAnnotation(ClickHouseAnnotationNames.SkippingIndexType);
        Assert.NotNull(typeAnnotation);
        Assert.Equal("set", typeAnnotation.Value);
    }

    // ── End-to-end: model differ → SQL gen → execute → verify ───────────────

    [Fact]
    public async Task Model_differ_drop_index_end_to_end()
    {
        // Step 1: Create table with skipping index
        await using var ctxV1 = CreateContext(b =>
        {
            b.Entity<IdValueEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.HasIndex(x => x.Value)
                    .HasDatabaseName("idx_e2e")
                    .HasSkippingIndexType("minmax")
                    .HasGranularity(3);
                e.ToTable("e2e_idx_test", t => t.HasMergeTreeEngine().WithOrderBy("Id"));
            });
        });
        await ctxV1.Database.EnsureDeletedAsync();
        await ctxV1.Database.EnsureCreatedAsync();

        // Verify index exists
        var countBefore = await QueryScalar(ctxV1,
            $"SELECT count() FROM system.data_skipping_indices WHERE database = '{_databaseName}' AND table = 'e2e_idx_test' AND name = 'idx_e2e'");
        Assert.Equal("1", countBefore);

        // Step 2: Model v2 without the index
        await using var ctxV2 = CreateContext(b =>
        {
            b.Entity<IdValueEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("e2e_idx_test", t => t.HasMergeTreeEngine().WithOrderBy("Id"));
            });
        });

        // Step 3: Diff → Generate → Execute
        var v1Model = ctxV1.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var v2Model = ctxV2.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var differ = ctxV2.GetService<IMigrationsModelDiffer>();
        var operations = differ.GetDifferences(v1Model, v2Model);

        var generator = ctxV2.GetService<IMigrationsSqlGenerator>();
        var commands = generator.Generate(operations);

        using var conn = new global::ClickHouse.Driver.ADO.ClickHouseConnection(_connectionString);
        await conn.OpenAsync();
        foreach (var command in commands)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = command.CommandText;
            await cmd.ExecuteNonQueryAsync();
        }

        // Step 4: Verify index is gone
        var countAfter = await QueryScalar(ctxV1,
            $"SELECT count() FROM system.data_skipping_indices WHERE database = '{_databaseName}' AND table = 'e2e_idx_test' AND name = 'idx_e2e'");
        Assert.Equal("0", countAfter);
    }

    // ── Nullable container types ────────────────────────────────────────────

    [Fact]
    public async Task Nullable_List_column_creates_Array_not_Nullable_Array()
    {
        await using var ctx = CreateContext(b =>
        {
            b.Entity<ListEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.Property(x => x.Tags).HasColumnType("Array(String)");
                e.ToTable("list_nullable_test", t => t.HasMergeTreeEngine().WithOrderBy("Id"));
            });
        });
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        // Verify column type is Array(String), not Nullable(Array(String))
        var colType = await QueryScalar(ctx,
            $"SELECT type FROM system.columns WHERE database = '{_databaseName}' AND table = 'list_nullable_test' AND name = 'Tags'");
        Assert.StartsWith("Array(", colType!);
        Assert.DoesNotContain("Nullable(Array", colType);
    }

    [Fact]
    public async Task Nullable_Map_column_creates_Map_not_Nullable_Map()
    {
        await using var ctx = CreateContext(b =>
        {
            b.Entity<MapEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.Property(x => x.Meta).HasColumnType("Map(String, String)");
                e.ToTable("map_nullable_test", t => t.HasMergeTreeEngine().WithOrderBy("Id"));
            });
        });
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var colType = await QueryScalar(ctx,
            $"SELECT type FROM system.columns WHERE database = '{_databaseName}' AND table = 'map_nullable_test' AND name = 'Meta'");
        Assert.StartsWith("Map(", colType!);
        Assert.DoesNotContain("Nullable(Map", colType);
    }

    [Fact]
    public async Task Null_List_inserts_as_empty_array_and_reads_back()
    {
        await using var ctx = CreateContext(b =>
        {
            b.Entity<ListEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.Property(x => x.Tags).HasColumnType("Array(String)");
                e.ToTable("list_null_test", t => t.HasMergeTreeEngine().WithOrderBy("Id"));
            });
        });
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        // Insert with null list
        ctx.Set<ListEntity>().Add(new ListEntity { Id = 1, Tags = null });
        // Insert with populated list
        ctx.Set<ListEntity>().Add(new ListEntity { Id = 2, Tags = ["a", "b"] });
        await ctx.SaveChangesAsync();

        await using var readCtx = CreateContext(b =>
        {
            b.Entity<ListEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.Property(x => x.Tags).HasColumnType("Array(String)");
                e.ToTable("list_null_test", t => t.HasMergeTreeEngine().WithOrderBy("Id"));
            });
        });

        var e1 = await readCtx.Set<ListEntity>().FirstAsync(e => e.Id == 1);
        Assert.NotNull(e1.Tags);
        Assert.Empty(e1.Tags);

        var e2 = await readCtx.Set<ListEntity>().FirstAsync(e => e.Id == 2);
        Assert.Equal(["a", "b"], e2.Tags);
    }

    // ── Issue #18: LowCardinality wrapper preservation ──────────────────────

    [Fact]
    public async Task LowCardinalityColumn_CreatesLowCardinalityInDatabase()
    {
        await using var ctx = CreateContext(b =>
        {
            b.Entity<IdValueEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.Property(x => x.Value).HasColumnType("LowCardinality(String)");
                e.ToTable("lowcard_migration_test", t => t.HasMergeTreeEngine().WithOrderBy("Id"));
            });
        });
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var colType = await QueryScalar(ctx,
            $"SELECT type FROM system.columns WHERE database = '{_databaseName}' AND table = 'lowcard_migration_test' AND name = 'Value'");
        Assert.Equal("LowCardinality(String)", colType);
    }

    // ── ReplacingMergeTree isDeleted ─────────────────────────────────────────

    [Fact]
    public async Task ReplacingMergeTree_with_isDeleted_creates_valid_table()
    {
        await using var ctx = CreateContext(b =>
        {
            b.Entity<VersionedDeleteEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("rmt_deleted_test", t => t
                    .HasReplacingMergeTreeEngine("Version", "IsDeleted")
                    .WithOrderBy("Id"));
            });
        });
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var engine = await QueryScalar(ctx,
            $"SELECT engine FROM system.tables WHERE database = '{_databaseName}' AND name = 'rmt_deleted_test'");
        Assert.Equal("ReplacingMergeTree", engine);

        // Verify the create_table_query includes both version and isDeleted args
        var createSql = await QueryScalar(ctx,
            $"SELECT create_table_query FROM system.tables WHERE database = '{_databaseName}' AND name = 'rmt_deleted_test'");
        Assert.Contains("ReplacingMergeTree(", createSql!);
        Assert.Contains("Version", createSql);
        Assert.Contains("IsDeleted", createSql);
    }

    [Fact]
    public async Task ReplacingMergeTree_with_isDeleted_insert_and_query()
    {
        await using var ctx = CreateContext(b =>
        {
            b.Entity<VersionedDeleteEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("rmt_deleted_rtrip", t => t
                    .HasReplacingMergeTreeEngine("Version", "IsDeleted")
                    .WithOrderBy("Id"));
            });
        });
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Set<VersionedDeleteEntity>().Add(new VersionedDeleteEntity
        {
            Id = 1, Name = "test", Version = 1, IsDeleted = 0
        });
        await ctx.SaveChangesAsync();

        await using var readCtx = CreateContext(b =>
        {
            b.Entity<VersionedDeleteEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("rmt_deleted_rtrip", t => t
                    .HasReplacingMergeTreeEngine("Version", "IsDeleted")
                    .WithOrderBy("Id"));
            });
        });

        var entity = await readCtx.Set<VersionedDeleteEntity>().FirstAsync(e => e.Id == 1);
        Assert.Equal("test", entity.Name);
        Assert.Equal((byte)0, entity.IsDeleted);
    }

    // ── Column with all features: default + codec + comment + TTL ──────────

    [Fact]
    public async Task Column_with_default_codec_comment_and_ttl()
    {
        await using var ctx = CreateContext(b =>
        {
            b.Entity<FullFeaturedEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.Property(x => x.Name)
                    .HasDefaultValue("unnamed")
                    .HasCodec("ZSTD")
                    .HasColumnComment("The display name");
                e.Property(x => x.CreatedAt)
                    .HasDefaultValueSql("now()")
                    .HasColumnTtl("CreatedAt + INTERVAL 30 DAY")
                    .HasColumnComment("Row creation time")
                    .HasCodec("Delta, ZSTD");
                e.ToTable("full_feat_test", t => t.HasMergeTreeEngine().WithOrderBy("Id"));
            });
        });
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        // Verify Name column: default, codec, comment
        var nameDefault = await QueryScalar(ctx,
            $"SELECT default_expression FROM system.columns WHERE database = '{_databaseName}' AND table = 'full_feat_test' AND name = 'Name'");
        Assert.Contains("unnamed", nameDefault!);

        var nameCodec = await QueryScalar(ctx,
            $"SELECT compression_codec FROM system.columns WHERE database = '{_databaseName}' AND table = 'full_feat_test' AND name = 'Name'");
        Assert.Contains("ZSTD", nameCodec!);

        var nameComment = await QueryScalar(ctx,
            $"SELECT comment FROM system.columns WHERE database = '{_databaseName}' AND table = 'full_feat_test' AND name = 'Name'");
        Assert.Equal("The display name", nameComment);

        // Verify CreatedAt column: default (now()), codec, comment, TTL
        var createdDefault = await QueryScalar(ctx,
            $"SELECT default_expression FROM system.columns WHERE database = '{_databaseName}' AND table = 'full_feat_test' AND name = 'CreatedAt'");
        Assert.Contains("now()", createdDefault!);

        var createdCodec = await QueryScalar(ctx,
            $"SELECT compression_codec FROM system.columns WHERE database = '{_databaseName}' AND table = 'full_feat_test' AND name = 'CreatedAt'");
        Assert.Contains("Delta", createdCodec!);
        Assert.Contains("ZSTD", createdCodec);

        var createdComment = await QueryScalar(ctx,
            $"SELECT comment FROM system.columns WHERE database = '{_databaseName}' AND table = 'full_feat_test' AND name = 'CreatedAt'");
        Assert.Equal("Row creation time", createdComment);

        // TTL appears in the CREATE TABLE statement
        var createSql = await QueryScalar(ctx,
            $"SELECT create_table_query FROM system.tables WHERE database = '{_databaseName}' AND name = 'full_feat_test'");
        Assert.Contains("TTL", createSql!);
        Assert.Contains("CreatedAt", createSql);
    }

    [Fact]
    public async Task Column_with_default_value_applied_by_clickhouse_on_insert()
    {
        await using var ctx = CreateContext(b =>
        {
            b.Entity<FullFeaturedEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.Property(x => x.Name).HasDefaultValue("unnamed");
                e.Property(x => x.CreatedAt).HasDefaultValueSql("now()");
                e.ToTable("default_val_test", t => t.HasMergeTreeEngine().WithOrderBy("Id"));
            });
        });
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        // Insert via raw SQL (omitting Name and CreatedAt so ClickHouse applies defaults)
        using var conn = new global::ClickHouse.Driver.ADO.ClickHouseConnection(_connectionString);
        await conn.OpenAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT INTO default_val_test (Id) VALUES (1)";
        await cmd.ExecuteNonQueryAsync();

        // Verify defaults were applied
        var name = await QueryScalar(ctx, "SELECT Name FROM default_val_test WHERE Id = 1");
        Assert.Equal("unnamed", name);

        var createdAt = await QueryScalar(ctx, "SELECT CreatedAt FROM default_val_test WHERE Id = 1");
        Assert.NotNull(createdAt);
        Assert.NotEqual("1970-01-01 00:00:00", createdAt); // not epoch — got now()
    }

    // ── Materialized view operations ──────────────────────────────────────

    [Fact]
    public async Task CreateMaterializedView_creates_view_and_populates_target()
    {
        await ExecuteRawAsync(
            "CREATE TABLE IF NOT EXISTS mv_source (ts DateTime, value UInt64) ENGINE = MergeTree() ORDER BY ts",
            "CREATE TABLE IF NOT EXISTS mv_target (hour DateTime, total UInt64) ENGINE = SummingMergeTree() ORDER BY hour");

        await ApplyMigrationAsync(new ClickHouseCreateMaterializedViewOperation
        {
            ViewName = "mv_test_view",
            TargetTable = "mv_target",
            SelectQuery = "SELECT toStartOfHour(ts) AS hour, sum(value) AS total FROM mv_source GROUP BY hour",
        });

        var engine = await QueryScalarRaw(
            "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'mv_test_view'");
        Assert.Equal("MaterializedView", engine);

        // DDL shape: the view targets mv_target via TO, and the SELECT body is preserved.
        var createSql = await QueryScalarRaw(
            "SELECT create_table_query FROM system.tables WHERE database = currentDatabase() AND name = 'mv_test_view'");
        Assert.Contains("TO", createSql!);
        Assert.Contains("mv_target", createSql!);
        var asSelect = await QueryScalarRaw(
            "SELECT as_select FROM system.tables WHERE database = currentDatabase() AND name = 'mv_test_view'");
        Assert.Contains("sum(value)", asSelect!);
        Assert.Contains("GROUP BY", asSelect!);

        await ExecuteRawAsync(
            "INSERT INTO mv_source VALUES ('2024-01-01 10:30:00', 5), ('2024-01-01 10:45:00', 3)");

        var total = await QueryScalarRaw("SELECT total FROM mv_target");
        Assert.Equal("8", total);

        await ExecuteRawAsync(
            "DROP VIEW IF EXISTS mv_test_view",
            "DROP TABLE IF EXISTS mv_target",
            "DROP TABLE IF EXISTS mv_source");
    }

    [Fact]
    public async Task DropMaterializedView_removes_view_but_keeps_target()
    {
        await ExecuteRawAsync(
            "CREATE TABLE IF NOT EXISTS mv_drop_source (id UInt64) ENGINE = MergeTree() ORDER BY id",
            "CREATE TABLE IF NOT EXISTS mv_drop_target (id UInt64) ENGINE = MergeTree() ORDER BY id",
            "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_drop_test TO mv_drop_target AS SELECT id FROM mv_drop_source");

        await ApplyMigrationAsync(new ClickHouseDropMaterializedViewOperation
        {
            ViewName = "mv_drop_test",
        });

        var viewCount = await QueryScalarRaw(
            "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'mv_drop_test'");
        Assert.Equal("0", viewCount);

        var targetCount = await QueryScalarRaw(
            "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'mv_drop_target'");
        Assert.Equal("1", targetCount);

        await ExecuteRawAsync(
            "DROP TABLE IF EXISTS mv_drop_target",
            "DROP TABLE IF EXISTS mv_drop_source");
    }

    [Fact]
    public async Task CreateMaterializedView_IfNotExists_is_idempotent()
    {
        await ExecuteRawAsync(
            "CREATE TABLE IF NOT EXISTS mv_idem_source (id UInt64) ENGINE = MergeTree() ORDER BY id",
            "CREATE TABLE IF NOT EXISTS mv_idem_target (id UInt64) ENGINE = MergeTree() ORDER BY id");

        var operation = new ClickHouseCreateMaterializedViewOperation
        {
            ViewName = "mv_idem_view",
            TargetTable = "mv_idem_target",
            SelectQuery = "SELECT id FROM mv_idem_source",
            IfNotExists = true,
        };

        await ApplyMigrationAsync(operation);
        await ApplyMigrationAsync(operation);

        await ExecuteRawAsync(
            "DROP VIEW IF EXISTS mv_idem_view",
            "DROP TABLE IF EXISTS mv_idem_target",
            "DROP TABLE IF EXISTS mv_idem_source");
    }

    [Fact]
    public async Task DropMaterializedView_IfExists_is_idempotent()
    {
        var operation = new ClickHouseDropMaterializedViewOperation
        {
            ViewName = "mv_nonexistent_view",
            IfExists = true,
        };

        await ApplyMigrationAsync(operation);
    }

    // ── Model-level materialized view tests (end-to-end through differ) ─────

    [Fact]
    public async Task ModelLevel_FromRaw_view_creates_via_differ()
    {
        await ExecuteRawAsync(
            "DROP VIEW IF EXISTS mv_model_raw_view",
            "DROP TABLE IF EXISTS mv_model_raw_source",
            "DROP TABLE IF EXISTS mv_model_raw_target");

        await using var ctx = CreateContext(b =>
        {
            b.Entity<MvSource>(e => { e.HasKey(x => x.Id); e.ToTable("mv_model_raw_source"); });
            b.Entity<MvTarget>(e =>
            {
                e.HasKey(x => x.Bucket);
                e.ToTable("mv_model_raw_target", t => t.HasSummingMergeTreeEngine("Total").WithOrderBy("Bucket"));
            });

            b.HasMaterializedView<MvTarget>("mv_model_raw_view")
                .FromRaw("SELECT Id AS Bucket, Value AS Total FROM mv_model_raw_source");
        });

        var prevModel = CreateContext(_ => { }).GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var currModel = ctx.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var operations = ctx.GetService<IMigrationsModelDiffer>().GetDifferences(prevModel, currModel);
        var generator = ctx.GetService<IMigrationsSqlGenerator>();

        using var conn = new global::ClickHouse.Driver.ADO.ClickHouseConnection(_connectionString);
        await conn.OpenAsync();
        foreach (var command in generator.Generate(operations))
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = command.CommandText;
            await cmd.ExecuteNonQueryAsync();
        }

        var engine = await QueryScalarRaw(
            "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'mv_model_raw_view'");
        Assert.Equal("MaterializedView", engine);

        // DDL shape: the differ-built view points at the EF-mapped target table and keeps the FromRaw body.
        var createSql = await QueryScalarRaw(
            "SELECT create_table_query FROM system.tables WHERE database = currentDatabase() AND name = 'mv_model_raw_view'");
        Assert.Contains("TO", createSql!);
        Assert.Contains("mv_model_raw_target", createSql!);
        var asSelect = await QueryScalarRaw(
            "SELECT as_select FROM system.tables WHERE database = currentDatabase() AND name = 'mv_model_raw_view'");
        Assert.Contains("mv_model_raw_source", asSelect!);
        Assert.Contains("Bucket", asSelect!);

        await ExecuteRawAsync("INSERT INTO mv_model_raw_source VALUES (1, 10), (1, 5), (2, 7)");
        var totalForOne = await QueryScalarRaw("SELECT Total FROM mv_model_raw_target FINAL WHERE Bucket = 1");
        Assert.Equal("15", totalForOne);

        await ExecuteRawAsync(
            "DROP VIEW IF EXISTS mv_model_raw_view",
            "DROP TABLE IF EXISTS mv_model_raw_source",
            "DROP TABLE IF EXISTS mv_model_raw_target");
    }

    [Fact]
    public async Task ModelLevel_LINQ_view_translates_and_executes()
    {
        await ExecuteRawAsync(
            "DROP VIEW IF EXISTS mv_model_linq_view",
            "DROP TABLE IF EXISTS mv_model_linq_source",
            "DROP TABLE IF EXISTS mv_model_linq_target");

        await using var ctx = CreateContext(b =>
        {
            b.Entity<MvSource>(e => { e.HasKey(x => x.Id); e.ToTable("mv_model_linq_source"); });
            b.Entity<MvTarget>(e =>
            {
                e.HasKey(x => x.Bucket);
                e.ToTable("mv_model_linq_target", t => t.HasSummingMergeTreeEngine("Total").WithOrderBy("Bucket"));
            });

            b.HasMaterializedView<MvTarget>("mv_model_linq_view")
                .From<MvSource>()
                .Select(src => src
                    .GroupBy(s => s.Id)
                    .Select(g => new MvTarget { Bucket = g.Key, Total = (long)g.Sum(s => s.Value) }));
        });

        var prevModel = CreateContext(_ => { }).GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var currModel = ctx.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var operations = ctx.GetService<IMigrationsModelDiffer>().GetDifferences(prevModel, currModel);
        var generator = ctx.GetService<IMigrationsSqlGenerator>();

        using var conn = new global::ClickHouse.Driver.ADO.ClickHouseConnection(_connectionString);
        await conn.OpenAsync();
        foreach (var command in generator.Generate(operations))
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = command.CommandText;
            await cmd.ExecuteNonQueryAsync();
        }

        // View should exist
        var engine = await QueryScalarRaw(
            "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'mv_model_linq_view'");
        Assert.Equal("MaterializedView", engine);

        // DDL shape: the LINQ body was translated to ClickHouse SQL at differ time (GROUP BY over the source),
        // and the view points at the EF-mapped target.
        var createSql = await QueryScalarRaw(
            "SELECT create_table_query FROM system.tables WHERE database = currentDatabase() AND name = 'mv_model_linq_view'");
        Assert.Contains("TO", createSql!);
        Assert.Contains("mv_model_linq_target", createSql!);
        var asSelect = await QueryScalarRaw(
            "SELECT as_select FROM system.tables WHERE database = currentDatabase() AND name = 'mv_model_linq_view'");
        Assert.Contains("mv_model_linq_source", asSelect!);
        Assert.Contains("GROUP BY", asSelect!);

        // Data flows through
        await ExecuteRawAsync("INSERT INTO mv_model_linq_source VALUES (1, 10), (1, 5), (2, 7)");
        var totalForOne = await QueryScalarRaw("SELECT Total FROM mv_model_linq_target FINAL WHERE Bucket = 1");
        Assert.Equal("15", totalForOne);

        await ExecuteRawAsync(
            "DROP VIEW IF EXISTS mv_model_linq_view",
            "DROP TABLE IF EXISTS mv_model_linq_source",
            "DROP TABLE IF EXISTS mv_model_linq_target");
    }

    [Fact]
    public async Task ModelLevel_changing_view_drops_and_recreates()
    {
        await ExecuteRawAsync(
            "DROP VIEW IF EXISTS mv_change_view",
            "DROP TABLE IF EXISTS mv_change_source",
            "DROP TABLE IF EXISTS mv_change_target");

        Action<ModelBuilder> mapTables = b =>
        {
            b.Entity<MvSource>(e => { e.HasKey(x => x.Id); e.ToTable("mv_change_source"); });
            b.Entity<MvTarget>(e =>
            {
                e.HasKey(x => x.Bucket);
                e.ToTable("mv_change_target", t => t.HasSummingMergeTreeEngine("Total").WithOrderBy("Bucket"));
            });
        };

        // v1: Total = Value
        await using var ctxV1 = CreateContext(b =>
        {
            mapTables(b);
            b.HasMaterializedView<MvTarget>("mv_change_view")
                .FromRaw("SELECT Id AS Bucket, Value AS Total FROM mv_change_source");
        });
        await using var emptyCtx = CreateContext(_ => { });

        var emptyModel = emptyCtx.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var v1Model = ctxV1.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        await ApplyOperationsAsync(
            ctxV1.GetService<IMigrationsModelDiffer>().GetDifferences(emptyModel, v1Model), ctxV1);

        var asSelectV1 = await QueryScalarRaw(
            "SELECT as_select FROM system.tables WHERE database = currentDatabase() AND name = 'mv_change_view'");
        Assert.Contains("mv_change_source", asSelectV1!);

        // v2: Total = Value * 10 — different SELECT body forces a drop + recreate.
        await using var ctxV2 = CreateContext(b =>
        {
            mapTables(b);
            b.HasMaterializedView<MvTarget>("mv_change_view")
                .FromRaw("SELECT Id AS Bucket, Value * 10 AS Total FROM mv_change_source");
        });

        var v2Model = ctxV2.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var changeOps = ctxV2.GetService<IMigrationsModelDiffer>().GetDifferences(v1Model, v2Model);

        // The differ emits both a drop and a recreate for the changed view.
        Assert.Contains(changeOps, o => o is ClickHouseDropMaterializedViewOperation);
        Assert.Contains(changeOps, o => o is ClickHouseCreateMaterializedViewOperation);

        await ApplyOperationsAsync(changeOps, ctxV2);

        var engine = await QueryScalarRaw(
            "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'mv_change_view'");
        Assert.Equal("MaterializedView", engine);

        var asSelectV2 = await QueryScalarRaw(
            "SELECT as_select FROM system.tables WHERE database = currentDatabase() AND name = 'mv_change_view'");
        Assert.NotEqual(asSelectV1, asSelectV2);

        // The recreated view (Value * 10) is the one now running.
        await ExecuteRawAsync("INSERT INTO mv_change_source VALUES (1, 5)");
        var total = await QueryScalarRaw("SELECT Total FROM mv_change_target FINAL WHERE Bucket = 1");
        Assert.Equal("50", total);

        await ExecuteRawAsync(
            "DROP VIEW IF EXISTS mv_change_view",
            "DROP TABLE IF EXISTS mv_change_source",
            "DROP TABLE IF EXISTS mv_change_target");
    }

    [Fact]
    public async Task ModelLevel_view_and_target_in_other_database()
    {
        var otherDb = _databaseName + "_alt";
        await ExecuteRawAsync(
            $"DROP DATABASE IF EXISTS {otherDb}",
            $"CREATE DATABASE {otherDb}",
            $"CREATE TABLE {otherDb}.qual_source (Id UInt64, Value UInt64) ENGINE = MergeTree() ORDER BY Id",
            $"CREATE TABLE {otherDb}.qual_target (Bucket UInt64, Total UInt64) ENGINE = SummingMergeTree() ORDER BY Bucket");

        // Map the target into the other database (schema). Pre-create the tables so the differ
        // only has to emit the view; the view itself is placed in the other database via InDatabase.
        Action<ModelBuilder> mapTarget = b =>
            b.Entity<MvTarget>(e =>
            {
                e.HasKey(x => x.Bucket);
                e.ToTable("qual_target", otherDb, t => t.HasSummingMergeTreeEngine("Total").WithOrderBy("Bucket"));
            });

        await using var baseCtx = CreateContext(mapTarget);
        await using var viewCtx = CreateContext(b =>
        {
            mapTarget(b);
            b.HasMaterializedView<MvTarget>("qual_view")
                .FromRaw($"SELECT Id AS Bucket, Value AS Total FROM {otherDb}.qual_source")
                .InDatabase(otherDb);
        });

        var prevModel = baseCtx.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var currModel = viewCtx.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        await ApplyOperationsAsync(
            viewCtx.GetService<IMigrationsModelDiffer>().GetDifferences(prevModel, currModel), viewCtx);

        var engine = await QueryScalarRaw(
            $"SELECT engine FROM system.tables WHERE database = '{otherDb}' AND name = 'qual_view'");
        Assert.Equal("MaterializedView", engine);

        // DDL is qualified with the other database for both the view and its TO target.
        var createSql = await QueryScalarRaw(
            $"SELECT create_table_query FROM system.tables WHERE database = '{otherDb}' AND name = 'qual_view'");
        Assert.Contains(otherDb, createSql!);
        Assert.Contains("qual_target", createSql!);

        await ExecuteRawAsync($"INSERT INTO {otherDb}.qual_source VALUES (1, 10), (1, 5)");
        var total = await QueryScalarRaw($"SELECT Total FROM {otherDb}.qual_target FINAL WHERE Bucket = 1");
        Assert.Equal("15", total);

        await ExecuteRawAsync($"DROP DATABASE IF EXISTS {otherDb}");
    }

    [Fact]
    public async Task ModelLevel_multi_source_join_view_translates_and_executes()
    {
        await ExecuteRawAsync(
            "DROP VIEW IF EXISTS mv_join_view",
            "DROP TABLE IF EXISTS mv_join_left",
            "DROP TABLE IF EXISTS mv_join_right",
            "DROP TABLE IF EXISTS mv_join_target");

        await using var ctx = CreateContext(b =>
        {
            b.Entity<JoinLeft>(e => { e.HasKey(x => x.Id); e.ToTable("mv_join_left"); });
            b.Entity<JoinRight>(e => { e.HasKey(x => x.Id); e.ToTable("mv_join_right"); });
            b.Entity<MvTarget>(e =>
            {
                e.HasKey(x => x.Bucket);
                e.ToTable("mv_join_target", t => t.HasSummingMergeTreeEngine("Total").WithOrderBy("Bucket"));
            });

            b.HasMaterializedView<MvTarget>("mv_join_view")
                .From<JoinLeft>()
                .Join<JoinRight>()
                .Select((left, right) => left
                    .Join(right, l => l.Id, r => r.Id, (l, r) => new MvTarget { Bucket = l.Id, Total = l.Value * r.Factor }));
        });

        var prevModel = CreateContext(_ => { }).GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var currModel = ctx.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        await ApplyOperationsAsync(
            ctx.GetService<IMigrationsModelDiffer>().GetDifferences(prevModel, currModel), ctx);

        var engine = await QueryScalarRaw(
            "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'mv_join_view'");
        Assert.Equal("MaterializedView", engine);

        // The multi-source LINQ body was translated to a JOIN over both source tables.
        var asSelect = await QueryScalarRaw(
            "SELECT as_select FROM system.tables WHERE database = currentDatabase() AND name = 'mv_join_view'");
        Assert.Contains("JOIN", asSelect!);
        Assert.Contains("mv_join_left", asSelect!);
        Assert.Contains("mv_join_right", asSelect!);

        // The right table must hold data before the left insert triggers the joined view.
        await ExecuteRawAsync("INSERT INTO mv_join_right VALUES (1, 3)");
        await ExecuteRawAsync("INSERT INTO mv_join_left VALUES (1, 10)");
        var total = await QueryScalarRaw("SELECT Total FROM mv_join_target FINAL WHERE Bucket = 1");
        Assert.Equal("30", total);

        await ExecuteRawAsync(
            "DROP VIEW IF EXISTS mv_join_view",
            "DROP TABLE IF EXISTS mv_join_left",
            "DROP TABLE IF EXISTS mv_join_right",
            "DROP TABLE IF EXISTS mv_join_target");
    }

    // Note: ClickHouse forbids combining POPULATE with a TO clause. Since this provider's
    // model-level materialized views always use TO-table (the target is an EF-mapped entity),
    // POPULATE is intentionally not exposed at the model level. Use INSERT INTO target SELECT ...
    // FROM source in a follow-up migration if backfill is needed.

    // ── Projection operations ─────────────────────────────────────────────

    [Fact]
    public async Task AddProjection_materializes_existing_parts_then_DropProjection_removes_it()
    {
        await ExecuteRawAsync(
            "DROP TABLE IF EXISTS proj_events",
            "CREATE TABLE proj_events (Id UInt64, Category String, Amount UInt64) ENGINE = MergeTree() ORDER BY Id");

        // Insert rows BEFORE the projection exists so the auto-MATERIALIZE has to backfill existing parts.
        await ExecuteRawAsync(
            "INSERT INTO proj_events VALUES (1, 'a', 10), (2, 'b', 5), (3, 'a', 7)");

        await ApplyMigrationAsync(new ClickHouseAddProjectionOperation
        {
            Table = "proj_events",
            ProjectionName = "proj_by_category",
            SelectQuery = "SELECT Category, sum(Amount) GROUP BY Category",
        });

        // The projection is part of the table definition…
        var createSql = await QueryScalarRaw(
            "SELECT create_table_query FROM system.tables WHERE database = currentDatabase() AND name = 'proj_events'");
        Assert.Contains("PROJECTION", createSql!);
        Assert.Contains("proj_by_category", createSql!);

        // …and the auto-MATERIALIZE backfilled the pre-existing part (so projection parts are active).
        var activeProjectionParts = await QueryScalarRaw(
            "SELECT count() FROM system.projection_parts "
            + "WHERE database = currentDatabase() AND table = 'proj_events' AND name = 'proj_by_category' AND active");
        Assert.NotEqual("0", activeProjectionParts);

        // Aggregation results are correct (category 'a' → 10 + 7).
        var totalA = await QueryScalarRaw("SELECT sum(Amount) FROM proj_events WHERE Category = 'a'");
        Assert.Equal("17", totalA);

        await ApplyMigrationAsync(new ClickHouseDropProjectionOperation
        {
            Table = "proj_events",
            ProjectionName = "proj_by_category",
            IfExists = true,
        });

        var createSqlAfter = await QueryScalarRaw(
            "SELECT create_table_query FROM system.tables WHERE database = currentDatabase() AND name = 'proj_events'");
        Assert.DoesNotContain("proj_by_category", createSqlAfter!);

        await ExecuteRawAsync("DROP TABLE IF EXISTS proj_events");
    }

    [Fact]
    public async Task AddProjection_without_materialize_adds_definition_but_no_parts()
    {
        await ExecuteRawAsync(
            "DROP TABLE IF EXISTS proj_nomat",
            "CREATE TABLE proj_nomat (Id UInt64, Category String) ENGINE = MergeTree() ORDER BY Id",
            "INSERT INTO proj_nomat VALUES (1, 'a'), (2, 'b')");

        await ApplyMigrationAsync(new ClickHouseAddProjectionOperation
        {
            Table = "proj_nomat",
            ProjectionName = "proj_cat",
            SelectQuery = "SELECT Category ORDER BY Category",
            Materialize = false,
        });

        var createSql = await QueryScalarRaw(
            "SELECT create_table_query FROM system.tables WHERE database = currentDatabase() AND name = 'proj_nomat'");
        Assert.Contains("proj_cat", createSql!);

        // Without MATERIALIZE the existing part is not projected yet.
        var activeProjectionParts = await QueryScalarRaw(
            "SELECT count() FROM system.projection_parts "
            + "WHERE database = currentDatabase() AND table = 'proj_nomat' AND name = 'proj_cat' AND active");
        Assert.Equal("0", activeProjectionParts);

        await ExecuteRawAsync("DROP TABLE IF EXISTS proj_nomat");
    }

    [Fact]
    public async Task ModelLevel_FromRaw_projection_creates_via_differ()
    {
        await ExecuteRawAsync("DROP TABLE IF EXISTS proj_model_events");

        Action<ModelBuilder> mapTable = b =>
            b.Entity<ProjEvent>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("proj_model_events", t => t.HasMergeTreeEngine().WithOrderBy("Id"));
            });

        // Baseline: the table only.
        await using var baseCtx = CreateContext(mapTable);
        var emptyModel = CreateContext(_ => { }).GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var baseModel = baseCtx.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        await ApplyOperationsAsync(
            baseCtx.GetService<IMigrationsModelDiffer>().GetDifferences(emptyModel, baseModel), baseCtx);

        // Add the projection (table already exists → diff emits only ADD PROJECTION).
        await using var ctx = CreateContext(b =>
        {
            mapTable(b);
            b.Entity<ProjEvent>().HasProjection("proj_cat")
                .FromRaw("SELECT Category, sum(Amount) GROUP BY Category");
        });

        var currModel = ctx.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var ops = ctx.GetService<IMigrationsModelDiffer>().GetDifferences(baseModel, currModel);
        Assert.Contains(ops, o => o is ClickHouseAddProjectionOperation);
        await ApplyOperationsAsync(ops, ctx);

        var createSql = await QueryScalarRaw(
            "SELECT create_table_query FROM system.tables WHERE database = currentDatabase() AND name = 'proj_model_events'");
        Assert.Contains("proj_cat", createSql!);

        await ExecuteRawAsync("DROP TABLE IF EXISTS proj_model_events");
    }

    [Fact]
    public async Task ModelLevel_LINQ_projection_translates_strips_FROM_and_applies()
    {
        await ExecuteRawAsync("DROP TABLE IF EXISTS proj_linq_events");

        Action<ModelBuilder> mapTable = b =>
            b.Entity<ProjEvent>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("proj_linq_events", t => t.HasMergeTreeEngine().WithOrderBy("Id"));
            });

        await using var baseCtx = CreateContext(mapTable);
        var emptyModel = CreateContext(_ => { }).GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var baseModel = baseCtx.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        await ApplyOperationsAsync(
            baseCtx.GetService<IMigrationsModelDiffer>().GetDifferences(emptyModel, baseModel), baseCtx);

        // LINQ-defined projection: translated to SQL and FROM-stripped at differ time.
        await using var ctx = CreateContext(b =>
        {
            mapTable(b);
            b.Entity<ProjEvent>().HasProjection("proj_linq")
                .Select(q => q.GroupBy(e => e.Category)
                    .Select(g => new { Category = g.Key, Total = g.Sum(e => e.Amount) }));
        });

        var currModel = ctx.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var ops = ctx.GetService<IMigrationsModelDiffer>().GetDifferences(baseModel, currModel);

        var add = Assert.Single(ops.OfType<ClickHouseAddProjectionOperation>());
        Assert.DoesNotContain("FROM", add.SelectQuery, StringComparison.OrdinalIgnoreCase);

        // The generated ADD PROJECTION DDL must be valid ClickHouse — applying it would throw otherwise.
        await ApplyOperationsAsync(ops, ctx);

        var createSql = await QueryScalarRaw(
            "SELECT create_table_query FROM system.tables WHERE database = currentDatabase() AND name = 'proj_linq_events'");
        Assert.Contains("proj_linq", createSql!);
        Assert.Contains("GROUP BY", createSql!, StringComparison.OrdinalIgnoreCase);

        // Data flows through and the projection serves the aggregate.
        await ExecuteRawAsync("INSERT INTO proj_linq_events VALUES (1, 'a', 10), (2, 'a', 5), (3, 'b', 7)");
        var totalA = await QueryScalarRaw("SELECT sum(Amount) FROM proj_linq_events WHERE Category = 'a'");
        Assert.Equal("15", totalA);

        await ExecuteRawAsync("DROP TABLE IF EXISTS proj_linq_events");
    }

    [Fact]
    public async Task ModelLevel_changing_projection_drops_and_recreates_over_existing_parts()
    {
        await ExecuteRawAsync("DROP TABLE IF EXISTS proj_change_events");

        Action<ModelBuilder> mapTable = b =>
            b.Entity<ProjEvent>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("proj_change_events", t => t.HasMergeTreeEngine().WithOrderBy("Id"));
            });

        // Baseline table only.
        await using var baseCtx = CreateContext(mapTable);
        var emptyModel = CreateContext(_ => { }).GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var tableOnlyModel = baseCtx.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        await ApplyOperationsAsync(
            baseCtx.GetService<IMigrationsModelDiffer>().GetDifferences(emptyModel, tableOnlyModel), baseCtx);

        // Insert rows BEFORE any projection exists so every (re)materialize must backfill existing parts.
        await ExecuteRawAsync("INSERT INTO proj_change_events VALUES (1, 'a', 10), (2, 'a', 5), (3, 'b', 7)");

        // v1 projection: sum(Amount) per Category.
        await using var v1Ctx = CreateContext(b =>
        {
            mapTable(b);
            b.Entity<ProjEvent>().HasProjection("proj_agg")
                .FromRaw("SELECT Category, sum(Amount) GROUP BY Category");
        });
        var v1Model = v1Ctx.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        await ApplyOperationsAsync(
            v1Ctx.GetService<IMigrationsModelDiffer>().GetDifferences(tableOnlyModel, v1Model), v1Ctx);

        var v1Sql = await QueryScalarRaw(
            "SELECT create_table_query FROM system.tables WHERE database = currentDatabase() AND name = 'proj_change_events'");
        Assert.Contains("sum(", v1Sql!, StringComparison.OrdinalIgnoreCase);

        // v2: same projection name, different body → differ must emit DROP then ADD (no ALTER PROJECTION in ClickHouse).
        await using var v2Ctx = CreateContext(b =>
        {
            mapTable(b);
            b.Entity<ProjEvent>().HasProjection("proj_agg")
                .FromRaw("SELECT Category, max(Amount) GROUP BY Category");
        });
        var v2Model = v2Ctx.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var ops = v2Ctx.GetService<IMigrationsModelDiffer>().GetDifferences(v1Model, v2Model).ToList();

        var drop = Assert.Single(ops.OfType<ClickHouseDropProjectionOperation>());
        var add = Assert.Single(ops.OfType<ClickHouseAddProjectionOperation>());
        Assert.Equal("proj_agg", drop.ProjectionName);
        Assert.Equal("proj_agg", add.ProjectionName);
        // DROP must precede ADD, otherwise ClickHouse rejects the duplicate projection name.
        Assert.True(ops.IndexOf(drop) < ops.IndexOf(add), "DROP PROJECTION must be ordered before ADD PROJECTION");

        // Apply the replacement against a table that already has a materialized projection over existing parts.
        await ApplyOperationsAsync(ops, v2Ctx);

        // The new definition replaced the old one…
        var v2Sql = await QueryScalarRaw(
            "SELECT create_table_query FROM system.tables WHERE database = currentDatabase() AND name = 'proj_change_events'");
        Assert.Contains("max(", v2Sql!, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("sum(", v2Sql!, StringComparison.OrdinalIgnoreCase);

        // …and the rebuilt projection backfilled the pre-existing part (active projection parts exist).
        var activeParts = await QueryScalarRaw(
            "SELECT count() FROM system.projection_parts "
            + "WHERE database = currentDatabase() AND table = 'proj_change_events' AND name = 'proj_agg' AND active");
        Assert.NotEqual("0", activeParts);

        // Results are correct through the new projection (category 'a' → max(10, 5) = 10).
        var maxA = await QueryScalarRaw("SELECT max(Amount) FROM proj_change_events WHERE Category = 'a'");
        Assert.Equal("10", maxA);

        await ExecuteRawAsync("DROP TABLE IF EXISTS proj_change_events");
    }

    public class ProjEvent
    {
        public long Id { get; set; }
        public string Category { get; set; } = "";
        public long Amount { get; set; }
    }

    public class MvSource
    {
        public long Id { get; set; }
        public long Value { get; set; }
    }

    public class MvTarget
    {
        public long Bucket { get; set; }
        public long Total { get; set; }
    }

    public class JoinLeft
    {
        public long Id { get; set; }
        public long Value { get; set; }
    }

    public class JoinRight
    {
        public long Id { get; set; }
        public long Factor { get; set; }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private TestContext CreateContext(Action<ModelBuilder> configure)
    {
        var options = new DbContextOptionsBuilder()
            .UseClickHouse(_connectionString)
            .EnableServiceProviderCaching(false)
            .Options;
        return new TestContext(options, configure);
    }

    private async Task ApplyMigrationAsync(params MigrationOperation[] operations)
    {
        var optionsBuilder = new DbContextOptionsBuilder()
            .UseClickHouse(_connectionString)
            .EnableServiceProviderCaching(false);
        await using var context = new DbContext(optionsBuilder.Options);
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var commands = generator.Generate(operations);

        using var conn = new global::ClickHouse.Driver.ADO.ClickHouseConnection(_connectionString);
        await conn.OpenAsync();
        foreach (var command in commands)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = command.CommandText;
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private async Task ApplyOperationsAsync(IReadOnlyList<MigrationOperation> operations, DbContext context)
    {
        var generator = context.GetService<IMigrationsSqlGenerator>();
        using var conn = new global::ClickHouse.Driver.ADO.ClickHouseConnection(_connectionString);
        await conn.OpenAsync();
        foreach (var command in generator.Generate(operations))
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = command.CommandText;
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private async Task ExecuteRawAsync(params string[] statements)
    {
        using var conn = new global::ClickHouse.Driver.ADO.ClickHouseConnection(_connectionString);
        await conn.OpenAsync();
        foreach (var sql in statements)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private async Task<string?> QueryScalarRaw(string sql)
    {
        using var conn = new global::ClickHouse.Driver.ADO.ClickHouseConnection(_connectionString);
        await conn.OpenAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = await cmd.ExecuteScalarAsync();
        return result?.ToString();
    }

    private static async Task<string?> QueryScalar(DbContext context, string sql)
    {
        var conn = context.Database.GetDbConnection();
        await conn.OpenAsync();
        try
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            var result = await cmd.ExecuteScalarAsync();
            return result?.ToString();
        }
        finally
        {
            await conn.CloseAsync();
        }
    }

    // ── Entity types ────────────────────────────────────────────────────────

    private class TestContext : DbContext
    {
        private readonly Action<ModelBuilder> _configure;
        public TestContext(DbContextOptions options, Action<ModelBuilder> configure) : base(options) => _configure = configure;
        protected override void OnModelCreating(ModelBuilder modelBuilder) => _configure(modelBuilder);
    }

    public class IdEntity
    {
        public long Id { get; set; }
    }

    public class IdValueEntity
    {
        public long Id { get; set; }
        public string Value { get; set; } = string.Empty;
    }

    public class TimestampEntity
    {
        public long Id { get; set; }
        public DateTime Timestamp { get; set; }
    }

    public class ListEntity
    {
        public long Id { get; set; }
        public List<string>? Tags { get; set; }
    }

    public class MapEntity
    {
        public long Id { get; set; }
        public Dictionary<string, string>? Meta { get; set; }
    }

    public class VersionedDeleteEntity
    {
        public long Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public ulong Version { get; set; }
        public byte IsDeleted { get; set; }
    }

    public class EventEntity
    {
        public long Id { get; set; }
        public long UserId { get; set; }
        public string Region { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; }
    }

    public class FullFeaturedEntity
    {
        public long Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; }
    }
}
