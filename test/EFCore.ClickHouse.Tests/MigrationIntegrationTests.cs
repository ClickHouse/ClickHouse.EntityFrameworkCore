using ClickHouse.EntityFrameworkCore.Extensions;
using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using System.Text.RegularExpressions;
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
        _databaseName = Regex.Match(_connectionString, @"Database=([^;]+)").Groups[1].Value;
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
}
