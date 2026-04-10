using ClickHouse.EntityFrameworkCore.Extensions;
using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.ClickHouse.Tests;

public class EnsureCreatedTests : IAsyncLifetime
{
    private string _connectionString = default!;

    public async Task InitializeAsync()
    {
        _connectionString = await SharedContainer.GetConnectionStringAsync();
    }

    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task EnsureCreated_MergeTree_creates_table()
    {
        await using var context = CreateContext(b =>
        {
            b.Entity<SimpleEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("mt_test", t => t
                    .HasMergeTreeEngine()
                    .WithOrderBy("Id"));
            });
        });

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Verify table exists and has correct engine via system.tables
        var engine = await QueryScalar(context, "SELECT engine FROM system.tables WHERE name = 'mt_test'");
        Assert.Equal("MergeTree", engine);
    }

    [Fact]
    public async Task EnsureCreated_ReplacingMergeTree_creates_table()
    {
        await using var context = CreateContext(b =>
        {
            b.Entity<VersionedEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("rmt_test", t => t
                    .HasReplacingMergeTreeEngine("Version")
                    .WithOrderBy("Id"));
            });
        });

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var engine = await QueryScalar(context, "SELECT engine FROM system.tables WHERE name = 'rmt_test'");
        Assert.Equal("ReplacingMergeTree", engine);
    }

    [Fact]
    public async Task EnsureCreated_default_engine_uses_MergeTree()
    {
        await using var context = CreateContext(b =>
        {
            b.Entity<SimpleEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("default_engine_test");
            });
        });

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var engine = await QueryScalar(context, "SELECT engine FROM system.tables WHERE name = 'default_engine_test'");
        Assert.Equal("MergeTree", engine);
    }

    [Fact]
    public async Task EnsureCreated_with_partitionBy_and_settings()
    {
        await using var context = CreateContext(b =>
        {
            b.Entity<TimestampedEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("partition_test", t => t
                    .HasMergeTreeEngine()
                    .WithOrderBy("Id")
                    .WithPartitionBy("toYYYYMM(Timestamp)")
                    .WithSetting("index_granularity", "4096"));
            });
        });

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var engine = await QueryScalar(context, "SELECT engine FROM system.tables WHERE name = 'partition_test'");
        Assert.Equal("MergeTree", engine);
    }

    [Fact]
    public async Task EnsureCreated_insert_and_query_roundtrip()
    {
        await using var context = CreateContext(b =>
        {
            b.Entity<SimpleEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("roundtrip_test", t => t
                    .HasMergeTreeEngine()
                    .WithOrderBy("Id"));
            });
        });

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        context.Set<SimpleEntity>().Add(new SimpleEntity { Id = 1, Name = "test" });
        await context.SaveChangesAsync();

        await using var readContext = CreateContext(b =>
        {
            b.Entity<SimpleEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("roundtrip_test", t => t
                    .HasMergeTreeEngine()
                    .WithOrderBy("Id"));
            });
        });

        var entity = await readContext.Set<SimpleEntity>().FirstAsync(e => e.Id == 1);
        Assert.Equal("test", entity.Name);
    }

    [Fact]
    public async Task EnsureCreated_with_codec_column()
    {
        await using var context = CreateContext(b =>
        {
            b.Entity<SimpleEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.Property(x => x.Id).HasCodec("Delta, ZSTD");
                e.ToTable("codec_test", t => t
                    .HasMergeTreeEngine()
                    .WithOrderBy("Id"));
            });
        });

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Table creation succeeds means codec was accepted
        var engine = await QueryScalar(context, "SELECT engine FROM system.tables WHERE name = 'codec_test'");
        Assert.Equal("MergeTree", engine);
    }

    [Fact]
    public async Task EnsureDeleted_drops_database()
    {
        await using var context = CreateContext(b =>
        {
            b.Entity<SimpleEntity>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("del_test", t => t
                    .HasMergeTreeEngine()
                    .WithOrderBy("Id"));
            });
        });

        await context.Database.EnsureCreatedAsync();
        Assert.True(await context.Database.EnsureCreatedAsync() is false); // already exists

        await context.Database.EnsureDeletedAsync();
        // After delete, creating again should return true
        Assert.True(await context.Database.EnsureCreatedAsync());
    }

    private TestContext CreateContext(Action<ModelBuilder> configure)
    {
        var options = new DbContextOptionsBuilder()
            .UseClickHouse(_connectionString)
            .EnableServiceProviderCaching(false)
            .Options;
        return new TestContext(options, configure);
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

    private class TestContext : DbContext
    {
        private readonly Action<ModelBuilder> _configure;

        public TestContext(DbContextOptions options, Action<ModelBuilder> configure)
            : base(options)
        {
            _configure = configure;
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder) => _configure(modelBuilder);
    }

    public class SimpleEntity
    {
        public long Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public class VersionedEntity
    {
        public long Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public ulong Version { get; set; }
    }

    public class TimestampedEntity
    {
        public long Id { get; set; }
        public DateTime Timestamp { get; set; }
    }
}
