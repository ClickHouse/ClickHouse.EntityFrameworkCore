using System.Text.Json.Nodes;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.ClickHouse.Tests;

public class JsonNodeTranslationEntity
{
    public long Id { get; set; }
    public JsonNode? Data { get; set; }
}

public class JsonNodeTranslationContext : DbContext
{
    public DbSet<JsonNodeTranslationEntity> Entities => Set<JsonNodeTranslationEntity>();
    private readonly string _connectionString;
    public JsonNodeTranslationContext(string cs) => _connectionString = cs;
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseClickHouse(_connectionString);
    protected override void OnModelCreating(ModelBuilder m)
    {
        m.Entity<JsonNodeTranslationEntity>(e =>
        {
            e.ToTable("json_node_translation_test");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.Data).HasColumnName("data").HasColumnType("Json");
        });
    }
}

public class JsonNodeTranslationFixture : IAsyncLifetime
{
    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        ConnectionString = await SharedContainer.GetConnectionStringAsync();

        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(ConnectionString);
        await connection.OpenAsync();

        using var createCmd = connection.CreateCommand();
        createCmd.CommandText = """
                                CREATE TABLE json_node_translation_test (
                                    id Int64,
                                    data Json
                                ) ENGINE = MergeTree()
                                ORDER BY id
                                """;
        await createCmd.ExecuteNonQueryAsync();

        using var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = """
                                INSERT INTO json_node_translation_test (id, data) VALUES
                                (1, '{
                                    "username": "alice_dev", 
                                    "age": 30, 
                                    "roles": ["admin", "developer"], 
                                    "address": {"city": "Saint Petersburg", "street": "Nevsky Prospekt", "building": 10}
                                }'),
                                (2, '{
                                    "username": "bob_manager", 
                                    "age": 25, 
                                    "roles": ["manager"], 
                                    "address": {"city": "Moscow", "street": "Arbat"},
                                    "orders": [
                                        {"position": "notepad", "price": 100}
                                    ]
                                }'),
                                (3, '{
                                    "username": "charlie_guest", 
                                    "age": 35, 
                                    "roles": [], 
                                    "types": {"s": "Test", "i": "10", "l": -100, "u": 100, "f": 0.5, "d": 15.55, "b": true}
                                }'),
                                (4, NULL),
                                (5, '{
                                    "username": "system_bot", 
                                    "meta": {
                                        "runtime": {"version": "10.0", "env": "production", "metrics": {"cpu_limit": 4, "memory_gb": 16}}
                                    }, 
                                    "points": [[10, 20], [30, 40]]
                                }');
                                """;
        await insertCmd.ExecuteNonQueryAsync();
    }

    public Task DisposeAsync() => Task.CompletedTask;
}

public class JsonNodeTranslationTests : IClassFixture<JsonNodeTranslationFixture>
{
    private readonly JsonNodeTranslationFixture _fixture;

    public JsonNodeTranslationTests(JsonNodeTranslationFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task JsonNode_DeepNesting_TranslatesCorrectly()
    {
        await using var context = new JsonNodeTranslationContext(_fixture.ConnectionString);

        var cpuLimit = await context.Entities
            .Where(x => x.Id == 5)
            .Select(e => e.Data["meta"]["runtime"]["metrics"]["cpu_limit"].GetValue<int>())
            .FirstOrDefaultAsync();

        Assert.Equal(4, cpuLimit);
    }

    [Fact]
    public async Task JsonNode_MixedAccess_TranslatesCorrectly()
    {
        await using var context = new JsonNodeTranslationContext(_fixture.ConnectionString);

        var price = await context.Entities
            .Where(x => x.Id == 2)
            .Select(e => e.Data["orders"][0]["price"].GetValue<double>())
            .FirstOrDefaultAsync();

        Assert.Equal(100d, price);
    }

    [Fact]
    public async Task JsonNode_ObjectProjection_Works()
    {
        await using var context = new JsonNodeTranslationContext(_fixture.ConnectionString);

        var order = await context.Entities
            .Where(x => x.Id == 2)
            .Select(e => e.Data["orders"][0])
            .FirstOrDefaultAsync();

        Assert.NotNull(order);
        Assert.Equal(100, (long)order["price"]!);
        Assert.Equal("notepad", (string)order["position"]!);
    }

    [Fact]
    public async Task JsonNode_MatrixAccess_TranslatesCorrectly()
    {
        await using var context = new JsonNodeTranslationContext(_fixture.ConnectionString);

        var result = await context.Entities
            .Where(e => e.Id == 5)
            .Select(e => new
            {
                Point = e.Data["points"][0][0].GetValue<int>(),
                Points = e.Data["points"][0].GetValue<int[]>(),
                Matrix = e.Data["points"].GetValue<int[][]>(),
            })
            .SingleAsync();

        Assert.Equal(10, result.Point);
        Assert.Equal([10, 20], result.Points);
        Assert.Equal([[10, 20], [30, 40]], result.Matrix);
    }

    [Fact]
    public async Task JsonNode_VariousTypes_CastsCorrectly()
    {
        await using var context = new JsonNodeTranslationContext(_fixture.ConnectionString);

        var result = await context.Entities
            .Where(x => x.Id == 3)
            .Select(e => new
            {
                String = e.Data["types"]["s"].GetValue<string>(),
                Int = (int)e.Data["types"]["i"],
                Long = e.Data["types"]["l"].GetValue<long>(),
                ULong = (ulong)e.Data["types"]["u"],
                Float = e.Data["types"]["f"].GetValue<float>(),
                Double = (double)e.Data["types"]["d"],
                Bool = e.Data["types"]["b"].GetValue<bool>(),
            }).FirstOrDefaultAsync();

        Assert.NotNull(result);
        Assert.Equal("Test", result.String);
        Assert.Equal(10, result.Int);
        Assert.Equal(-100L, result.Long);
        Assert.Equal(100UL, result.ULong);
        Assert.Equal(0.5f, result.Float);
        Assert.Equal(15.55d, result.Double);
        Assert.Equal(true, result.Bool);
    }

    [Fact]
    public async Task JsonNode_MissingKey_ReturnsDefault()
    {
        await using var context = new JsonNodeTranslationContext(_fixture.ConnectionString);

        var result = await context.Entities
            .Select(e => new
            {
                String = e.Data["non_existent"].GetValue<string>(),
                Int = (int)e.Data["non_existent"],
                Long = e.Data["non_existent"].GetValue<long>(),
                ULong = e.Data["non_existent"].GetValue<ulong>(),
                Float = e.Data["non_existent"].GetValue<float>(),
                Double = e.Data["non_existent"].GetValue<double>(),
                Bool = e.Data["non_existent"].GetValue<bool>(),
            })
            .FirstOrDefaultAsync();

        Assert.Equal(string.Empty, result.String);
        Assert.Equal(0, result.Int);
        Assert.Equal(0L, result.Long);
        Assert.Equal(0UL, result.ULong);
        Assert.Equal(0f, result.Float);
        Assert.Equal(0d, result.Double);
        Assert.Equal(false, result.Bool);
    }

    [Fact]
    public async Task JsonNode_InWhereClause_Works()
    {
        await using var context = new JsonNodeTranslationContext(_fixture.ConnectionString);

        var count = await context.Entities
            .Where(e => (int)e.Data["age"] > 20 && e.Data["username"].GetValue<string>() == "alice_dev")
            .CountAsync();

        Assert.Equal(1, count);
    }
}