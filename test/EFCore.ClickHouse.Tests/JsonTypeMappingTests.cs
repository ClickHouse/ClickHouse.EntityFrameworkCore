using System.Text.Json.Nodes;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EFCore.ClickHouse.Tests;

#region Entities

public class JsonNodeEntity
{
    public long Id { get; set; }
    public JsonNode? Data { get; set; }
}

public class JsonStringEntity
{
    public long Id { get; set; }
    public string? Data { get; set; }
}

#endregion

#region DbContexts

public class JsonNodeDbContext : DbContext
{
    public DbSet<JsonNodeEntity> Entities => Set<JsonNodeEntity>();
    private readonly string _connectionString;
    public JsonNodeDbContext(string cs) => _connectionString = cs;
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseClickHouse(_connectionString);
    protected override void OnModelCreating(ModelBuilder m)
    {
        m.Entity<JsonNodeEntity>(e =>
        {
            e.ToTable("json_node_test");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.Data).HasColumnName("data").HasColumnType("Json");
        });
    }
}

public class JsonStringDbContext : DbContext
{
    public DbSet<JsonStringEntity> Entities => Set<JsonStringEntity>();
    private readonly string _connectionString;
    public JsonStringDbContext(string cs) => _connectionString = cs;
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseClickHouse(_connectionString);
    protected override void OnModelCreating(ModelBuilder m)
    {
        m.Entity<JsonStringEntity>(e =>
        {
            e.ToTable("json_node_test");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.Data).HasColumnName("data").HasColumnType("Json");
        });
    }
}

#endregion

#region Fixture

public class JsonTypesFixture : IAsyncLifetime
{
    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        ConnectionString = await SharedContainer.GetConnectionStringAsync();

        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(ConnectionString);
        await connection.OpenAsync();

        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "SET allow_experimental_json_type = 1";
            await cmd.ExecuteNonQueryAsync();
        }

        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE json_node_test (
                    id Int64,
                    data Json
                ) ENGINE = MergeTree() ORDER BY id
                SETTINGS allow_experimental_json_type = 1
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                INSERT INTO json_node_test VALUES
                (1, '{"name": "Alice", "age": 30}'),
                (2, '{"name": "Bob", "tags": ["a", "b"]}')
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        // NULL must be inserted separately to avoid coercion
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO json_node_test VALUES (3, NULL)";
            await cmd.ExecuteNonQueryAsync();
        }

        // Table for insert round-trip tests
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE json_insert_test (
                    id Int64,
                    data Json
                ) ENGINE = MergeTree() ORDER BY id
                SETTINGS allow_experimental_json_type = 1
                """;
            await cmd.ExecuteNonQueryAsync();
        }
    }

    public Task DisposeAsync() => Task.CompletedTask;
}

#endregion

#region Collection Fixture

[CollectionDefinition("JsonTypes")]
public class JsonTypesCollection : ICollectionFixture<JsonTypesFixture>;

#endregion

#region Integration Tests

[Collection("JsonTypes")]
public class JsonNodeReadTests
{
    private readonly JsonTypesFixture _fixture;
    public JsonNodeReadTests(JsonTypesFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task ReadAll_JsonNode_RoundTrip()
    {
        await using var ctx = new JsonNodeDbContext(_fixture.ConnectionString);
        var rows = await ctx.Entities.OrderBy(e => e.Id).AsNoTracking().ToListAsync();

        Assert.Equal(3, rows.Count);

        // Row 1: structured JSON
        Assert.NotNull(rows[0].Data);
        Assert.Equal("Alice", rows[0].Data!["name"]?.GetValue<string>());
        // ClickHouse JSON stores integers as Int64
        Assert.Equal(30L, rows[0].Data!["age"]?.GetValue<long>());

        // Row 2: JSON with array
        Assert.NotNull(rows[1].Data);
        Assert.Equal("Bob", rows[1].Data!["name"]?.GetValue<string>());

        // Row 3: NULL → ClickHouse JSON returns empty object '{}' for NULL
        Assert.NotNull(rows[2].Data);
    }

    [Fact]
    public async Task Where_JsonTable_FilterById()
    {
        await using var ctx = new JsonNodeDbContext(_fixture.ConnectionString);
        var result = await ctx.Entities
            .Where(e => e.Id == 1)
            .AsNoTracking().SingleAsync();

        Assert.NotNull(result.Data);
        Assert.Equal("Alice", result.Data!["name"]?.GetValue<string>());
    }
}

[Collection("JsonTypes")]
public class JsonStringReadTests
{
    private readonly JsonTypesFixture _fixture;
    public JsonStringReadTests(JsonTypesFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task ReadAll_JsonString_RoundTrip()
    {
        await using var ctx = new JsonStringDbContext(_fixture.ConnectionString);
        var rows = await ctx.Entities.OrderBy(e => e.Id).AsNoTracking().ToListAsync();

        Assert.Equal(3, rows.Count);

        // Row 1: verify it's valid JSON containing expected data
        Assert.NotNull(rows[0].Data);
        var parsed = JsonNode.Parse(rows[0].Data!);
        Assert.Equal("Alice", parsed!["name"]?.GetValue<string>());

        // Row 3: ClickHouse JSON returns empty object '{}' for NULL
        Assert.NotNull(rows[2].Data);
    }
}

[Collection("JsonTypes")]
public class JsonInsertTests
{
    private readonly JsonTypesFixture _fixture;
    public JsonInsertTests(JsonTypesFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task Insert_JsonNode_RoundTrip()
    {
        // Use a dedicated context pointing at insert table
        var cs = _fixture.ConnectionString;
        await using var insertCtx = new JsonInsertDbContext(cs);

        var entity = new JsonInsertEntity
        {
            Id = 100,
            Data = JsonNode.Parse("""{"key": "value", "num": 42}""")
        };

        insertCtx.Entities.Add(entity);
        await insertCtx.SaveChangesAsync();

        // Read back
        await using var readCtx = new JsonInsertDbContext(cs);
        var result = await readCtx.Entities
            .Where(e => e.Id == 100)
            .AsNoTracking().SingleAsync();

        Assert.NotNull(result.Data);
        Assert.Equal("value", result.Data!["key"]?.GetValue<string>());
        Assert.Equal(42L, result.Data!["num"]?.GetValue<long>());
    }

    [Fact]
    public async Task Insert_NullJson_RoundTrip()
    {
        var cs = _fixture.ConnectionString;
        await using var insertCtx = new JsonInsertDbContext(cs);

        var entity = new JsonInsertEntity { Id = 101, Data = null };
        insertCtx.Entities.Add(entity);
        await insertCtx.SaveChangesAsync();

        await using var readCtx = new JsonInsertDbContext(cs);
        var result = await readCtx.Entities
            .Where(e => e.Id == 101)
            .AsNoTracking().SingleAsync();

        // ClickHouse JSON type returns empty object '{}' for NULL, not SQL NULL
        Assert.NotNull(result.Data);
    }
}

public class JsonInsertEntity
{
    public long Id { get; set; }
    public JsonNode? Data { get; set; }
}

public class JsonInsertDbContext : DbContext
{
    public DbSet<JsonInsertEntity> Entities => Set<JsonInsertEntity>();
    private readonly string _connectionString;
    public JsonInsertDbContext(string cs) => _connectionString = cs;
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseClickHouse(_connectionString);
    protected override void OnModelCreating(ModelBuilder m)
    {
        m.Entity<JsonInsertEntity>(e =>
        {
            e.ToTable("json_insert_test");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.Data).HasColumnName("data").HasColumnType("Json");
        });
    }
}

#endregion

#region Unit Tests

public class JsonTypeMappingUnitTests
{
    [Fact]
    public void SqlLiteral_SimpleJson()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(JsonNode))!;
        var node = JsonNode.Parse("""{"name":"Alice"}""")!;
        var literal = mapping.GenerateSqlLiteral(node);
        Assert.Equal("""'{"name":"Alice"}'""", literal);
    }

    [Fact]
    public void SqlLiteral_JsonWithBackslash()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(JsonNode))!;
        // JSON with a value containing a literal backslash
        var node = JsonNode.Parse("""{"path":"C:\\Users"}""")!;
        var literal = mapping.GenerateSqlLiteral(node);
        // ToJsonString() produces: {"path":"C\\Users"}
        // Our escaping doubles backslashes for ClickHouse
        Assert.Contains("\\\\", literal);
        Assert.StartsWith("'", literal);
        Assert.EndsWith("'", literal);
    }

    [Fact]
    public void SqlLiteral_NullJson_ReturnsNULL()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(JsonNode))!;
        var literal = mapping.GenerateSqlLiteral(null);
        Assert.Equal("NULL", literal);
    }

    [Fact]
    public void FindMapping_JsonNode_FromClrType()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(JsonNode));
        Assert.NotNull(mapping);
        Assert.Equal("Json", mapping.StoreType);
        Assert.Equal(typeof(JsonNode), mapping.ClrType);
    }

    [Fact]
    public void FindMapping_String_WithJsonStoreType_Resolves()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(string), "Json");
        Assert.NotNull(mapping);
        Assert.Equal("Json", mapping.StoreType);
        Assert.Equal(typeof(string), mapping.ClrType);
        // No ValueConverter — driver accepts string directly for writes,
        // and CustomizeDataReaderExpression handles the read-side conversion.
        Assert.Null(mapping.Converter);
    }

    [Fact]
    public void FindMapping_Object_WithJsonStoreType_Resolves()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(object), "Json");
        Assert.NotNull(mapping);
        Assert.Equal("Json", mapping.StoreType);
    }

    [Fact]
    public void FindMapping_JsonWithTypeHints_Resolves()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(object), "Json(name String, age Int32)");
        Assert.NotNull(mapping);
        Assert.Equal("Json", mapping.StoreType);
    }

    [Fact]
    public void FindMapping_String_WithoutJsonStoreType_ResolvesToString()
    {
        var source = GetTypeMappingSource();
        var mapping = source.FindMapping(typeof(string));
        Assert.NotNull(mapping);
        Assert.Equal("String", mapping.StoreType);
        Assert.Null(mapping.Converter);
    }

    private static Microsoft.EntityFrameworkCore.Storage.IRelationalTypeMappingSource GetTypeMappingSource()
    {
        var builder = new DbContextOptionsBuilder();
        builder.UseClickHouse("Host=localhost;Protocol=http");
        using var ctx = new DbContext(builder.Options);
        return ((IInfrastructure<IServiceProvider>)ctx).Instance
            .GetRequiredService<Microsoft.EntityFrameworkCore.Storage.IRelationalTypeMappingSource>();
    }
}

#endregion
