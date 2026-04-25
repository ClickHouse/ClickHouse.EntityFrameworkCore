using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EFCore.ClickHouse.Tests;

public class JsonEntity
{
    public long Id { get; set; }
    public string Data { get; set; } = string.Empty;
}

public class JsonDbContext : DbContext
{
    public DbSet<JsonEntity> JsonEntities => Set<JsonEntity>();
    
    private readonly string _connectionString;

    public JsonDbContext(string connectionString)
    {
        _connectionString = connectionString;
    }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseClickHouse(_connectionString);
    }
    
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<JsonEntity>(entity =>
        {
            entity.ToTable("json_dbfunctions_test");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.Data).HasColumnName("data");
        });
    }
}

public class JsonFixture : IAsyncLifetime
{
    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        ConnectionString = await SharedContainer.GetConnectionStringAsync();

        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(ConnectionString);
        await connection.OpenAsync();
        
        using var createCmd = connection.CreateCommand();
        createCmd.CommandText = """
                                CREATE TABLE json_dbfunctions_test (
                                    id Int64,
                                    data String
                                ) ENGINE = MergeTree()
                                ORDER BY id
                                """;
        await createCmd.ExecuteNonQueryAsync();

        using var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = """
                                INSERT INTO json_dbfunctions_test (id, data) VALUES
                                (1, '{"name":"Alice","age":30,"is_admin":true,"height":1.75,"extra":{"active":true}}'),
                                (2, '{"name":"Bob","age":25,"is_admin":false,"height":1.80}'),
                                (3, '{"note":"quote\"inside"}')
                                (4, NULL)
                                """;
        await insertCmd.ExecuteNonQueryAsync();
    }

    public Task DisposeAsync() => Task.CompletedTask;
}

public class JsonDbFunctionsTranslationTest : IClassFixture<JsonFixture>
{
    private readonly JsonFixture _fixture;

    public JsonDbFunctionsTranslationTest(JsonFixture fixture)
    {
        _fixture = fixture;
    }
    
    [Fact]
    public async Task DbFunctions_SimpleJsonExtractBool_TranslatesCorrectly()
    {
        await using var context = new JsonDbContext(_fixture.ConnectionString);

        var isAdmin = await context.JsonEntities
            .Where(e => e.Id == 2)
            .Select(e => EF.Functions.SimpleJsonExtractBool(e.Data, "is_admin"))
            .SingleAsync();

        Assert.False(isAdmin);
    }
    
    [Fact]
    public async Task DbFunctions_SimpleJsonExtractFloat_TranslatesCorrectly()
    {
        await using var context = new JsonDbContext(_fixture.ConnectionString);

        var height = await context.JsonEntities
            .Where(e => e.Id == 1)
            .Select(e => EF.Functions.SimpleJsonExtractFloat(e.Data, "height"))
            .SingleAsync();

        Assert.Equal(1.75, height, 2);
    }
    
    [Fact]
    public async Task DbFunctions_SimpleJsonExtractInt_TranslatesCorrectly()
    {
        await using var context = new JsonDbContext(_fixture.ConnectionString);

        var age = await context.JsonEntities
            .Where(e => e.Id == 1)
            .Select(e => EF.Functions.SimpleJsonExtractInt(e.Data, "age"))
            .SingleAsync();

        Assert.Equal(30, age);
    }
    
    [Fact]
    public async Task DbFunctions_SimpleJsonExtractRaw_TranslatesCorrectly()
    {
        await using var context = new JsonDbContext(_fixture.ConnectionString);

        var result = await context.JsonEntities
            .Where(x => x.Id == 1)
            .Select(e => new { Extra = EF.Functions.SimpleJsonExtractRaw(e.Data, "extra") })
            .FirstOrDefaultAsync();

        Assert.NotNull(result);
        Assert.Equal("{\"active\":true}", result.Extra);
    }
    
    [Fact]
    public async Task DbFunctions_SimpleJsonExtractString_TranslatesCorrectly()
    {
        await using var context = new JsonDbContext(_fixture.ConnectionString);

        var result = await context.JsonEntities
            .Select(e => new { Name = EF.Functions.SimpleJsonExtractString(e.Data, "name") })
            .Where(x => x.Name == "Alice")
            .FirstOrDefaultAsync();

        Assert.NotNull(result);
        Assert.Equal("Alice", result.Name);
    }
    
    [Fact]
    public async Task DbFunctions_SimpleJsonExtractUInt_TranslatesCorrectly()
    {
        await using var context = new JsonDbContext(_fixture.ConnectionString);

        var age = await context.JsonEntities
            .Where(e => e.Id == 1)
            .Select(e => EF.Functions.SimpleJsonExtractUInt(e.Data, "age"))
            .SingleAsync();

        Assert.Equal(30u, age);
    }
    
    [Fact]
    public async Task DbFunctions_SimpleJsonHas_TranslatesCorrectly()
    {
        await using var context = new JsonDbContext(_fixture.ConnectionString);

        var hasAdminField = await context.JsonEntities
            .Where(e => e.Id == 1)
            .Select(e => EF.Functions.SimpleJsonHas(e.Data, "is_admin"))
            .SingleAsync();

        Assert.True(hasAdminField);
    }
    
    [Fact]
    public async Task DbFunctions_SimpleJsonExtract_HandlesNullColumn()
    {
        await using var context = new JsonDbContext(_fixture.ConnectionString);
        
        var result = await context.JsonEntities
            .Where(e => e.Id == 4)
            .Select(e => new 
            {
                ExtractedString = EF.Functions.SimpleJsonExtractString(e.Data, "any_key"),
                ExtractedInt = EF.Functions.SimpleJsonExtractInt(e.Data, "any_key")
            })
            .SingleAsync();
        
        Assert.Equal(string.Empty, result.ExtractedString);
        Assert.Equal(0, result.ExtractedInt);
    }
    
    [Fact]
    public async Task DbFunctions_SimpleJsonExtract_ReturnsDefault_WhenKeyNotFound()
    {
        await using var context = new JsonDbContext(_fixture.ConnectionString);

        var result = await context.JsonEntities
            .Where(e => e.Id == 1)
            .Select(e => new 
            {
                NonExistentString = EF.Functions.SimpleJsonExtractString(e.Data, "unknown_key"),
                NonExistentInt = EF.Functions.SimpleJsonExtractInt(e.Data, "unknown_key")
            })
            .SingleAsync();

        Assert.Equal(string.Empty, result.NonExistentString);
        Assert.Equal(0, result.NonExistentInt);
    }
    
    [Fact]
    public async Task DbFunctions_SimpleJsonExtractRaw_CanBeChained()
    {
        await using var context = new JsonDbContext(_fixture.ConnectionString);
        
        var result = await context.JsonEntities
            .Where(e => e.Id == 1)
            .Select(e => EF.Functions.SimpleJsonExtractRaw(e.Data, "extra"))
            .Select(raw => EF.Functions.SimpleJsonExtractBool(raw, "active"))
            .SingleAsync();

        Assert.True(result);
    }
    
    [Fact]
    public async Task DbFunctions_SimpleJson_WorksInWhereClause()
    {
        await using var context = new JsonDbContext(_fixture.ConnectionString);

        var users = await context.JsonEntities
            .Where(e => EF.Functions.SimpleJsonExtractInt(e.Data, "age") > 28)
            .ToListAsync();

        Assert.Single(users);
    }
}