using ClickHouse.EntityFrameworkCore.Design.Internal;
using ClickHouse.EntityFrameworkCore.Migrations;
using ClickHouse.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Migrations.Design;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EFCore.ClickHouse.Tests;

/// <summary>
/// Verifies that <c>ClickHouseCSharpMigrationOperationGenerator</c> renders this provider's custom
/// migration operations into valid C#. Drives the real design-time code-generation pipeline
/// (the same one <c>dotnet ef migrations add</c> uses) — no database required.
/// </summary>
public class MigrationOperationCodeGenTests
{
    private static string Generate(params MigrationOperation[] upOperations)
    {
        var services = new ServiceCollection();
        services.AddEntityFrameworkDesignTimeServices();
        new ClickHouseDesignTimeServices().ConfigureDesignTimeServices(services);

        using var provider = services.BuildServiceProvider();
        var generator = provider.GetRequiredService<IMigrationsCodeGeneratorSelector>().Select(language: null);
        return generator.GenerateMigration("TestNs", "TestMigration", upOperations, []);
    }

    [Fact]
    public void CreateMaterializedView_minimal_emits_required_args_only()
    {
        var code = Generate(new ClickHouseCreateMaterializedViewOperation
        {
            ViewName = "hits_mv",
            TargetTable = "hits_by_hour",
            SelectQuery = "SELECT Id AS Bucket, Value AS Hits FROM hits_source",
        });

        Assert.Contains("migrationBuilder.CreateClickHouseMaterializedView(", code);
        Assert.Contains("viewName: \"hits_mv\"", code);
        Assert.Contains("targetTable: \"hits_by_hour\"", code);
        Assert.Contains("selectQuery: \"SELECT Id AS Bucket, Value AS Hits FROM hits_source\"", code);
        // Optional args at their defaults are omitted.
        Assert.DoesNotContain("database:", code);
        Assert.DoesNotContain("cluster:", code);
        Assert.DoesNotContain("populate:", code);
        Assert.DoesNotContain("ifNotExists:", code);
    }

    [Fact]
    public void CreateMaterializedView_full_emits_all_non_default_args()
    {
        var code = Generate(new ClickHouseCreateMaterializedViewOperation
        {
            ViewName = "mv",
            TargetTable = "tgt",
            SelectQuery = "SELECT * FROM src",
            Database = "viewdb",
            TargetDatabase = "tgtdb",
            Cluster = "prod",
            IfNotExists = true,
            Populate = true,
        });

        Assert.Contains("database: \"viewdb\"", code);
        Assert.Contains("targetDatabase: \"tgtdb\"", code);
        Assert.Contains("cluster: \"prod\"", code);
        Assert.Contains("ifNotExists: true", code);
        Assert.Contains("populate: true", code);
    }

    [Fact]
    public void DropMaterializedView_emits_view_name_and_options()
    {
        var code = Generate(new ClickHouseDropMaterializedViewOperation
        {
            ViewName = "mv",
            Database = "viewdb",
            Cluster = "prod",
            IfExists = true,
        });

        Assert.Contains("migrationBuilder.DropClickHouseMaterializedView(", code);
        Assert.Contains("viewName: \"mv\"", code);
        Assert.Contains("database: \"viewdb\"", code);
        Assert.Contains("cluster: \"prod\"", code);
        Assert.Contains("ifExists: true", code);
    }

    [Fact]
    public void CreateDatabase_and_DropDatabase_emit_named_calls()
    {
        var create = Generate(new ClickHouseCreateDatabaseOperation { Name = "analytics" });
        Assert.Contains("migrationBuilder.CreateClickHouseDatabase(", create);
        Assert.Contains("name: \"analytics\"", create);

        var drop = Generate(new ClickHouseDropDatabaseOperation { Name = "analytics" });
        Assert.Contains("migrationBuilder.DropClickHouseDatabase(", drop);
        Assert.Contains("name: \"analytics\"", drop);
    }

    [Fact]
    public void CreateDictionary_emits_builder_call_with_columns_and_keys()
    {
        var code = Generate(new ClickHouseCreateDictionaryOperation
        {
            DictionaryName = "currency_dict",
            Columns =
            [
                new ClickHouseDictionaryColumn("Id", "UInt64"),
                new ClickHouseDictionaryColumn("Name", "String", "''"),
            ],
            KeyColumns = ["Id"],
            SourceTable = "currencies",
            Layout = "HASHED",
            LifetimeMin = 300,
            LifetimeMax = 360,
        });

        Assert.Contains("migrationBuilder.CreateClickHouseDictionary(", code);
        Assert.Contains("name: \"currency_dict\"", code);
        Assert.Contains("new ClickHouseDictionaryColumn(\"Id\", \"UInt64\")", code);
        Assert.Contains("new ClickHouseDictionaryColumn(\"Name\", \"String\", \"''\")", code);
        Assert.Contains("keyColumns: new[] { \"Id\" }", code);
        Assert.Contains("sourceTable: \"currencies\"", code);
        Assert.Contains("layout: \"HASHED\"", code);
        Assert.Contains("lifetimeMin: 300", code);
        Assert.Contains("lifetimeMax: 360", code);
    }

    [Fact]
    public void DropDictionary_emits_builder_call()
    {
        var code = Generate(new ClickHouseDropDictionaryOperation { DictionaryName = "currency_dict", IfExists = true });
        Assert.Contains("migrationBuilder.DropClickHouseDictionary(", code);
        Assert.Contains("name: \"currency_dict\"", code);
        Assert.Contains("ifExists: true", code);
    }

    [Fact]
    public void AddProjection_minimal_emits_required_args_and_omits_default_materialize()
    {
        var code = Generate(new ClickHouseAddProjectionOperation
        {
            Table = "events",
            ProjectionName = "p_by_cat",
            SelectQuery = "SELECT Category, count() GROUP BY Category",
        });

        Assert.Contains("migrationBuilder.AddClickHouseProjection(", code);
        Assert.Contains("table: \"events\"", code);
        Assert.Contains("name: \"p_by_cat\"", code);
        Assert.Contains("selectQuery: \"SELECT Category, count() GROUP BY Category\"", code);
        // Materialize defaults to true → not emitted; other optionals omitted too.
        Assert.DoesNotContain("materialize:", code);
        Assert.DoesNotContain("schema:", code);
        Assert.DoesNotContain("cluster:", code);
        Assert.DoesNotContain("ifNotExists:", code);
    }

    [Fact]
    public void AddProjection_full_emits_all_non_default_args()
    {
        var code = Generate(new ClickHouseAddProjectionOperation
        {
            Table = "events",
            Schema = "analytics",
            ProjectionName = "p",
            SelectQuery = "SELECT a",
            Cluster = "prod",
            Materialize = false,
            IfNotExists = true,
        });

        Assert.Contains("schema: \"analytics\"", code);
        Assert.Contains("cluster: \"prod\"", code);
        Assert.Contains("materialize: false", code);
        Assert.Contains("ifNotExists: true", code);
    }

    [Fact]
    public void DropProjection_emits_builder_call()
    {
        var code = Generate(new ClickHouseDropProjectionOperation
        {
            Table = "events",
            ProjectionName = "p",
            IfExists = true,
        });

        Assert.Contains("migrationBuilder.DropClickHouseProjection(", code);
        Assert.Contains("table: \"events\"", code);
        Assert.Contains("name: \"p\"", code);
        Assert.Contains("ifExists: true", code);
    }

    [Fact]
    public void Builtin_operations_still_generate_via_base()
    {
        // The override must delegate unknown operations to the base generator.
        var code = Generate(new DropTableOperation { Name = "some_table" });
        Assert.Contains("migrationBuilder.DropTable(", code);
        Assert.Contains("name: \"some_table\"", code);
    }
}
