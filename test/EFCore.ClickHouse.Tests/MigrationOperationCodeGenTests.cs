using ClickHouse.EntityFrameworkCore.Design.Internal;
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
    public void Builtin_operations_still_generate_via_base()
    {
        // The override must delegate unknown operations to the base generator.
        var code = Generate(new DropTableOperation { Name = "some_table" });
        Assert.Contains("migrationBuilder.DropTable(", code);
        Assert.Contains("name: \"some_table\"", code);
    }
}
