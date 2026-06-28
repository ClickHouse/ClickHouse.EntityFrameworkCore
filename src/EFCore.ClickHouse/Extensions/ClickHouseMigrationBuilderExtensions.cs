using ClickHouse.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations.Builders;

namespace ClickHouse.EntityFrameworkCore.Extensions;

public static class ClickHouseMigrationBuilderExtensions
{
    public static OperationBuilder<ClickHouseCreateDatabaseOperation> CreateClickHouseDatabase(
        this MigrationBuilder builder,
        string name)
    {
        var operation = new ClickHouseCreateDatabaseOperation { Name = name };
        builder.Operations.Add(operation);
        return new OperationBuilder<ClickHouseCreateDatabaseOperation>(operation);
    }

    public static OperationBuilder<ClickHouseDropDatabaseOperation> DropClickHouseDatabase(
        this MigrationBuilder builder,
        string name)
    {
        var operation = new ClickHouseDropDatabaseOperation { Name = name };
        builder.Operations.Add(operation);
        return new OperationBuilder<ClickHouseDropDatabaseOperation>(operation);
    }

    public static OperationBuilder<ClickHouseCreateMaterializedViewOperation> CreateClickHouseMaterializedView(
        this MigrationBuilder builder,
        string viewName,
        string targetTable,
        string selectQuery,
        string? database = null,
        string? targetDatabase = null,
        string? cluster = null,
        bool ifNotExists = false,
        bool populate = false)
    {
        var operation = new ClickHouseCreateMaterializedViewOperation
        {
            ViewName = viewName,
            TargetTable = targetTable,
            SelectQuery = selectQuery,
            Database = database,
            TargetDatabase = targetDatabase,
            Cluster = cluster,
            IfNotExists = ifNotExists,
            Populate = populate,
        };
        builder.Operations.Add(operation);
        return new OperationBuilder<ClickHouseCreateMaterializedViewOperation>(operation);
    }

    public static OperationBuilder<ClickHouseDropMaterializedViewOperation> DropClickHouseMaterializedView(
        this MigrationBuilder builder,
        string viewName,
        string? database = null,
        string? cluster = null,
        bool ifExists = false)
    {
        var operation = new ClickHouseDropMaterializedViewOperation
        {
            ViewName = viewName,
            Database = database,
            Cluster = cluster,
            IfExists = ifExists,
        };
        builder.Operations.Add(operation);
        return new OperationBuilder<ClickHouseDropMaterializedViewOperation>(operation);
    }
}
