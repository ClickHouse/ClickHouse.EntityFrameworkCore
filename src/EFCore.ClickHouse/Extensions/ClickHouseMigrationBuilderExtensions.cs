using ClickHouse.EntityFrameworkCore.Migrations;
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

    public static OperationBuilder<ClickHouseCreateDictionaryOperation> CreateClickHouseDictionary(
        this MigrationBuilder builder,
        string name,
        IReadOnlyList<ClickHouseDictionaryColumn> columns,
        IReadOnlyList<string> keyColumns,
        string sourceTable,
        string layout,
        string? sourceDatabase = null,
        string? sourceNamedCollection = null,
        string? sourceHost = null,
        int? sourcePort = null,
        string? sourceUser = null,
        string? sourcePassword = null,
        string? layoutParams = null,
        int? lifetimeMin = null,
        int? lifetimeMax = null,
        string? database = null,
        string? cluster = null,
        bool ifNotExists = false,
        bool orReplace = false)
    {
        var operation = new ClickHouseCreateDictionaryOperation
        {
            DictionaryName = name,
            Columns = columns,
            KeyColumns = keyColumns,
            SourceTable = sourceTable,
            SourceDatabase = sourceDatabase,
            SourceNamedCollection = sourceNamedCollection,
            SourceHost = sourceHost,
            SourcePort = sourcePort,
            SourceUser = sourceUser,
            SourcePassword = sourcePassword,
            Layout = layout,
            LayoutParams = layoutParams,
            LifetimeMin = lifetimeMin,
            LifetimeMax = lifetimeMax,
            Database = database,
            Cluster = cluster,
            IfNotExists = ifNotExists,
            OrReplace = orReplace,
        };
        builder.Operations.Add(operation);
        return new OperationBuilder<ClickHouseCreateDictionaryOperation>(operation);
    }

    public static OperationBuilder<ClickHouseDropDictionaryOperation> DropClickHouseDictionary(
        this MigrationBuilder builder,
        string name,
        string? database = null,
        string? cluster = null,
        bool ifExists = false)
    {
        var operation = new ClickHouseDropDictionaryOperation
        {
            DictionaryName = name,
            Database = database,
            Cluster = cluster,
            IfExists = ifExists,
        };
        builder.Operations.Add(operation);
        return new OperationBuilder<ClickHouseDropDictionaryOperation>(operation);
    }
}
