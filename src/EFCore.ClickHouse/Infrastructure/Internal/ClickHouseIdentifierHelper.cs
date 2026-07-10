namespace ClickHouse.EntityFrameworkCore.Infrastructure.Internal;

internal static class ClickHouseIdentifierHelper
{
    internal static string DelimitIdentifier(string name)
        => $"`{name.Replace("`", "``")}`";
    
    internal static string BuildQualifiedTableName(string tableName, string? schema)
        => string.IsNullOrWhiteSpace(schema)
            ? DelimitIdentifier(tableName)
            : $"{DelimitIdentifier(schema)}.{DelimitIdentifier(tableName)}";
}