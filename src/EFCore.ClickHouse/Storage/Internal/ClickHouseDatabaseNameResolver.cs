using Microsoft.Extensions.Logging;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal;

internal static class ClickHouseDatabaseNameResolver
{
    public static string Resolve(
        string? connectionDatabase,
        string? schema,
        ILogger logger)
    {
        var hasConnectionDatabase = !string.IsNullOrWhiteSpace(connectionDatabase);
        var hasSchema = !string.IsNullOrWhiteSpace(schema);

        if (!hasSchema)
            return connectionDatabase ?? string.Empty;

        if (!hasConnectionDatabase)
            return schema!;

        if (string.Equals(connectionDatabase, schema, StringComparison.Ordinal))
        {
            logger.LogInformation(
                "The ClickHouse connection string database '{Database}' is the same as the configured EF Core schema." +
                " The Database defined in the Schema overrides the Connection String database. Setting both is unnecessary.",
                connectionDatabase);
        }
        else
        {
            logger.LogWarning(
                "The configured EF Core schema '{Schema}' overrides the ClickHouse connection " +
                "string database '{Database}'. The connection string database will not be used for this context.",
                schema,
                connectionDatabase);
        }

        return schema!;
    }
}
