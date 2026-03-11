using ClickHouse.EntityFrameworkCore.Storage.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace ClickHouse.EntityFrameworkCore.Extensions;

public static class ClickHouseBulkInsertExtensions
{
    /// <summary>
    /// Inserts entities into ClickHouse using the driver's native binary insert protocol.
    /// This bypasses EF Core change tracking entirely and is intended for high-throughput bulk loads.
    /// Entities are NOT tracked or marked as Unchanged after insert.
    /// </summary>
    public static async Task<long> BulkInsertAsync<TEntity>(
        this DbContext context,
        IEnumerable<TEntity> entities,
        CancellationToken cancellationToken = default) where TEntity : class
    {
        var connection = context.GetService<IClickHouseRelationalConnection>();
        var client = connection.GetClickHouseClient();

        var entityType = context.Model.FindEntityType(typeof(TEntity))
            ?? throw new InvalidOperationException(
                $"The entity type '{typeof(TEntity).Name}' is not part of the model for the current context.");

        var tableName = entityType.GetTableName()
            ?? throw new InvalidOperationException(
                $"The entity type '{typeof(TEntity).Name}' is not mapped to a table.");

        // Build column list and property accessors
        var properties = entityType.GetProperties()
            .Where(p => p.GetTableColumnMappings().Any())
            .ToList();

        var columns = properties
            .Select(p => p.GetTableColumnMappings().First().Column.Name)
            .ToList();

        var accessors = properties
            .Select(p => p.GetGetter())
            .ToList();

        // Convert entities to row arrays
        // TODO quite inefficient, update this after adding direct POCO insert to client API
        var rows = entities.Select(entity =>
        {
            var row = new object[accessors.Count];
            for (var i = 0; i < accessors.Count; i++)
            {
                row[i] = accessors[i].GetClrValue(entity) ?? DBNull.Value;
            }
            return row;
        });

        return await client.InsertBinaryAsync(tableName, columns, rows, cancellationToken: cancellationToken);
    }
}
