using ClickHouse.EntityFrameworkCore.Storage.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;

namespace ClickHouse.EntityFrameworkCore.Update.Internal;

public class ClickHouseModificationCommandBatch : ModificationCommandBatch
{
    private readonly IClickHouseRelationalConnection _connection;
    private readonly List<IReadOnlyModificationCommand> _commands = [];
    private readonly int _maxBatchSize;
    private bool _completed;
    private bool _moreExpected;

    public ClickHouseModificationCommandBatch(
        IClickHouseRelationalConnection connection,
        int maxBatchSize)
    {
        _connection = connection;
        _maxBatchSize = maxBatchSize;
    }

    public override IReadOnlyList<IReadOnlyModificationCommand> ModificationCommands => _commands;

    public override bool RequiresTransaction => false;

    public override bool AreMoreBatchesExpected => _moreExpected;

    public override bool TryAddCommand(IReadOnlyModificationCommand modificationCommand)
    {
        if (_completed)
            throw new InvalidOperationException("Batch has already been completed.");

        if (modificationCommand.EntityState is EntityState.Modified)
            throw new NotSupportedException(
                "UPDATE operations are not supported by the ClickHouse EF Core provider. " +
                "ClickHouse mutations (ALTER TABLE ... UPDATE) are asynchronous and not OLTP-compatible.");

        if (modificationCommand.EntityState is EntityState.Deleted)
            throw new NotSupportedException(
                "DELETE operations are not supported by the ClickHouse EF Core provider. " +
                "ClickHouse mutations (ALTER TABLE ... DELETE) are asynchronous and not OLTP-compatible.");

        if (modificationCommand.EntityState is not EntityState.Added)
            throw new NotSupportedException(
                $"Unexpected entity state '{modificationCommand.EntityState}'. " +
                "The ClickHouse EF Core provider only supports INSERT (EntityState.Added).");

        // Block server-generated values (ClickHouse has no RETURNING / auto-increment)
        foreach (var columnMod in modificationCommand.ColumnModifications)
        {
            if (columnMod.IsRead)
                throw new NotSupportedException(
                    $"Server-generated values are not supported by the ClickHouse EF Core provider. " +
                    $"Column '{columnMod.ColumnName}' on table '{modificationCommand.TableName}' is configured " +
                    $"to read a value back from the database after INSERT. Remove ValueGeneratedOnAdd() or " +
                    $"use HasValueGenerator() with a client-side generator instead.");
        }

        if (_commands.Count >= _maxBatchSize)
            return false;

        _commands.Add(modificationCommand);
        return true;
    }

    public override void Complete(bool moreBatchesExpected)
    {
        _completed = true;
        _moreExpected = moreBatchesExpected;
    }

    public override void Execute(IRelationalConnection connection)
        => ExecuteAsync(connection).GetAwaiter().GetResult();

    public override async Task ExecuteAsync(
        IRelationalConnection connection,
        CancellationToken cancellationToken = default)
    {
        if (_commands.Count == 0)
            return;

        var client = _connection.GetClickHouseClient();

        // Group commands by table name for efficient batching
        var groups = _commands.GroupBy(c => c.TableName);

        foreach (var group in groups)
        {
            var tableName = group.Key;
            var commands = group.ToList();

            // Get column names from the first command's write columns
            var columns = commands[0].ColumnModifications
                .Where(cm => cm.IsWrite)
                .Select(cm => cm.ColumnName)
                .ToList();

            // Build rows as object arrays
            var rows = commands.Select(cmd =>
            {
                var writeColumns = cmd.ColumnModifications.Where(cm => cm.IsWrite).ToList();
                var row = new object[writeColumns.Count];
                for (var i = 0; i < writeColumns.Count; i++)
                {
                    row[i] = writeColumns[i].Value ?? DBNull.Value;
                }
                return row;
            });

            await client.InsertBinaryAsync(tableName, columns, rows, cancellationToken: cancellationToken);
        }

        // Propagate state: mark all entries as Unchanged
        foreach (var command in _commands)
        {
            foreach (var entry in command.Entries)
            {
                entry.EntityState = EntityState.Unchanged;
            }
        }
    }
}
