namespace ClickHouse.EntityFrameworkCore.Migrations;

/// <summary>
/// Thrown by the <c>Down</c> method of a generated step migration.
/// <para>
/// Because ClickHouse has no transactions, <c>dotnet ef migrations add</c> is scaffolded as a
/// sequence of single-operation step migrations that are applied forward-only (each step records
/// its own history row so a partial failure is resumable). Reverting an individual step is not
/// supported, so the generated <c>Down</c> methods throw this exception.
/// </para>
/// </summary>
public sealed class ClickHouseDownMigrationNotSupportedException : NotSupportedException
{
    public ClickHouseDownMigrationNotSupportedException(string migrationId)
        : base(
            $"Cannot revert migration '{migrationId}'. ClickHouse migrations are scaffolded as " +
            "forward-only single-operation steps (ClickHouse has no transactions), so Down/rollback " +
            "is not supported. To undo a change, author a new forward migration.")
    {
        MigrationId = migrationId;
    }

    /// <summary>The id of the step migration whose <c>Down</c> was invoked.</summary>
    public string MigrationId { get; }
}
