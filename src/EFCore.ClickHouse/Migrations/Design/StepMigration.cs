using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace ClickHouse.EntityFrameworkCore.Migrations.Design;

/// <summary>
/// One scaffolded step: a single migration operation together with its 1-based position in the
/// dependency-ordered sequence produced by <see cref="ClickHouseMigrationsSplitter"/>.
/// </summary>
public sealed record StepMigration(
    int StepNumber,
    string OperationDescription,
    MigrationOperation Operation,
    int OriginalIndex)
{
    /// <summary>The step number formatted as a 3-digit suffix (e.g. "001", "002").</summary>
    public string StepSuffix => StepNumber.ToString("D3");
}
