using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace ClickHouse.EntityFrameworkCore.Migrations.Operations;

/// <summary>
/// Adds a projection to a MergeTree-family table: <c>ALTER TABLE … ADD PROJECTION name (SELECT …)</c>,
/// optionally followed by <c>MATERIALIZE PROJECTION</c> so existing parts are covered.
/// </summary>
public class ClickHouseAddProjectionOperation : MigrationOperation
{
    public required string Table { get; set; }
    public string? Schema { get; set; }
    public required string ProjectionName { get; set; }

    /// <summary>The projection body: a SELECT list with optional GROUP BY / ORDER BY and no FROM clause.</summary>
    public required string SelectQuery { get; set; }

    public string? Cluster { get; set; }

    /// <summary>When true (the default), emit a follow-up MATERIALIZE PROJECTION so existing parts are projected.</summary>
    public bool Materialize { get; set; } = true;

    public bool IfNotExists { get; set; }
}
