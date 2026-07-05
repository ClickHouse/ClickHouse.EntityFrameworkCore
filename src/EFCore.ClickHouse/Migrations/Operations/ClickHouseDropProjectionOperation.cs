using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace ClickHouse.EntityFrameworkCore.Migrations.Operations;

/// <summary>
/// Drops a projection from a table: <c>ALTER TABLE … DROP PROJECTION [IF EXISTS] name</c>.
/// </summary>
public class ClickHouseDropProjectionOperation : MigrationOperation
{
    public required string Table { get; set; }
    public string? Schema { get; set; }
    public required string ProjectionName { get; set; }
    public string? Cluster { get; set; }
    public bool IfExists { get; set; }
}
