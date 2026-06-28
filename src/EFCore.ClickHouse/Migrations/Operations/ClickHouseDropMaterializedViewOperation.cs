using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace ClickHouse.EntityFrameworkCore.Migrations.Operations;

public class ClickHouseDropMaterializedViewOperation : MigrationOperation
{
    public required string ViewName { get; set; }
    public string? Database { get; set; }
    public string? Cluster { get; set; }
    public bool IfExists { get; set; }
}
