using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace ClickHouse.EntityFrameworkCore.Migrations.Operations;

public class ClickHouseCreateMaterializedViewOperation : MigrationOperation
{
    public required string ViewName { get; set; }
    public required string TargetTable { get; set; }
    public required string SelectQuery { get; set; }
    public string? Database { get; set; }
    public string? TargetDatabase { get; set; }
    public string? Cluster { get; set; }
    public bool IfNotExists { get; set; }
    public bool Populate { get; set; }
}
