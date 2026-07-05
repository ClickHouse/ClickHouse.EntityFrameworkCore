using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace ClickHouse.EntityFrameworkCore.Migrations.Operations;

public class ClickHouseDropDictionaryOperation : MigrationOperation
{
    public required string DictionaryName { get; set; }
    public string? Database { get; set; }
    public string? Cluster { get; set; }
    public bool IfExists { get; set; }
}
