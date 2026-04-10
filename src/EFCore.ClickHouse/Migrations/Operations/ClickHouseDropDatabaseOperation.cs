using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace ClickHouse.EntityFrameworkCore.Migrations.Operations;

public class ClickHouseDropDatabaseOperation : MigrationOperation
{
    public required string Name { get; set; }
}
