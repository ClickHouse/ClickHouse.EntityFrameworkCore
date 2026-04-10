using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace ClickHouse.EntityFrameworkCore.Migrations.Operations;

public class ClickHouseCreateDatabaseOperation : MigrationOperation
{
    public required string Name { get; set; }
}
