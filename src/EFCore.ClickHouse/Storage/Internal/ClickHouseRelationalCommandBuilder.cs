using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal;

public class ClickHouseRelationalCommandBuilder : RelationalCommandBuilder
{
    public ClickHouseRelationalCommandBuilder(RelationalCommandBuilderDependencies dependencies)
        : base(dependencies)
    {
    }

    public override IRelationalCommand Build()
    {
        var command = base.Build();
        return new ClickHouseRelationalCommand(
            Dependencies, command.CommandText, command.LogCommandText, command.Parameters);
    }
}
