using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal;

/// <summary>
/// Creates the builders for <see cref="ClickHouseRelationalCommand"/>. EF Core gets every command it
/// runs from this factory, including the commands that a connection rents for a query.
/// </summary>
public class ClickHouseRelationalCommandBuilderFactory : RelationalCommandBuilderFactory
{
    public ClickHouseRelationalCommandBuilderFactory(RelationalCommandBuilderDependencies dependencies)
        : base(dependencies)
    {
    }

    public override IRelationalCommandBuilder Create()
        => new ClickHouseRelationalCommandBuilder(Dependencies);
}
