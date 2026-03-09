using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

public class ClickHouseGuidTypeMapping : GuidTypeMapping
{
    public ClickHouseGuidTypeMapping()
        : base("UUID", System.Data.DbType.Guid)
    {
    }

    protected ClickHouseGuidTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseGuidTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
        => $"'{(Guid)value}'";
}
