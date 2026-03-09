using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

public class ClickHouseBoolTypeMapping : BoolTypeMapping
{
    public ClickHouseBoolTypeMapping()
        : base("Bool", System.Data.DbType.Boolean)
    {
    }

    protected ClickHouseBoolTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseBoolTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
        => (bool)value ? "1" : "0";
}
