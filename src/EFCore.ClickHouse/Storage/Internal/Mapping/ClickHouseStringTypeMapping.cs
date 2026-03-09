using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

public class ClickHouseStringTypeMapping : StringTypeMapping
{
    public ClickHouseStringTypeMapping()
        : base("String", System.Data.DbType.String)
    {
    }

    protected ClickHouseStringTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseStringTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
        => $"'{EscapeSqlLiteral((string)value)}'";

    protected override string EscapeSqlLiteral(string literal)
        => literal.Replace("\\", "\\\\").Replace("'", "\\'");
}
