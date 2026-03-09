using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

public class ClickHouseFixedStringTypeMapping : RelationalTypeMapping
{
    public ClickHouseFixedStringTypeMapping(int size)
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(typeof(string)),
                $"FixedString({size})",
                StoreTypePostfix.None,
                System.Data.DbType.StringFixedLength,
                size: size,
                fixedLength: true))
    {
    }

    protected ClickHouseFixedStringTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseFixedStringTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var str = (string)value;
        var escaped = str.Replace("\\", "\\\\").Replace("'", "\\'");
        return $"'{escaped}'";
    }
}
