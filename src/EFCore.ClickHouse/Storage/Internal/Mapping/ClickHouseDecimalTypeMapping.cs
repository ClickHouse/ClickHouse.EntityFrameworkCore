using System.Globalization;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

public class ClickHouseDecimalTypeMapping : RelationalTypeMapping
{
    private const int DefaultPrecision = 18;
    private const int DefaultScale = 2;

    public ClickHouseDecimalTypeMapping(int? precision = null, int? scale = null, int? size = null)
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(typeof(decimal)),
                FormatStoreType(precision ?? DefaultPrecision, scale ?? DefaultScale),
                StoreTypePostfix.PrecisionAndScale,
                System.Data.DbType.Decimal,
                precision: precision ?? DefaultPrecision,
                scale: scale ?? DefaultScale))
    {
    }

    protected ClickHouseDecimalTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseDecimalTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
        => Convert.ToDecimal(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture);

    private static string FormatStoreType(int precision, int scale)
        => $"Decimal({precision}, {scale})";
}
