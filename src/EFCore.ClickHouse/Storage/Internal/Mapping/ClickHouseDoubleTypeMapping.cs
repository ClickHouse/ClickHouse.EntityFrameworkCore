using System.Globalization;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

public class ClickHouseDoubleTypeMapping : RelationalTypeMapping
{
    public ClickHouseDoubleTypeMapping()
        : base("Float64", typeof(double), System.Data.DbType.Double)
    {
    }

    protected ClickHouseDoubleTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseDoubleTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var d = (double)value;
        return d switch
        {
            double.NaN => "CAST('NaN' AS Float64)",
            double.PositiveInfinity => "CAST('Inf' AS Float64)",
            double.NegativeInfinity => "CAST('-Inf' AS Float64)",
            _ => d.ToString("G17", CultureInfo.InvariantCulture)
        };
    }
}
