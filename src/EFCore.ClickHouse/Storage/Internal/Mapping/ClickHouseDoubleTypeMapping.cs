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
        // EF Core can hand us a boxed value whose runtime type differs from double
        // (e.g. an Int32 0 from SUM's COALESCE(SUM(x), 0) rewrite), so convert
        // rather than unbox to avoid an InvalidCastException.
        var d = Convert.ToDouble(value, CultureInfo.InvariantCulture);
        return d switch
        {
            double.NaN => "CAST('NaN' AS Float64)",
            double.PositiveInfinity => "CAST('Inf' AS Float64)",
            double.NegativeInfinity => "CAST('-Inf' AS Float64)",
            _ => d.ToString("G17", CultureInfo.InvariantCulture)
        };
    }
}
