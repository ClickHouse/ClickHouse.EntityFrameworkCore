using System.Globalization;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

public class ClickHouseFloatTypeMapping : RelationalTypeMapping
{
    public ClickHouseFloatTypeMapping()
        : base("Float32", typeof(float), System.Data.DbType.Single)
    {
    }

    protected ClickHouseFloatTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseFloatTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var f = (float)value;
        return f switch
        {
            float.NaN => "CAST('NaN' AS Float32)",
            float.PositiveInfinity => "CAST('Inf' AS Float32)",
            float.NegativeInfinity => "CAST('-Inf' AS Float32)",
            _ => f.ToString("G9", CultureInfo.InvariantCulture)
        };
    }
}
