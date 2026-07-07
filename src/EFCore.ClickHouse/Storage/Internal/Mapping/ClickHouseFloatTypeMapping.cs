using System.Data.Common;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

public class ClickHouseFloatTypeMapping : RelationalTypeMapping
{
    private static readonly MethodInfo GetValueMethod =
        typeof(DbDataReader).GetRuntimeMethod(nameof(DbDataReader.GetValue), [typeof(int)])!;

    private static readonly MethodInfo ConvertToSingleMethod =
        typeof(Convert).GetMethod(nameof(Convert.ToSingle), [typeof(object)])!;

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

    // ClickHouse widens Float32 aggregates to Float64 (e.g. sum(Float32) returns
    // Float64), so the driver's GetFloat() would throw when downcasting. Read the
    // raw value and convert instead.
    public override MethodInfo GetDataReaderMethod()
        => GetValueMethod;

    public override Expression CustomizeDataReaderExpression(Expression expression)
        => Expression.Call(ConvertToSingleMethod, expression);

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        // EF Core can hand us a boxed value whose runtime type differs from float
        // (e.g. an Int32 0 from SUM's COALESCE(SUM(x), 0) rewrite), so convert
        // rather than unbox to avoid an InvalidCastException.
        var f = Convert.ToSingle(value, CultureInfo.InvariantCulture);
        return f switch
        {
            float.NaN => "CAST('NaN' AS Float32)",
            float.PositiveInfinity => "CAST('Inf' AS Float32)",
            float.NegativeInfinity => "CAST('-Inf' AS Float32)",
            _ => f.ToString("G9", CultureInfo.InvariantCulture)
        };
    }
}
