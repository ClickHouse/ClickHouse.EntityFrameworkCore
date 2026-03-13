using System.Data.Common;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using ClickHouse.Driver.Numerics;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

/// <summary>
/// Maps ClickHouse Decimal types to <see cref="ClickHouseDecimal"/> for full-precision support.
/// Use this instead of <see cref="ClickHouseDecimalTypeMapping"/> when you need Decimal128/256
/// precision beyond .NET decimal's 28-29 digit limit.
/// </summary>
public class ClickHouseBigDecimalTypeMapping : RelationalTypeMapping
{
    private const int DefaultPrecision = 38;
    private const int DefaultScale = 18;

    private static readonly MethodInfo GetValueMethod =
        typeof(DbDataReader).GetRuntimeMethod(nameof(DbDataReader.GetValue), [typeof(int)])!;

    private static readonly MethodInfo ConvertMethod =
        typeof(ClickHouseBigDecimalTypeMapping).GetMethod(nameof(ConvertToClickHouseDecimal), BindingFlags.Static | BindingFlags.NonPublic)!;

    public ClickHouseBigDecimalTypeMapping(int? precision = null, int? scale = null)
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(typeof(ClickHouseDecimal)),
                FormatStoreType(precision ?? DefaultPrecision, scale ?? DefaultScale),
                StoreTypePostfix.PrecisionAndScale,
                System.Data.DbType.Object,
                precision: precision ?? DefaultPrecision,
                scale: scale ?? DefaultScale))
    {
    }

    protected ClickHouseBigDecimalTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseBigDecimalTypeMapping(parameters);

    public override MethodInfo GetDataReaderMethod()
        => GetValueMethod;

    public override Expression CustomizeDataReaderExpression(Expression expression)
        => Expression.Call(ConvertMethod, expression);

    protected override string GenerateNonNullSqlLiteral(object value)
        => value is ClickHouseDecimal chd
            ? chd.ToString(CultureInfo.InvariantCulture)
            : Convert.ToDecimal(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture);

    // The driver may return either ClickHouseDecimal (UseBigDecimal=true) or decimal (default).
    // Handle both cases.
    private static ClickHouseDecimal ConvertToClickHouseDecimal(object value)
        => value switch
        {
            ClickHouseDecimal chd => chd,
            decimal d => new ClickHouseDecimal(d),
            _ => ClickHouseDecimal.Parse(
                Convert.ToString(value, CultureInfo.InvariantCulture)!,
                CultureInfo.InvariantCulture)
        };

    private static string FormatStoreType(int precision, int scale)
        => $"Decimal({precision},{scale})";
}
