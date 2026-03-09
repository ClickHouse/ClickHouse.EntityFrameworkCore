using System.Globalization;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

public class ClickHouseDateTime64TypeMapping : RelationalTypeMapping
{
    private const int DefaultPrecision = 3;

    public string? Timezone { get; }

    public ClickHouseDateTime64TypeMapping(int precision = DefaultPrecision, string? timezone = null)
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(typeof(DateTime)),
                FormatStoreType(precision, timezone),
                StoreTypePostfix.None,
                System.Data.DbType.DateTime,
                precision: precision))
    {
        Timezone = timezone;
    }

    protected ClickHouseDateTime64TypeMapping(RelationalTypeMappingParameters parameters, string? timezone)
        : base(parameters)
    {
        Timezone = timezone;
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseDateTime64TypeMapping(parameters, Timezone);

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var dt = (DateTime)value;
        var precision = Precision ?? DefaultPrecision;
        var format = precision switch
        {
            0 => "yyyy-MM-dd HH:mm:ss",
            1 => "yyyy-MM-dd HH:mm:ss.f",
            2 => "yyyy-MM-dd HH:mm:ss.ff",
            3 => "yyyy-MM-dd HH:mm:ss.fff",
            4 => "yyyy-MM-dd HH:mm:ss.ffff",
            5 => "yyyy-MM-dd HH:mm:ss.fffff",
            6 => "yyyy-MM-dd HH:mm:ss.ffffff",
            7 => "yyyy-MM-dd HH:mm:ss.fffffff",
            8 => "yyyy-MM-dd HH:mm:ss.ffffffff",
            9 => "yyyy-MM-dd HH:mm:ss.fffffffff",
            _ => "yyyy-MM-dd HH:mm:ss.fffffff"
        };
        return $"'{dt.ToString(format, CultureInfo.InvariantCulture)}'";
    }

    private static string FormatStoreType(int precision, string? timezone)
        => timezone is null
            ? $"DateTime64({precision})"
            : $"DateTime64({precision}, '{timezone}')";
}
