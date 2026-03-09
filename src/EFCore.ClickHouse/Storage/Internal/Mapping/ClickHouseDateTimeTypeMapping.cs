using System.Globalization;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

public class ClickHouseDateTimeTypeMapping : RelationalTypeMapping
{
    public string? Timezone { get; }

    public ClickHouseDateTimeTypeMapping(string? timezone = null)
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(typeof(DateTime)),
                FormatStoreType(timezone),
                StoreTypePostfix.None,
                System.Data.DbType.DateTime))
    {
        Timezone = timezone;
    }

    protected ClickHouseDateTimeTypeMapping(RelationalTypeMappingParameters parameters, string? timezone)
        : base(parameters)
    {
        Timezone = timezone;
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseDateTimeTypeMapping(parameters, Timezone);

    protected override string GenerateNonNullSqlLiteral(object value)
        => $"'{((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)}'";

    private static string FormatStoreType(string? timezone)
        => timezone is null ? "DateTime" : $"DateTime('{timezone}')";
}
