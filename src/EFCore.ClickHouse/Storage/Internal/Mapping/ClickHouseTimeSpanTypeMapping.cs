using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

public class ClickHouseTimeSpanTypeMapping : RelationalTypeMapping
{
    private readonly int _fractionalDigits;

    public ClickHouseTimeSpanTypeMapping(int? precision = null)
        : base(
            FormatStoreType(precision),
            typeof(TimeSpan),
            System.Data.DbType.Time)
    {
        // Time = 0 fractional digits (seconds), Time64(N) = N fractional digits
        _fractionalDigits = precision ?? 0;
    }

    protected ClickHouseTimeSpanTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
        _fractionalDigits = parameters.Precision ?? 0;
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseTimeSpanTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var ts = (TimeSpan)value;
        // Use TotalHours (not hh) to support values >= 24 hours
        var basePart = $"{(int)ts.TotalHours:00}:{ts.Minutes:00}:{ts.Seconds:00}";

        if (_fractionalDigits <= 0)
            return $"'{basePart}'";

        // 1 tick = 100ns = 10^-7s, so 7 digits at full resolution; truncate to _fractionalDigits
        var fraction = (ts.Ticks % TimeSpan.TicksPerSecond).ToString("0000000")[.._fractionalDigits];
        return $"'{basePart}.{fraction}'";
    }

    private static string FormatStoreType(int? precision)
        => precision.HasValue ? $"Time64({precision.Value})" : "Time";
}
