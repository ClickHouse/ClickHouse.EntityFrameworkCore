using System.Collections.Concurrent;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

/// <summary>
/// Maps <see cref="DateTimeOffset"/> to <c>DateTime64</c> (or <c>DateTime</c>).
///
/// ClickHouse has no type that stores a UTC offset: <c>DateTime64</c> holds an instant, and any
/// declared timezone only decides how that instant is rendered. So the instant is preserved and
/// the original offset is not. A value read back carries the offset of the column's declared
/// timezone, which is <c>+00:00</c> for the default store type.
///
/// The default store type pins the timezone to <c>'UTC'</c> on purpose. For a timezone-less
/// parameter type such as <c>DateTime64(7)</c>, the driver sends a UTC wall clock and the server
/// then reads it in <c>session_timezone</c>, which moves the instant when that setting is not UTC.
///
/// No value converter is used. The driver accepts a <see cref="DateTimeOffset"/> directly on both
/// the query parameter path and the bulk insert path, and converts it to the correct instant.
///
/// One limit applies to a column that declares a timezone with daylight saving. The driver gives a
/// wall clock in that timezone and drops the offset, so the repeated hour when clocks go back is
/// ambiguous. <see cref="ResolveOffset"/> recovers it when the zone's standard offset is zero, for
/// example Europe/London. In a zone where both candidate offsets are not zero, such as
/// Europe/Paris, the reading falls back to standard time and can be one hour early. The default
/// <c>'UTC'</c> store type has no daylight saving and is not affected.
/// </summary>
public class ClickHouseDateTimeOffsetTypeMapping : RelationalTypeMapping
{
    /// <summary>
    /// One .NET tick is 100 ns, which is precision 7. This makes the round trip exact, so a
    /// stored value never comes back truncated.
    /// </summary>
    public const int DefaultPrecision = 7;

    public const string DefaultTimezone = "UTC";

    /// <summary>.NET cannot render more than 7 fractional digits, because a tick is its smallest unit.</summary>
    private const int MaxFractionalDigits = 7;

    private static readonly MethodInfo GetValueMethod =
        typeof(DbDataReader).GetRuntimeMethod(nameof(DbDataReader.GetValue), [typeof(int)])!;

    private static readonly MethodInfo ConvertToDateTimeOffsetMethod =
        typeof(ClickHouseDateTimeOffsetTypeMapping).GetMethod(
            nameof(ConvertToDateTimeOffset),
            BindingFlags.Public | BindingFlags.Static)!;

    // Resolved per row during materialization, so the lookup is cached.
    private static readonly ConcurrentDictionary<string, TimeZoneInfo?> TimeZoneCache = new();

    /// <summary>
    /// The timezone declared by the store type, or <see langword="null" /> when the store type
    /// declares none. A timezone-less column is read as a UTC wall clock by the driver.
    /// </summary>
    public string? Timezone { get; }

    public ClickHouseDateTimeOffsetTypeMapping()
        : this(DefaultPrecision, DefaultTimezone)
    {
    }

    /// <param name="precision">
    /// The <c>DateTime64</c> precision, or <see langword="null" /> for the second-precision
    /// <c>DateTime</c> store type.
    /// </param>
    /// <param name="timezone">The declared timezone, or <see langword="null" /> for none.</param>
    public ClickHouseDateTimeOffsetTypeMapping(int? precision, string? timezone)
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(typeof(DateTimeOffset)),
                FormatStoreType(precision, timezone),
                StoreTypePostfix.None,
                System.Data.DbType.DateTimeOffset,
                precision: precision))
    {
        Timezone = timezone;
    }

    protected ClickHouseDateTimeOffsetTypeMapping(RelationalTypeMappingParameters parameters, string? timezone)
        : base(parameters)
    {
        Timezone = timezone;
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseDateTimeOffsetTypeMapping(parameters, Timezone);

    // The driver returns DateTime for DateTime64 columns, never DateTimeOffset —
    // GetFieldValue<DateTimeOffset> throws InvalidCastException. Read the raw value
    // and attach the offset of the column's declared timezone.
    public override MethodInfo GetDataReaderMethod()
        => GetValueMethod;

    public override Expression CustomizeDataReaderExpression(Expression expression)
        => Expression.Call(
            ConvertToDateTimeOffsetMethod,
            expression,
            Expression.Constant(Timezone, typeof(string)));

    /// <summary>
    /// Converts a value read from a ClickHouse date/time column into a <see cref="DateTimeOffset"/>.
    /// </summary>
    /// <remarks>
    /// The driver gives <see cref="DateTimeKind.Utc"/> when the column's timezone is offset zero at
    /// that instant, and a <see cref="DateTimeKind.Unspecified"/> wall clock in the column's
    /// timezone in all other cases. A column with no declared timezone is read as a UTC wall clock.
    /// </remarks>
    public static DateTimeOffset ConvertToDateTimeOffset(object value, string? timezone)
    {
        if (value is DateTimeOffset dateTimeOffset)
            return dateTimeOffset;

        var dateTime = (DateTime)value;

        // The driver already resolved the instant for us.
        if (dateTime.Kind == DateTimeKind.Utc)
            return new DateTimeOffset(dateTime);

        // The column declares no timezone, so the driver's wall clock is already UTC.
        if (timezone is null)
            return new DateTimeOffset(DateTime.SpecifyKind(dateTime, DateTimeKind.Utc));

        var zone = FindTimeZone(timezone)
            ?? throw new InvalidOperationException(
                $"Cannot read the DateTimeOffset column because this machine does not know the "
                + $"timezone '{timezone}' that the column declares. The driver gives a wall clock in "
                + $"that timezone, so the offset cannot be found without it. Install the operating "
                + $"system timezone data (the 'tzdata' package on a minimal Linux image), or declare "
                + $"the column as DateTime64(P, 'UTC').");

        var wallClock = DateTime.SpecifyKind(dateTime, DateTimeKind.Unspecified);
        return new DateTimeOffset(wallClock, ResolveOffset(zone, wallClock));
    }

    private static TimeSpan ResolveOffset(TimeZoneInfo zone, DateTime wallClock)
    {
        // GetUtcOffset reads an Unspecified value as a local time in the given zone.
        if (!zone.IsAmbiguousTime(wallClock))
            return zone.GetUtcOffset(wallClock);

        // An ambiguous wall clock — the hour that repeats when clocks go back — has two candidate
        // offsets, and GetUtcOffset would pick the standard-time one. We know more than it does:
        // the driver only gives Kind=Unspecified when the true offset is not zero, so a zero
        // candidate can be discarded. That recovers the exact instant for every zone whose
        // standard offset is zero, such as Europe/London.
        //
        // Where both candidates are non-zero (Europe/Paris, America/New_York) the offset the
        // driver dropped cannot be recovered, so keep the standard-time reading.
        var standardOffset = zone.GetUtcOffset(wallClock);
        if (standardOffset != TimeSpan.Zero)
            return standardOffset;

        return zone.GetAmbiguousTimeOffsets(wallClock)
            .FirstOrDefault(candidate => candidate != TimeSpan.Zero, standardOffset);
    }

    private static TimeZoneInfo? FindTimeZone(string timezone)
        => TimeZoneCache.GetOrAdd(timezone, static id =>
        {
            try
            {
                return TimeZoneInfo.FindSystemTimeZoneById(id);
            }
            catch (Exception ex) when (ex is TimeZoneNotFoundException or InvalidTimeZoneException)
            {
                return null;
            }
        });

    // An ISO-8601 literal that carries the offset is instant-exact whatever timezone the target
    // column declares. A bare wall clock is not: the server reads it in the column's timezone.
    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var dateTimeOffset = (DateTimeOffset)value;
        var digits = Math.Min(Precision ?? 0, MaxFractionalDigits);
        var fraction = digits == 0 ? string.Empty : "." + new string('f', digits);
        return $"'{dateTimeOffset.ToString($"yyyy-MM-dd HH:mm:ss{fraction}zzz", CultureInfo.InvariantCulture)}'";
    }

    private static string FormatStoreType(int? precision, string? timezone)
        => (precision, timezone) switch
        {
            (null, null) => "DateTime",
            (null, _) => $"DateTime('{timezone}')",
            (_, null) => $"DateTime64({precision})",
            _ => $"DateTime64({precision}, '{timezone}')"
        };
}
