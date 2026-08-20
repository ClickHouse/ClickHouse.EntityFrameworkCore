using System.Collections.Concurrent;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;
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
/// A column may declare a fixed UTC offset rather than a named zone, which ClickHouse spells
/// <c>Fixed/UTC±HH:MM:SS</c>. <see cref="TryParseFixedOffset"/> reads those, because .NET has no
/// timezone of that name.
///
/// A column that declares a timezone with daylight saving needs care on read. The driver gives a
/// wall clock in that timezone and drops the offset, so the repeated hour when clocks go back is
/// ambiguous. <see cref="ResolveOffset"/> recovers it when the zone's standard offset is zero, for
/// example Europe/London, because a zero candidate can then be discarded. Where both candidates are
/// not zero, such as Europe/Paris, the instant cannot be recovered and the read throws — reporting
/// standard time would move the instant and map two distinct instants onto one. The default
/// <c>'UTC'</c> store type has no daylight saving and is not affected, and neither is a fixed
/// offset, which by definition never changes.
///
/// Two more reads throw rather than return a value that is quietly wrong: a value before
/// <see cref="FirstStandardTimeYear"/> in a named zone, where the offset is Local Mean Time to the
/// second and <see cref="TimeZoneInfo"/> may round it to the minute; and a fixed-offset name the
/// driver does not apply. An offset outside what <see cref="DateTimeOffset"/> can hold is not one of
/// them — the instant is still exact, so it is reported at offset zero.
///
/// On write, <see cref="ValidateWriteValue"/> refuses a value the store type cannot hold. ClickHouse
/// wraps such a value rather than reporting it.
/// </summary>
public class ClickHouseDateTimeOffsetTypeMapping : RelationalTypeMapping, IClickHouseWriteValidatingTypeMapping
{
    /// <summary>
    /// One .NET tick is 100 ns, which is precision 7. This makes the round trip exact, so a
    /// stored value never comes back truncated.
    /// </summary>
    public const int DefaultPrecision = 7;

    public const string DefaultTimezone = "UTC";

    /// <summary>.NET cannot render more than 7 fractional digits, because a tick is its smallest unit.</summary>
    private const int MaxFractionalDigits = 7;

    /// <summary>
    /// The first year for which a named timezone has a standard offset in whole minutes. Before it,
    /// IANA records Local Mean Time to the second and <see cref="TimeZoneInfo"/> may round to the
    /// minute. ClickHouse documents the same year as the start of the DateTime64 range.
    /// </summary>
    private const int FirstStandardTimeYear = 1900;

    /// <summary>
    /// ClickHouse spells a fixed-offset timezone <c>Fixed/UTC±HH:MM:SS</c>. See
    /// <see cref="TryParseFixedOffset"/> for why the pattern is this strict.
    /// </summary>
    private static readonly Regex FixedOffsetRegex = new(
        @"^Fixed/UTC([+-])(\d{2}):(\d{2}):(\d{2})$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    // DateTimeOffset holds an offset only within ±14 hours, and only in whole minutes. ClickHouse
    // accepts both a larger magnitude and a finer granularity, for example 'Fixed/UTC+00:00:42'.
    private static readonly TimeSpan MaxRepresentableOffset = TimeSpan.FromHours(14);
    private static readonly TimeSpan MinRepresentableOffset = TimeSpan.FromHours(-14);

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

    /// <summary>
    /// Whether the declared timezone has one offset for every instant. Date/time addition can only
    /// preserve <see cref="DateTimeOffset"/> semantics for these mappings: named zones with daylight
    /// saving may change offset while .NET deliberately keeps the instance offset.
    /// </summary>
    internal bool HasFixedOffset
        => Timezone == DefaultTimezone
           || Timezone is not null && FixedOffsetRegex.IsMatch(Timezone);

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

        // A fixed offset resolves without the host's timezone data. This must come before the
        // lookup below, which cannot resolve such a name.
        if (TryParseFixedOffset(timezone, out var fixedOffset))
        {
            var fixedWallClock = DateTime.SpecifyKind(dateTime, DateTimeKind.Unspecified);

            // DateTimeOffset caps an offset at plus or minus 14 hours and holds only whole minutes,
            // while ClickHouse accepts more. The instant is still exact — subtract the offset from
            // the wall clock — so report it at offset zero rather than refusing to read the column.
            // This mapping already does not keep the offset, so nothing more is lost here.
            return IsRepresentableOffset(fixedOffset)
                ? new DateTimeOffset(fixedWallClock, fixedOffset)
                : new DateTimeOffset(DateTime.SpecifyKind(fixedWallClock - fixedOffset, DateTimeKind.Utc));
        }

        var zone = FindTimeZone(timezone)
            ?? throw new InvalidOperationException(
                $"Cannot read the DateTimeOffset column because this machine does not know the "
                + $"timezone '{timezone}' that the column declares. The driver gives a wall clock in "
                + $"that timezone, so the offset cannot be found without it. Install the operating "
                + $"system timezone data (the 'tzdata' package on a minimal Linux image), or declare "
                + $"the column as DateTime64(P, 'UTC').");

        var wallClock = DateTime.SpecifyKind(dateTime, DateTimeKind.Unspecified);

        // Before standard time, a zone's offset is Local Mean Time, which IANA records to the
        // second — Asia/Tokyo is +09:18:59. TimeZoneInfo rounds that to whole minutes on some
        // hosts, so the instant would come back quietly shifted by up to a minute. ClickHouse
        // documents DateTime64 as valid from 1900 for the same reason, so refuse rather than guess.
        if (wallClock.Year < FirstStandardTimeYear)
            throw new InvalidOperationException(
                $"Cannot read the DateTimeOffset column because the value {wallClock:yyyy-MM-dd HH:mm:ss} "
                + $"predates standard time in the timezone '{timezone}' that the column declares. Before "
                + $"{FirstStandardTimeYear} a zone's offset is Local Mean Time, which is recorded to the "
                + $"second, and TimeZoneInfo rounds it to whole minutes — so the instant cannot be "
                + $"reproduced exactly. Declare the column as DateTime64(P, 'UTC') to store such a value.");

        return new DateTimeOffset(wallClock, ResolveOffset(zone, wallClock, timezone));
    }

    /// <summary>
    /// Reports whether <see cref="DateTimeOffset"/> can hold <paramref name="offset"/>: it caps the
    /// magnitude at 14 hours and accepts only whole minutes.
    /// </summary>
    private static bool IsRepresentableOffset(TimeSpan offset)
        => offset >= MinRepresentableOffset
            && offset <= MaxRepresentableOffset
            && offset.Ticks % TimeSpan.TicksPerMinute == 0;

    /// <summary>
    /// Reads a ClickHouse fixed-offset timezone name into its offset.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ClickHouse lets a column declare a fixed UTC offset instead of a named zone, and spells it
    /// <c>Fixed/UTC±HH:MM:SS</c> — for example <c>DateTime64(7, 'Fixed/UTC+05:30:00')</c>. Each
    /// field must have exactly two digits, and the server rejects <c>Fixed/UTC+5:30:00</c>,
    /// <c>Fixed/UTC+05:30</c> and any change of case. Such a name is not in the IANA database, so
    /// <see cref="TimeZoneInfo.FindSystemTimeZoneById"/> cannot resolve it however complete the
    /// host's timezone data is. It needs no daylight-saving logic either, because the offset is
    /// fixed by definition, so the reading is never ambiguous.
    /// </para>
    /// <para>
    /// The minutes and seconds fields are not held to 59. ClickHouse carries the excess, so
    /// <c>Fixed/UTC+05:60:00</c> is a legal name for the offset <c>+06:00</c>, and the server
    /// accepts any name up to a total of 24 hours. The whole shape is matched here so that such a
    /// name is diagnosed rather than left to the unresolvable-timezone error, but the offset is
    /// only returned for the spelling the driver also reads. See the throw below.
    /// </para>
    /// </remarks>
    private static bool TryParseFixedOffset(string timezone, out TimeSpan offset)
    {
        offset = default;

        var match = FixedOffsetRegex.Match(timezone);
        if (!match.Success)
            return false;

        var magnitude = new TimeSpan(
            int.Parse(match.Groups[2].Value, CultureInfo.InvariantCulture),
            int.Parse(match.Groups[3].Value, CultureInfo.InvariantCulture),
            int.Parse(match.Groups[4].Value, CultureInfo.InvariantCulture));

        var sign = match.Groups[1].Value == "-" ? -1 : 1;
        var candidate = sign * magnitude;

        // An offset that DateTimeOffset cannot hold is no longer refused. The caller reports the
        // instant at offset zero instead — see IsRepresentableOffset and its use above.

        // ClickHouse carries minutes and seconds above 59, so 'Fixed/UTC+05:60:00' is a legal name
        // for the offset +06:00. The driver does not read those, and returns a UTC wall clock
        // instead of one in the column's timezone, so the offset here cannot be attached to it —
        // that would move the instant by the whole offset and report nothing. Only the spelling the
        // driver agrees with can be read.
        if (magnitude.Minutes != int.Parse(match.Groups[3].Value, CultureInfo.InvariantCulture)
            || magnitude.Seconds != int.Parse(match.Groups[4].Value, CultureInfo.InvariantCulture))
        {
            throw new InvalidOperationException(
                $"Cannot read the DateTimeOffset column because the ClickHouse driver does not "
                + $"support the timezone '{timezone}' that the column declares. ClickHouse reads it "
                + $"as the offset {candidate}, but only spells that offset in a form the driver "
                + $"accepts when the minutes and seconds are below 60. Declare the column as "
                + $"DateTime64(P, '{FormatFixedOffset(candidate)}') instead.");
        }

        offset = candidate;
        return true;
    }

    /// <summary>Spells an offset the way ClickHouse names a fixed-offset timezone.</summary>
    private static string FormatFixedOffset(TimeSpan offset)
        => string.Create(
            CultureInfo.InvariantCulture,
            $"Fixed/UTC{(offset < TimeSpan.Zero ? '-' : '+')}{offset.Duration():hh\\:mm\\:ss}");

    private static TimeSpan ResolveOffset(TimeZoneInfo zone, DateTime wallClock, string timezone)
    {
        // GetUtcOffset reads an Unspecified value as a local time in the given zone.
        if (!zone.IsAmbiguousTime(wallClock))
            return zone.GetUtcOffset(wallClock);

        // An ambiguous wall clock — the hour that repeats when clocks go back — has two candidate
        // offsets, and GetUtcOffset would pick the standard-time one. We know more than it does:
        // the driver only gives Kind=Unspecified when the true offset is not zero, so a zero
        // candidate can be discarded. That recovers the exact instant for every zone whose
        // standard offset is zero, such as Europe/London.
        var standardOffset = zone.GetUtcOffset(wallClock);
        if (standardOffset == TimeSpan.Zero)
        {
            return zone.GetAmbiguousTimeOffsets(wallClock)
                .FirstOrDefault(candidate => candidate != TimeSpan.Zero, standardOffset);
        }

        // Where both candidates are non-zero (Europe/Paris, America/New_York) the offset the driver
        // dropped cannot be recovered. Picking one would silently move the instant of every value in
        // the repeated hour, and would map two distinct instants onto the same result, so refuse.
        var candidates = zone.GetAmbiguousTimeOffsets(wallClock);
        throw new InvalidOperationException(
            $"Cannot read the DateTimeOffset column because the wall clock "
            + $"{wallClock:yyyy-MM-dd HH:mm:ss} is ambiguous in the timezone '{timezone}' that the "
            + $"column declares. It is the hour that repeats when clocks go back, so it means either "
            + $"{string.Join(" or ", candidates.Select(FormatOffset))}, and the ClickHouse driver "
            + $"gives a wall clock without the offset. Two distinct instants would read back as one. "
            + $"Declare the column as DateTime64(P, 'UTC') to store an unambiguous instant.");
    }

    private static string FormatOffset(TimeSpan offset)
        => string.Create(CultureInfo.InvariantCulture, $"{(offset < TimeSpan.Zero ? '-' : '+')}{offset.Duration():hh\\:mm}");

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

    /// <summary>
    /// ClickHouse holds a <c>DateTime64(P)</c> as an <see cref="long"/> count of 10^-P seconds since
    /// the epoch, and a <c>DateTime</c> as a <see cref="uint"/> count of seconds. Neither reports a
    /// value that does not fit — the count wraps, and the row comes back with a different date
    /// entirely. Precision 7 spans about 29 000 years and so covers every
    /// <see cref="DateTimeOffset"/>, but a finer precision does not: precision 9 reaches only
    /// 1678–2262. So the value is checked here rather than left to wrap silently.
    /// </summary>
    public void ValidateWriteValue(object value, string? columnName)
    {
        if (value is not DateTimeOffset dateTimeOffset)
            return;

        var seconds = dateTimeOffset.ToUnixTimeSeconds();
        var (min, max) = RepresentableSecondsRange(Precision);
        if (seconds >= min && seconds <= max)
            return;

        var column = columnName is null ? "a DateTimeOffset column" : $"column '{columnName}'";
        throw new InvalidOperationException(
            $"Cannot write {dateTimeOffset:yyyy-MM-dd HH:mm:ssK} to {column}, because the store type "
            + $"'{StoreType}' holds only {DateTimeOffset.FromUnixTimeSeconds(min):yyyy-MM-dd} to "
            + $"{DateTimeOffset.FromUnixTimeSeconds(max):yyyy-MM-dd}. ClickHouse would wrap the value "
            + $"rather than report it, and the row would read back with a different date. Use a "
            + $"coarser precision — DateTime64(7) covers the whole DateTimeOffset range — or store a "
            + $"value inside the range.");
    }

    /// <summary>
    /// The instants a store type of the given precision can hold, in seconds from the epoch.
    /// </summary>
    private static (long Min, long Max) RepresentableSecondsRange(int? precision)
    {
        // No precision means the second-resolution DateTime store type, an unsigned 32-bit count.
        if (precision is null)
            return (0, uint.MaxValue);

        var ticksPerSecond = 1L;
        for (var i = 0; i < precision.Value; i++)
            ticksPerSecond *= 10;

        var magnitude = long.MaxValue / ticksPerSecond;

        // Never report a range wider than DateTimeOffset itself, so the message stays meaningful.
        return (
            Math.Max(-magnitude, DateTimeOffset.MinValue.ToUnixTimeSeconds()),
            Math.Min(magnitude, DateTimeOffset.MaxValue.ToUnixTimeSeconds()));
    }

    // An ISO-8601 literal that carries the offset is instant-exact whatever timezone the target
    // column declares. A bare wall clock is not: the server reads it in the column's timezone.
    protected override string GenerateNonNullSqlLiteral(object value)
    {
        ValidateWriteValue(value, columnName: null);
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
