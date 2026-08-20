using System.Text.RegularExpressions;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

/// <summary>
/// A date/time mapping whose ClickHouse store type can declare a timezone, such as
/// <c>DateTime64(3, 'Europe/London')</c>.
/// </summary>
/// <remarks>
/// The declared timezone decides how ClickHouse renders an instant, and therefore how its date/time
/// functions behave. <see cref="ClickHouseTimezones"/> classifies the name.
/// </remarks>
public interface IClickHouseTimezoneTypeMapping
{
    /// <summary>
    /// The timezone the store type declares, or <see langword="null" /> when it declares none.
    /// </summary>
    string? Timezone { get; }
}

/// <summary>
/// Classifies the timezone name in a ClickHouse date/time store type.
/// </summary>
public static class ClickHouseTimezones
{
    /// <summary>The one named timezone that is known to have no daylight saving.</summary>
    public const string Utc = "UTC";

    /// <summary>
    /// ClickHouse spells a fixed-offset timezone <c>Fixed/UTC±HH:MM:SS</c>. The pattern is strict
    /// because the offset is read back from the captured groups, and .NET has no timezone of that name.
    /// </summary>
    public static readonly Regex FixedOffsetRegex = new(
        @"^Fixed/UTC([+-])(\d{2}):(\d{2}):(\d{2})$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// Whether the timezone has one offset for every instant, so that adding a calendar unit and adding
    /// the matching span of absolute time always agree.
    /// </summary>
    public static bool IsFixedOffset(string? timezone)
        => timezone == Utc || (timezone is not null && FixedOffsetRegex.IsMatch(timezone));

    /// <summary>
    /// Whether the store type names a timezone that may change offset, which makes ClickHouse date/time
    /// arithmetic disagree with .NET.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A named zone with daylight saving breaks both halves of the ClickHouse <c>add*</c> family, in
    /// different ways. The calendar functions (<c>addDays</c>, <c>addMonths</c>, <c>addYears</c>) keep
    /// the wall clock, which normally matches .NET, but a result that lands in the hour the clocks skip
    /// does not exist: measured on ClickHouse 26.7,
    /// <c>addDays(toDateTime64('2024-03-30 01:30:00', 3, 'Europe/London'), 1)</c> gives
    /// <c>2024-03-31 00:30</c>, whereas .NET gives <c>01:30</c>. The absolute functions
    /// (<c>addHours</c> … <c>addMilliseconds</c>) move the instant, so any interval that crosses a
    /// transition shifts the wall clock by an hour against .NET.
    /// </para>
    /// <para>
    /// A store type that declares no timezone is not one of these. The driver reads such a column as a
    /// UTC wall clock, so absolute arithmetic agrees with .NET. Its calendar arithmetic still follows the
    /// server's <c>session_timezone</c>, which cannot be seen from here — that residual limit is
    /// documented rather than translated around, because refusing it would give up the default mapping.
    /// </para>
    /// </remarks>
    public static bool MayObserveDaylightSaving(string? timezone)
        => timezone is not null && !IsFixedOffset(timezone);
}
