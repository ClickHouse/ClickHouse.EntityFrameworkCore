using ClickHouse.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace Microsoft.EntityFrameworkCore;

/// <summary>
/// ClickHouse-specific <see cref="DbFunctions"/> extension methods for the <c>toStartOf*</c> family of
/// date-time functions. Each method is translated to the corresponding ClickHouse SQL function and cannot
/// be evaluated on the client.
/// </summary>
public static class ClickHouseDateTimeDbFunctionsExtensions
{
    /// <summary>
    /// Rounds a date or date with time down to the first day of the year.
    /// Maps to ClickHouse: <c>toStartOfYear(source)</c>.
    /// </summary>
    /// <param name="_">The <see cref="DbFunctions"/> instance.</param>
    /// <param name="source">The date or date-time value to truncate.</param>
    [DbFunction("toStartOfYear")]
    public static T ToStartOfYear<T>(this DbFunctions _, T source) =>
        throw new InvalidOperationException(CoreStrings.FunctionOnClient(nameof(ToStartOfYear)));

    /// <summary>
    /// Rounds a date or date with time down to the first day of the quarter.
    /// Maps to ClickHouse: <c>toStartOfQuarter(source)</c>.
    /// </summary>
    /// <param name="_">The <see cref="DbFunctions"/> instance.</param>
    /// <param name="source">The date or date-time value to truncate.</param>
    [DbFunction("toStartOfQuarter")]
    public static T ToStartOfQuarter<T>(this DbFunctions _, T source) =>
        throw new InvalidOperationException(CoreStrings.FunctionOnClient(nameof(ToStartOfQuarter)));

    /// <summary>
    /// Rounds a date or date with time down to the first day of the month.
    /// Maps to ClickHouse: <c>toStartOfMonth(source)</c>.
    /// </summary>
    /// <param name="_">The <see cref="DbFunctions"/> instance.</param>
    /// <param name="source">The date or date-time value to truncate.</param>
    [DbFunction("toStartOfMonth")]
    public static T ToStartOfMonth<T>(this DbFunctions _, T source) =>
        throw new InvalidOperationException(CoreStrings.FunctionOnClient(nameof(ToStartOfMonth)));

    /// <summary>
    /// Rounds a date or date with time down to the start of the week. The default ClickHouse week mode is
    /// <c>0</c>, which treats Sunday as the first day of the week; use the <c>mode</c> overload to change this.
    /// Maps to ClickHouse: <c>toStartOfWeek(source)</c>.
    /// </summary>
    /// <param name="_">The <see cref="DbFunctions"/> instance.</param>
    /// <param name="source">The date or date-time value to truncate.</param>
    [DbFunction("toStartOfWeek")]
    public static T ToStartOfWeek<T>(this DbFunctions _, T source) =>
        throw new InvalidOperationException(CoreStrings.FunctionOnClient(nameof(ToStartOfWeek)));

    /// <summary>
    /// Rounds a date or date with time down to the start of the week using the specified week mode.
    /// Maps to ClickHouse: <c>toStartOfWeek(source, mode)</c>.
    /// </summary>
    /// <param name="_">The <see cref="DbFunctions"/> instance.</param>
    /// <param name="source">The date or date-time value to truncate.</param>
    /// <param name="mode">The ClickHouse week mode (0-9) that determines the first day of the week.</param>
    [DbFunction("toStartOfWeek")]
    public static T ToStartOfWeek<T>(this DbFunctions _, T source, byte mode) =>
        throw new InvalidOperationException(CoreStrings.FunctionOnClient(nameof(ToStartOfWeek)));

    /// <summary>
    /// Rounds a date with time down to the start of the day.
    /// Maps to ClickHouse: <c>toStartOfDay(source)</c>.
    /// </summary>
    /// <param name="_">The <see cref="DbFunctions"/> instance.</param>
    /// <param name="source">The date-time value to truncate.</param>
    [DbFunction("toStartOfDay")]
    public static T ToStartOfDay<T>(this DbFunctions _, T source) =>
        throw new InvalidOperationException(CoreStrings.FunctionOnClient(nameof(ToStartOfDay)));

    /// <summary>
    /// Rounds a date with time down to the start of the hour.
    /// Maps to ClickHouse: <c>toStartOfHour(source)</c>.
    /// </summary>
    /// <param name="_">The <see cref="DbFunctions"/> instance.</param>
    /// <param name="source">The date-time value to truncate.</param>
    [DbFunction("toStartOfHour")]
    public static T ToStartOfHour<T>(this DbFunctions _, T source) =>
        throw new InvalidOperationException(CoreStrings.FunctionOnClient(nameof(ToStartOfHour)));

    /// <summary>
    /// Rounds a date with time down to the start of the minute.
    /// Maps to ClickHouse: <c>toStartOfMinute(source)</c>.
    /// </summary>
    /// <param name="_">The <see cref="DbFunctions"/> instance.</param>
    /// <param name="source">The date-time value to truncate.</param>
    [DbFunction("toStartOfMinute")]
    public static T ToStartOfMinute<T>(this DbFunctions _, T source) =>
        throw new InvalidOperationException(CoreStrings.FunctionOnClient(nameof(ToStartOfMinute)));

    /// <summary>
    /// Rounds a date with time down to the start of the second.
    /// Maps to ClickHouse: <c>toStartOfSecond(source)</c>.
    /// </summary>
    /// <remarks>ClickHouse <c>toStartOfSecond</c> requires a <c>DateTime64</c> argument.</remarks>
    /// <param name="_">The <see cref="DbFunctions"/> instance.</param>
    /// <param name="source">The date-time value to truncate. Must map to a ClickHouse <c>DateTime64</c> column.</param>
    [DbFunction("toStartOfSecond")]
    public static T ToStartOfSecond<T>(this DbFunctions _, T source) =>
        throw new InvalidOperationException(CoreStrings.FunctionOnClient(nameof(ToStartOfSecond)));

    /// <summary>
    /// Rounds a date with time down to the start of the five-minute interval.
    /// Maps to ClickHouse: <c>toStartOfFiveMinutes(source)</c>.
    /// </summary>
    /// <param name="_">The <see cref="DbFunctions"/> instance.</param>
    /// <param name="source">The date-time value to truncate.</param>
    [DbFunction("toStartOfFiveMinutes")]
    public static T ToStartOfFiveMinutes<T>(this DbFunctions _, T source) =>
        throw new InvalidOperationException(CoreStrings.FunctionOnClient(nameof(ToStartOfFiveMinutes)));

    /// <summary>
    /// Rounds a date with time down to the start of the ten-minute interval.
    /// Maps to ClickHouse: <c>toStartOfTenMinutes(source)</c>.
    /// </summary>
    /// <param name="_">The <see cref="DbFunctions"/> instance.</param>
    /// <param name="source">The date-time value to truncate.</param>
    [DbFunction("toStartOfTenMinutes")]
    public static T ToStartOfTenMinutes<T>(this DbFunctions _, T source) =>
        throw new InvalidOperationException(CoreStrings.FunctionOnClient(nameof(ToStartOfTenMinutes)));

    /// <summary>
    /// Rounds a date with time down to the start of the fifteen-minute interval.
    /// Maps to ClickHouse: <c>toStartOfFifteenMinutes(source)</c>.
    /// </summary>
    /// <param name="_">The <see cref="DbFunctions"/> instance.</param>
    /// <param name="source">The date-time value to truncate.</param>
    [DbFunction("toStartOfFifteenMinutes")]
    public static T ToStartOfFifteenMinutes<T>(this DbFunctions _, T source) =>
        throw new InvalidOperationException(CoreStrings.FunctionOnClient(nameof(ToStartOfFifteenMinutes)));

    /// <summary>
    /// Rounds a date or date with time down to the start of the specified interval.
    /// Maps to ClickHouse: <c>toStartOfInterval(source, INTERVAL value unit)</c>, emitted as
    /// <c>toStartOfInterval(source, toInterval&lt;unit&gt;(value))</c>.
    /// </summary>
    /// <param name="_">The <see cref="DbFunctions"/> instance.</param>
    /// <param name="source">The date or date-time value to truncate.</param>
    /// <param name="value">The number of interval units in each bucket.</param>
    /// <param name="unit">The interval unit. Must be a constant so it can be translated to SQL.</param>
    [DbFunction("toStartOfInterval")]
    public static T ToStartOfInterval<T>(this DbFunctions _, T source, int value, ClickHouseInterval unit) =>
        throw new InvalidOperationException(CoreStrings.FunctionOnClient(nameof(ToStartOfInterval)));
}
