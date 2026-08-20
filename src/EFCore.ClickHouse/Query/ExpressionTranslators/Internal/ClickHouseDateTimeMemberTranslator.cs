using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace ClickHouse.EntityFrameworkCore.Query.ExpressionTranslators.Internal;

/// <summary>
/// Translates the standard date/time members of <see cref="DateTime"/>, <see cref="DateTimeOffset"/>
/// and <see cref="DateOnly"/> to ClickHouse functions.
/// </summary>
/// <remarks>
/// <para>
/// One class serves all three CLR types, because the ClickHouse function is the same for each: the
/// <c>to*</c> extraction functions accept <c>Date</c>, <c>Date32</c>, <c>DateTime</c> and
/// <c>DateTime64</c> alike. <see cref="RegisterInstanceMembers"/> registers the members that the
/// given type declares, so <see cref="DateOnly"/> gets the date components only.
/// </para>
/// <para>
/// For <see cref="DateTimeOffset"/> the result is in the timezone the column declares. The default
/// mapping pins that timezone to UTC; an explicitly configured named or fixed-offset timezone is
/// preserved instead. In either case, a materialized value carries the same declared-zone offset, so
/// its components agree with the server result.
/// </para>
/// </remarks>
public class ClickHouseDateTimeMemberTranslator : IMemberTranslator
{
    /// <summary>
    /// Week mode 2 makes <c>toDayOfWeek</c> agree with <see cref="DayOfWeek"/> exactly: Sunday is 0
    /// through to Saturday is 6. The default mode 0 starts the week on Monday, so the argument is
    /// required and no arithmetic correction is needed.
    /// </summary>
    private const byte SundayFirstWeekMode = 2;

    /// <summary>
    /// One <see cref="TimeSpan"/> tick is 100 ns, which is <c>Time64</c> precision 7, and one .NET
    /// tick is also the resolution of <c>now64(7)</c>. Asking for that precision keeps
    /// <see cref="DateTime.TimeOfDay"/> exact, whereas <c>toTime</c> drops the fraction.
    /// </summary>
    private const int TickPrecision = 7;

    /// <summary>Members that map to a ClickHouse function taking the source value alone.</summary>
    private static readonly Dictionary<MemberInfo, string> ComponentFunctions = [];

    private static readonly HashSet<MemberInfo> DayOfWeekMembers = [];
    private static readonly HashSet<MemberInfo> DateMembers = [];
    private static readonly HashSet<MemberInfo> TimeOfDayMembers = [];

    /// <summary>Static members that read the server clock.</summary>
    private static readonly Dictionary<MemberInfo, ServerClock> ServerClockMembers = [];

    private readonly ISqlExpressionFactory _sqlExpressionFactory;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    /// <summary>How a server-clock member is built: a function name, and whether it pins UTC.</summary>
    private enum ServerClock
    {
        /// <summary><c>now64(7, 'UTC')</c> — an exact instant, independent of server settings.</summary>
        UtcNow,

        /// <summary><c>now64(7)</c> — the server's local clock, in its configured timezone.</summary>
        LocalNow,

        /// <summary><c>toStartOfDay(now())</c> — midnight today on the server's local clock.</summary>
        LocalToday
    }

    /// <summary>
    /// The mapping given to a <see cref="DayOfWeek"/> result. Built once, because it composes a
    /// converter over the Int32 mapping and nothing about it varies per translation.
    /// </summary>
    private readonly RelationalTypeMapping? _dayOfWeekMapping;

    static ClickHouseDateTimeMemberTranslator()
    {
        RegisterInstanceMembers(typeof(DateTime), hasTimeComponents: true);
        RegisterInstanceMembers(typeof(DateTimeOffset), hasTimeComponents: true);
        RegisterInstanceMembers(typeof(DateOnly), hasTimeComponents: false);

        ServerClockMembers.Add(Property(typeof(DateTime), nameof(DateTime.UtcNow)), ServerClock.UtcNow);
        ServerClockMembers.Add(Property(typeof(DateTime), nameof(DateTime.Now)), ServerClock.LocalNow);
        ServerClockMembers.Add(Property(typeof(DateTime), nameof(DateTime.Today)), ServerClock.LocalToday);

        // UtcNow is an instant and the default DateTimeOffset store type is UTC-pinned.
        // DateTimeOffset.Now deliberately stays untranslated: unlike UtcNow, it exposes the client's
        // current local offset, which a UTC-pinned server value cannot preserve.
        ServerClockMembers.Add(Property(typeof(DateTimeOffset), nameof(DateTimeOffset.UtcNow)), ServerClock.UtcNow);
    }

    public ClickHouseDateTimeMemberTranslator(
        ISqlExpressionFactory sqlExpressionFactory,
        IRelationalTypeMappingSource typeMappingSource)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
        _typeMappingSource = typeMappingSource;

        // DayOfWeek is an enum, and this provider maps a C# enum to a ClickHouse string. That mapping
        // would render the other side of a comparison as 'Sunday' against a number, so the result
        // carries a number-backed enum mapping instead.
        _dayOfWeekMapping = typeMappingSource.FindMapping(typeof(int)) is { } intMapping
            ? (RelationalTypeMapping)intMapping.WithComposedConverter(new EnumToNumberConverter<DayOfWeek, int>())
            : null;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MemberInfo member,
        Type returnType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (instance is null)
        {
            return TranslateServerClock(member, returnType);
        }

        // toYear and friends return UInt8/UInt16, which the provider's integer mappings widen on read.
        if (ComponentFunctions.TryGetValue(member, out var function))
        {
            return _sqlExpressionFactory.Function(
                name: function,
                arguments: [instance],
                nullable: true,
                argumentsPropagateNullability: [true],
                returnType: returnType,
                typeMapping: _typeMappingSource.FindMapping(returnType));
        }

        if (DayOfWeekMembers.Contains(member))
        {
            return _sqlExpressionFactory.Function(
                name: "toDayOfWeek",
                arguments: [instance, _sqlExpressionFactory.Constant(SundayFirstWeekMode)],
                nullable: true,
                // Only the source propagates nullability; the week mode is a constant.
                argumentsPropagateNullability: [true, false],
                returnType: returnType,
                typeMapping: _dayOfWeekMapping);
        }

        // toStartOfDay keeps the timezone of the source, which is what DateTime.Date means for a
        // column: midnight on the same calendar day that the column renders.
        //
        // Note that toStartOfDay returns a DateTime, whose range is 1970-2106. ClickHouse wraps a value
        // outside that window rather than reporting it, so a DateTime64 column holding a date before
        // 1970 reads back wrong unless the session enables
        // enable_extended_results_for_datetime_functions, which widens the result to DateTime64. This
        // matches EF.Functions.ToStartOfDay and is documented alongside it.
        if (DateMembers.Contains(member))
        {
            return _sqlExpressionFactory.Function(
                name: "toStartOfDay",
                arguments: [instance],
                nullable: true,
                argumentsPropagateNullability: [true],
                returnType: returnType,
                // Reuse the source's mapping only when it describes the member's own CLR type. A mapping
                // for a different type cannot be coerced during materialization.
                typeMapping: instance.TypeMapping?.ClrType == returnType
                    ? instance.TypeMapping
                    : _typeMappingSource.FindMapping(returnType));
        }

        if (TimeOfDayMembers.Contains(member))
        {
            return _sqlExpressionFactory.Function(
                name: "toTime64",
                arguments: [instance, _sqlExpressionFactory.Constant(TickPrecision)],
                nullable: true,
                argumentsPropagateNullability: [true, false],
                returnType: returnType,
                typeMapping: _typeMappingSource.FindMapping($"Time64({TickPrecision})"));
        }

        return null;
    }

    private SqlExpression? TranslateServerClock(MemberInfo member, Type returnType)
    {
        if (!ServerClockMembers.TryGetValue(member, out var clock))
        {
            return null;
        }

        var mapping = _typeMappingSource.FindMapping(returnType);

        // DateTime.Today is midnight today. today() returns a Date, whereas the member's type is
        // DateTime, so truncate the clock value instead and keep a DateTime store type.
        if (clock == ServerClock.LocalToday)
        {
            return _sqlExpressionFactory.Function(
                name: "toStartOfDay",
                arguments: [Niladic("now", returnType, mapping)],
                nullable: false,
                argumentsPropagateNullability: [false],
                returnType: returnType,
                typeMapping: mapping);
        }

        List<SqlExpression> arguments = [_sqlExpressionFactory.Constant(TickPrecision)];
        if (clock == ServerClock.UtcNow)
        {
            arguments.Add(_sqlExpressionFactory.Constant("UTC"));
        }

        return _sqlExpressionFactory.Function(
            name: "now64",
            arguments: arguments,
            nullable: false,
            argumentsPropagateNullability: arguments.Select(_ => false),
            returnType: returnType,
            typeMapping: mapping);
    }

    private SqlExpression Niladic(string name, Type returnType, RelationalTypeMapping? typeMapping)
        => _sqlExpressionFactory.Function(
            name: name,
            arguments: [],
            nullable: false,
            argumentsPropagateNullability: [],
            returnType: returnType,
            typeMapping: typeMapping);

    private static void RegisterInstanceMembers(Type type, bool hasTimeComponents)
    {
        ComponentFunctions.Add(Property(type, nameof(DateTime.Year)), "toYear");
        ComponentFunctions.Add(Property(type, nameof(DateTime.Month)), "toMonth");
        ComponentFunctions.Add(Property(type, nameof(DateTime.Day)), "toDayOfMonth");
        ComponentFunctions.Add(Property(type, nameof(DateTime.DayOfYear)), "toDayOfYear");

        DayOfWeekMembers.Add(Property(type, nameof(DateTime.DayOfWeek)));

        if (!hasTimeComponents)
        {
            return;
        }

        ComponentFunctions.Add(Property(type, nameof(DateTime.Hour)), "toHour");
        ComponentFunctions.Add(Property(type, nameof(DateTime.Minute)), "toMinute");
        ComponentFunctions.Add(Property(type, nameof(DateTime.Second)), "toSecond");
        ComponentFunctions.Add(Property(type, nameof(DateTime.Millisecond)), "toMillisecond");

        DateMembers.Add(Property(type, nameof(DateTime.Date)));
        TimeOfDayMembers.Add(Property(type, nameof(DateTime.TimeOfDay)));
    }

    private static MemberInfo Property(Type type, string name)
        => type.GetProperty(name)
           ?? throw new InvalidOperationException($"Property {type.Name}.{name} was not found.");
}
