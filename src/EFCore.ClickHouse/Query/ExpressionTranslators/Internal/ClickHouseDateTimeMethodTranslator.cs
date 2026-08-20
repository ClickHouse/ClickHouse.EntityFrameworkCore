using System.Reflection;
using ClickHouse.EntityFrameworkCore.Metadata;
using ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using DateTimeDbFunctions = Microsoft.EntityFrameworkCore.ClickHouseDateTimeDbFunctionsExtensions;

namespace ClickHouse.EntityFrameworkCore.Query.ExpressionTranslators.Internal;

/// <summary>
/// Translates date/time method calls to ClickHouse SQL functions: the
/// <c>EF.Functions.ToStartOf*</c> extension methods
/// (<see cref="ClickHouseDateTimeDbFunctionsExtensions"/>), and the standard <c>Add*</c> methods of
/// <see cref="DateTime"/>, <see cref="DateTimeOffset"/> and <see cref="DateOnly"/>. DateTimeOffset
/// addition is limited to UTC and fixed-offset mappings so daylight-saving calendar rules cannot
/// change .NET's offset-preserving semantics.
/// </summary>
public class ClickHouseDateTimeMethodTranslator : IMethodCallTranslator
{
    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    /// <summary>
    /// <c>Add*</c> methods that take an <see cref="int"/>, keyed to their ClickHouse function. An
    /// integer count needs no precision check. <see cref="DateTimeOffset"/> calls still require a
    /// fixed-offset source mapping so daylight-saving rules cannot change their semantics.
    /// </summary>
    private static readonly Dictionary<MethodInfo, string> IntegralAddMethods = [];

    /// <summary>
    /// <c>Add*</c> methods that take a <see cref="double"/>, keyed to their ClickHouse function and
    /// the tick length of the method's own unit. Keeping the two families in separate dictionaries
    /// makes a unit with no fixed tick length (a month, a year) unrepresentable here.
    /// </summary>
    private static readonly Dictionary<MethodInfo, (string Function, long TicksPerUnit, long MaxUnitCount)> FractionalAddMethods = [];

    private enum TickConversionResult
    {
        Success,
        NonConstant,
        OutOfRange
    }

    /// <summary>
    /// Maps the generic method definitions that take only the source value (and, for
    /// <c>ToStartOfWeek(source, mode)</c>, an extra scalar argument) directly to a ClickHouse function name.
    /// <see cref="ClickHouseDateTimeDbFunctionsExtensions.ToStartOfInterval{T}"/> is handled separately.
    /// </summary>
    private static readonly Dictionary<MethodInfo, string> SupportedMethods;

    /// <summary>Maps <see cref="ClickHouseInterval"/> values to the ClickHouse <c>toInterval*</c> helper function.</summary>
    private static readonly Dictionary<ClickHouseInterval, string> IntervalFunctions = new()
    {
        [ClickHouseInterval.Second] = "toIntervalSecond",
        [ClickHouseInterval.Minute] = "toIntervalMinute",
        [ClickHouseInterval.Hour] = "toIntervalHour",
        [ClickHouseInterval.Day] = "toIntervalDay",
        [ClickHouseInterval.Week] = "toIntervalWeek",
        [ClickHouseInterval.Month] = "toIntervalMonth",
        [ClickHouseInterval.Quarter] = "toIntervalQuarter",
        [ClickHouseInterval.Year] = "toIntervalYear",
    };

    private static readonly MethodInfo ToStartOfWeekWithModeMethod;
    private static readonly MethodInfo ToStartOfIntervalMethod;

    static ClickHouseDateTimeMethodTranslator()
    {
        var type = typeof(DateTimeDbFunctions);
        SupportedMethods = new Dictionary<MethodInfo, string>();

        // Source-only methods: (DbFunctions, T) -> functionName
        void RegisterSourceOnly(string methodName, string sqlFunction)
        {
            var method = type.GetMethods().FirstOrDefault(m =>
            {
                if (m.Name != methodName || !m.IsGenericMethod)
                {
                    return false;
                }

                var parameters = m.GetParameters();
                return parameters.Length == 2
                       && parameters[0].ParameterType == typeof(DbFunctions)
                       && parameters[1].ParameterType.IsGenericParameter;
            }) ?? throw new InvalidOperationException($"Method {methodName} with strict signature not found.");

            SupportedMethods.Add(method, sqlFunction);
        }

        RegisterSourceOnly(nameof(DateTimeDbFunctions.ToStartOfYear), "toStartOfYear");
        RegisterSourceOnly(nameof(DateTimeDbFunctions.ToStartOfQuarter), "toStartOfQuarter");
        RegisterSourceOnly(nameof(DateTimeDbFunctions.ToStartOfMonth), "toStartOfMonth");
        RegisterSourceOnly(nameof(DateTimeDbFunctions.ToStartOfWeek), "toStartOfWeek");
        RegisterSourceOnly(nameof(DateTimeDbFunctions.ToStartOfDay), "toStartOfDay");
        RegisterSourceOnly(nameof(DateTimeDbFunctions.ToStartOfHour), "toStartOfHour");
        RegisterSourceOnly(nameof(DateTimeDbFunctions.ToStartOfMinute), "toStartOfMinute");
        RegisterSourceOnly(nameof(DateTimeDbFunctions.ToStartOfSecond), "toStartOfSecond");
        RegisterSourceOnly(nameof(DateTimeDbFunctions.ToStartOfFiveMinutes), "toStartOfFiveMinutes");
        RegisterSourceOnly(nameof(DateTimeDbFunctions.ToStartOfTenMinutes), "toStartOfTenMinutes");
        RegisterSourceOnly(nameof(DateTimeDbFunctions.ToStartOfFifteenMinutes), "toStartOfFifteenMinutes");

        // ToStartOfWeek(source, byte mode) -> toStartOfWeek
        ToStartOfWeekWithModeMethod = type.GetMethods().FirstOrDefault(m =>
        {
            if (m.Name != nameof(DateTimeDbFunctions.ToStartOfWeek) || !m.IsGenericMethod)
            {
                return false;
            }

            var parameters = m.GetParameters();
            return parameters.Length == 3
                   && parameters[0].ParameterType == typeof(DbFunctions)
                   && parameters[1].ParameterType.IsGenericParameter
                   && parameters[2].ParameterType == typeof(byte);
        }) ?? throw new InvalidOperationException("Method ToStartOfWeek(source, mode) with strict signature not found.");
        SupportedMethods.Add(ToStartOfWeekWithModeMethod, "toStartOfWeek");

        ToStartOfIntervalMethod = type.GetMethods().FirstOrDefault(m =>
        {
            if (m.Name != nameof(DateTimeDbFunctions.ToStartOfInterval) || !m.IsGenericMethod)
            {
                return false;
            }

            var parameters = m.GetParameters();
            return parameters.Length == 4
                   && parameters[0].ParameterType == typeof(DbFunctions)
                   && parameters[1].ParameterType.IsGenericParameter
                   && parameters[2].ParameterType == typeof(int)
                   && parameters[3].ParameterType == typeof(ClickHouseInterval);
        }) ?? throw new InvalidOperationException("Method ToStartOfInterval with strict signature not found.");

        RegisterAddMethods(typeof(DateTime), hasTimeComponents: true);
        RegisterAddMethods(typeof(DateTimeOffset), hasTimeComponents: true);

        // DateOnly declares no time-based Add* method, and its AddDays takes an int.
        RegisterAddMethods(typeof(DateOnly), hasTimeComponents: false);
    }

    /// <summary>
    /// Registers the <c>Add*</c> methods that <paramref name="type"/> declares.
    /// </summary>
    /// <remarks>
    /// <c>AddYears</c> and <c>AddMonths</c> take an <see cref="int"/> on every supported type, so they
    /// map straight onto <c>addYears</c>/<c>addMonths</c>. The time-based methods take a
    /// <see cref="double"/> on <see cref="DateTime"/>, which needs the exactness check that
    /// <see cref="TranslateFractionalAdd"/> applies. On <see cref="DateOnly"/>, <c>AddDays</c> takes an
    /// <see cref="int"/> instead, so it is registered as integral.
    /// </remarks>
    private static void RegisterAddMethods(Type type, bool hasTimeComponents)
    {
        IntegralAddMethods.Add(Method(type, nameof(DateTime.AddYears), typeof(int)), "addYears");
        IntegralAddMethods.Add(Method(type, nameof(DateTime.AddMonths), typeof(int)), "addMonths");

        if (!hasTimeComponents)
        {
            IntegralAddMethods.Add(Method(type, nameof(DateOnly.AddDays), typeof(int)), "addDays");
            return;
        }

        RegisterFractionalAdd(type, nameof(DateTime.AddDays), "addDays", TimeSpan.TicksPerDay);
        RegisterFractionalAdd(type, nameof(DateTime.AddHours), "addHours", TimeSpan.TicksPerHour);
        RegisterFractionalAdd(type, nameof(DateTime.AddMinutes), "addMinutes", TimeSpan.TicksPerMinute);
        RegisterFractionalAdd(type, nameof(DateTime.AddSeconds), "addSeconds", TimeSpan.TicksPerSecond);
        RegisterFractionalAdd(type, nameof(DateTime.AddMilliseconds), "addMilliseconds", TimeSpan.TicksPerMillisecond);
    }

    private static void RegisterFractionalAdd(Type type, string methodName, string function, long ticksPerUnit)
        => FractionalAddMethods.Add(
            Method(type, methodName, typeof(double)),
            (function, ticksPerUnit, DateTime.MaxValue.Ticks / ticksPerUnit));

    private static MethodInfo Method(Type type, string name, Type argumentType)
        => type.GetRuntimeMethod(name, [argumentType])
           ?? throw new InvalidOperationException($"Method {type.Name}.{name}({argumentType.Name}) was not found.");

    public ClickHouseDateTimeMethodTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (instance is not null)
        {
            if (IsAddMethod(method) && !CanTranslateDateTimeOffsetAdd(method, instance))
            {
                return null;
            }

            if (IntegralAddMethods.TryGetValue(method, out var integralFunction))
            {
                return AddFunction(integralFunction, instance, arguments[0], method.ReturnType);
            }

            if (FractionalAddMethods.TryGetValue(method, out var fractional))
            {
                return TranslateFractionalAdd(
                    instance,
                    fractional.Function,
                    fractional.TicksPerUnit,
                    fractional.MaxUnitCount,
                    arguments[0],
                    method.ReturnType);
            }
        }

        var genericMethod = method.IsGenericMethod ? method.GetGenericMethodDefinition() : method;

        if (SupportedMethods.TryGetValue(genericMethod, out var function))
        {
            // ClickHouse requires the optional week mode to be constant for the query. SQL literals and
            // parameters both satisfy that requirement, but row-dependent expressions (such as columns) do not.
            if (genericMethod == ToStartOfWeekWithModeMethod && !IsQueryConstant(arguments[2]))
            {
                throw QueryConstantRequired(nameof(DateTimeDbFunctions.ToStartOfWeek), "mode");
            }

            // arguments[0] is the DbFunctions receiver; the source is arguments[1].
            var sqlArguments = arguments.Skip(1).ToList();
            var source = sqlArguments[0];

            return _sqlExpressionFactory.Function(
                name: function,
                arguments: sqlArguments,
                nullable: true,
                // Only the source value propagates nullability; the optional week mode is a constant.
                argumentsPropagateNullability: sqlArguments.Select((_, i) => i == 0),
                returnType: method.ReturnType,
                typeMapping: source.TypeMapping);
        }

        if (genericMethod == ToStartOfIntervalMethod)
        {
            var source = arguments[1];
            var value = arguments[2];

            // ClickHouse requires the interval value to be constant for the query. The unit is stricter: it must
            // be a SQL literal because its value selects the toInterval* function emitted into the SQL tree.
            if (!IsQueryConstant(value))
            {
                throw QueryConstantRequired(nameof(DateTimeDbFunctions.ToStartOfInterval), "value");
            }

            if (arguments[3] is not SqlConstantExpression { Value: ClickHouseInterval unit }
                || !IntervalFunctions.TryGetValue(unit, out var intervalFunction))
            {
                return null;
            }

            var interval = _sqlExpressionFactory.Function(
                name: intervalFunction,
                arguments: [value],
                nullable: true,
                argumentsPropagateNullability: [false],
                returnType: value.Type);

            return _sqlExpressionFactory.Function(
                name: "toStartOfInterval",
                arguments: [source, interval],
                nullable: true,
                argumentsPropagateNullability: [true, false],
                returnType: method.ReturnType,
                typeMapping: source.TypeMapping);
        }

        return null;
    }

    /// <summary>
    /// Translates one <c>Add*</c> call whose .NET argument is a <see cref="double"/>, or returns
    /// <see langword="null" /> when no exact translation exists.
    /// </summary>
    /// <remarks>
    /// <para>
    /// .NET separates the integral and fractional parts, scales each to ticks, and truncates any
    /// fractional tick toward zero. Consequently,
    /// <c>AddSeconds(0.1234567)</c> adds exactly 1 234 567 ticks. The matching ClickHouse function takes
    /// a whole number of its own unit and discards the rest, so <c>addDays(x, 1.5)</c> would add one day.
    /// The two agree only when the tick count divides exactly into the function's unit.
    /// </para>
    /// <para>
    /// A constant is therefore folded to ticks here and then expressed in the coarsest unit that holds
    /// it exactly. The natural unit is preferred, and not only for readability: it keeps the store type
    /// of the source, and <c>addMilliseconds</c> rejects a <c>Date</c> or <c>Date32</c> source outright
    /// (<c>ILLEGAL_TYPE_OF_ARGUMENT</c>).
    /// </para>
    /// <para>
    /// Everything else is left untranslated on purpose, rather than rounded to fit. This covers a
    /// sub-millisecond offset and any value that is not a constant. Milliseconds are as fine as this
    /// goes: <c>addNanoseconds</c> would express a tick exactly but promotes the result to
    /// <c>DateTime64(9)</c>, whose Int64 nanosecond count cannot span the <c>DateTime64</c> range, which
    /// would trade a rounding error for a silently wrong date. An untranslated call still gives the
    /// correct .NET value through client evaluation in a projection, and reports a clear reason in a
    /// predicate.
    /// </para>
    /// </remarks>
    private SqlExpression? TranslateFractionalAdd(
        SqlExpression instance,
        string function,
        long ticksPerUnit,
        long maxUnitCount,
        SqlExpression value,
        Type returnType)
    {
        if (TryGetTicks(value, ticksPerUnit, maxUnitCount, out var ticks) != TickConversionResult.Success)
        {
            return null;
        }

        if (ticks % ticksPerUnit == 0)
        {
            return AddFunction(function, instance, _sqlExpressionFactory.Constant(ticks / ticksPerUnit), returnType);
        }

        if (ticks % TimeSpan.TicksPerMillisecond != 0)
        {
            return null;
        }

        return AddFunction(
            "addMilliseconds",
            instance,
            _sqlExpressionFactory.Constant(ticks / TimeSpan.TicksPerMillisecond),
            returnType);
    }

    /// <summary>
    /// Computes the tick count that .NET would add and reports why it cannot be folded when necessary.
    /// </summary>
    /// <remarks>
    /// Mirrors .NET 10's <c>DateTime.AddUnits</c> implementation: reject values outside that method's
    /// per-unit bound, split the integral and fractional parts, and truncate fractional ticks toward
    /// zero. A value that <see cref="DateTime"/> cannot represent is rejected, so
    /// the <see cref="ArgumentOutOfRangeException"/> comes from .NET during client evaluation instead of
    /// from a wrapped Int64 on the server, which ClickHouse reports as a decimal overflow — or, for the
    /// larger magnitudes, does not report at all.
    /// </remarks>
    private static TickConversionResult TryGetTicks(
        SqlExpression value,
        long ticksPerUnit,
        long maxUnitCount,
        out long ticks)
    {
        ticks = default;

        if (value is not SqlConstantExpression { Value: double constantValue })
        {
            return TickConversionResult.NonConstant;
        }

        if (!double.IsFinite(constantValue) || Math.Abs(constantValue) > maxUnitCount)
        {
            return TickConversionResult.OutOfRange;
        }

        var integralPart = Math.Truncate(constantValue);
        var fractionalPart = constantValue - integralPart;
        ticks = (long)integralPart * ticksPerUnit;
        ticks += (long)(fractionalPart * ticksPerUnit);

        return TickConversionResult.Success;
    }

    internal static bool IsAddMethod(MethodInfo method)
        => IntegralAddMethods.ContainsKey(method) || FractionalAddMethods.ContainsKey(method);

    /// <summary>
    /// Returns a provider-specific explanation when a recognized <c>Add*</c> method was deliberately
    /// left untranslated.
    /// </summary>
    internal static string? GetUnsupportedAddTranslationErrorDetails(
        MethodInfo method,
        SqlExpression instance,
        SqlExpression value)
    {
        if (!IsAddMethod(method))
        {
            return null;
        }

        var displayName = $"{method.DeclaringType?.Name}.{method.Name}";

        if (!CanTranslateDateTimeOffsetAdd(method, instance))
        {
            var timezone = (instance.TypeMapping as ClickHouseDateTimeOffsetTypeMapping)?.Timezone;
            var timezoneDescription = timezone is null ? "no declared timezone" : $"timezone '{timezone}'";

            return $"The '{displayName}' method cannot be translated for a DateTimeOffset column with "
                   + $"{timezoneDescription}. .NET preserves the instance offset, while ClickHouse applies "
                   + "the column timezone's calendar rules and may change the offset across a daylight-saving "
                   + "transition. Use a UTC or Fixed/UTC offset store type, or perform the addition on the client.";
        }

        if (!FractionalAddMethods.TryGetValue(method, out var fractional))
        {
            return null;
        }

        var tickResult = TryGetTicks(value, fractional.TicksPerUnit, fractional.MaxUnitCount, out var ticks);
        if (tickResult == TickConversionResult.NonConstant)
        {
            return $"The '{displayName}' argument must be a constant so its exact .NET tick count can be "
                   + "checked before translating it to ClickHouse. Perform the addition on the client when "
                   + "the offset is row-dependent or parameterized.";
        }

        if (tickResult == TickConversionResult.OutOfRange)
        {
            return $"The '{displayName}' argument is outside the range that .NET accepts for that unit, so "
                   + "it cannot be translated safely. Let .NET evaluate the call to preserve its "
                   + "ArgumentOutOfRangeException.";
        }

        if (ticks % fractional.TicksPerUnit != 0
            && ticks % TimeSpan.TicksPerMillisecond != 0)
        {
            return $"The '{displayName}' argument produces a sub-millisecond tick offset that ClickHouse "
                   + "cannot represent exactly without narrowing the supported DateTime64 range. Only exact "
                   + "whole-unit or whole-millisecond offsets are translated.";
        }

        return null;
    }

    private static bool CanTranslateDateTimeOffsetAdd(MethodInfo method, SqlExpression instance)
        => method.DeclaringType != typeof(DateTimeOffset)
           || instance.TypeMapping is ClickHouseDateTimeOffsetTypeMapping { HasFixedOffset: true };

    private SqlExpression AddFunction(string function, SqlExpression instance, SqlExpression value, Type returnType)
        => _sqlExpressionFactory.Function(
            name: function,
            arguments: [instance, value],
            nullable: true,
            argumentsPropagateNullability: [true, true],
            returnType: returnType,
            typeMapping: instance.TypeMapping);

    private static bool IsQueryConstant(SqlExpression expression)
        => expression is SqlConstantExpression or SqlParameterExpression;

    private static InvalidOperationException QueryConstantRequired(string method, string argument)
        => new(
            $"The '{method}' method's '{argument}' argument must be a SQL literal or query parameter because " +
            "ClickHouse requires it to be constant for the query. Row-dependent expressions cannot be translated.");
}
