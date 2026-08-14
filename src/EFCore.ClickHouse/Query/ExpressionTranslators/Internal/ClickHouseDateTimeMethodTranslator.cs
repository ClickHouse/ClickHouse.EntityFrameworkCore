using System.Reflection;
using ClickHouse.EntityFrameworkCore.Metadata;
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
/// <see cref="DateTime"/>, <see cref="DateTimeOffset"/> and <see cref="DateOnly"/>.
/// </summary>
public class ClickHouseDateTimeMethodTranslator : IMethodCallTranslator
{
    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    /// <summary>
    /// <c>Add*</c> methods that take an <see cref="int"/>, keyed to their ClickHouse function. An
    /// integer count needs no rounding, so these always translate.
    /// </summary>
    private static readonly Dictionary<MethodInfo, string> IntegralAddMethods = [];

    /// <summary>
    /// <c>Add*</c> methods that take a <see cref="double"/>, keyed to their ClickHouse function and
    /// the tick length of the method's own unit. Keeping the two families in separate dictionaries
    /// makes a unit with no fixed tick length (a month, a year) unrepresentable here.
    /// </summary>
    private static readonly Dictionary<MethodInfo, (string Function, long TicksPerUnit)> FractionalAddMethods = [];

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

        // DateOnly declares no time-based Add* method, and its AddDays takes an int.
        RegisterAddMethods(typeof(DateOnly), hasTimeComponents: false);

        // DateTimeOffset is deliberately absent: the provider has no DateTimeOffset store mapping yet,
        // so such a property resolves to String and these functions would either fail on the server or
        // silently drop the offset. Add it here together with the mapping (issue #53).
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

        FractionalAddMethods.Add(Method(type, nameof(DateTime.AddDays), typeof(double)), ("addDays", TimeSpan.TicksPerDay));
        FractionalAddMethods.Add(Method(type, nameof(DateTime.AddHours), typeof(double)), ("addHours", TimeSpan.TicksPerHour));
        FractionalAddMethods.Add(Method(type, nameof(DateTime.AddMinutes), typeof(double)), ("addMinutes", TimeSpan.TicksPerMinute));
        FractionalAddMethods.Add(Method(type, nameof(DateTime.AddSeconds), typeof(double)), ("addSeconds", TimeSpan.TicksPerSecond));
        FractionalAddMethods.Add(Method(type, nameof(DateTime.AddMilliseconds), typeof(double)), ("addMilliseconds", TimeSpan.TicksPerMillisecond));
    }

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
            if (IntegralAddMethods.TryGetValue(method, out var integralFunction))
            {
                return AddFunction(integralFunction, instance, arguments[0], method.ReturnType);
            }

            if (FractionalAddMethods.TryGetValue(method, out var fractional))
            {
                return TranslateFractionalAdd(
                    instance, fractional.Function, fractional.TicksPerUnit, arguments[0], method.ReturnType);
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
    /// .NET scales the argument to whole ticks and rounds half away from zero, so
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
        SqlExpression value,
        Type returnType)
    {
        if (value is not SqlConstantExpression { Value: double constantValue })
        {
            return null;
        }

        if (TicksFor(constantValue, ticksPerUnit) is not { } ticks)
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
    /// The tick count that .NET would add, or <see langword="null" /> when .NET would not produce one.
    /// </summary>
    /// <remarks>
    /// Mirrors <see cref="DateTime.Add(TimeSpan)"/>'s scaling: multiply by the unit's tick length, then
    /// round half away from zero. A value that <see cref="DateTime"/> cannot represent is rejected, so
    /// the <see cref="ArgumentOutOfRangeException"/> comes from .NET during client evaluation instead of
    /// from a wrapped Int64 on the server, which ClickHouse reports as a decimal overflow — or, for the
    /// larger magnitudes, does not report at all.
    /// </remarks>
    private static long? TicksFor(double value, long ticksPerUnit)
    {
        if (double.IsNaN(value) || double.IsInfinity(value))
        {
            return null;
        }

        var scaled = value * ticksPerUnit + (value >= 0 ? 0.5 : -0.5);

        return double.Abs(scaled) > DateTime.MaxValue.Ticks ? null : (long)scaled;
    }

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
