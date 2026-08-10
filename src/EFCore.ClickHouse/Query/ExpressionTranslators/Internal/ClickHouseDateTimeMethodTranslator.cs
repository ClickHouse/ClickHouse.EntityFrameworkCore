using System.Reflection;
using ClickHouse.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using DateTimeDbFunctions = Microsoft.EntityFrameworkCore.ClickHouseDateTimeDbFunctionsExtensions;

namespace ClickHouse.EntityFrameworkCore.Query.ExpressionTranslators.Internal;

/// <summary>
/// Translates the <c>EF.Functions.ToStartOf*</c> extension methods
/// (<see cref="ClickHouseDateTimeDbFunctionsExtensions"/>) to their ClickHouse SQL functions.
/// </summary>
public class ClickHouseDateTimeMethodTranslator : IMethodCallTranslator
{
    private readonly ISqlExpressionFactory _sqlExpressionFactory;

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
        var weekWithMode = type.GetMethods().FirstOrDefault(m =>
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
        SupportedMethods.Add(weekWithMode, "toStartOfWeek");

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
    }

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
        var genericMethod = method.IsGenericMethod ? method.GetGenericMethodDefinition() : method;

        if (SupportedMethods.TryGetValue(genericMethod, out var function))
        {
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

            // The interval unit must be a compile-time constant so it can be mapped to a toInterval* function.
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
}
