using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace ClickHouse.EntityFrameworkCore.Query.ExpressionTranslators.Internal;

public class ClickHouseStringMethodTranslator : IMethodCallTranslator, IMemberTranslator
{
    private const string Whitespace = " \t\r\n\f\v";

    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    private static readonly MethodInfo ToLower = typeof(string).GetRuntimeMethod(nameof(string.ToLower), Type.EmptyTypes)!;
    private static readonly MethodInfo ToUpper = typeof(string).GetRuntimeMethod(nameof(string.ToUpper), Type.EmptyTypes)!;
    private static readonly MethodInfo Contains = typeof(string).GetRuntimeMethod(nameof(string.Contains), [typeof(string)])!;
    private static readonly MethodInfo StartsWith = typeof(string).GetRuntimeMethod(nameof(string.StartsWith), [typeof(string)])!;
    private static readonly MethodInfo EndsWith = typeof(string).GetRuntimeMethod(nameof(string.EndsWith), [typeof(string)])!;
    private static readonly MethodInfo IndexOf = typeof(string).GetRuntimeMethod(nameof(string.IndexOf), [typeof(string)])!;
    private static readonly MethodInfo Replace = typeof(string).GetRuntimeMethod(nameof(string.Replace), [typeof(string), typeof(string)])!;
    private static readonly MethodInfo SubstringOneArg = typeof(string).GetRuntimeMethod(nameof(string.Substring), [typeof(int)])!;
    private static readonly MethodInfo SubstringTwoArg = typeof(string).GetRuntimeMethod(nameof(string.Substring), [typeof(int), typeof(int)])!;
    private static readonly MethodInfo IsNullOrEmpty = typeof(string).GetRuntimeMethod(nameof(string.IsNullOrEmpty), [typeof(string)])!;
    private static readonly MethodInfo Trim = typeof(string).GetRuntimeMethod(nameof(string.Trim), Type.EmptyTypes)!;
    private static readonly MethodInfo TrimStart = typeof(string).GetRuntimeMethod(nameof(string.TrimStart), Type.EmptyTypes)!;
    private static readonly MethodInfo TrimEnd = typeof(string).GetRuntimeMethod(nameof(string.TrimEnd), Type.EmptyTypes)!;
    private static readonly MethodInfo TrimChar = typeof(string).GetRuntimeMethod(nameof(string.Trim), [typeof(char)])!;
    private static readonly MethodInfo TrimChars = typeof(string).GetRuntimeMethod(nameof(string.Trim), [typeof(char[])])!;
    private static readonly MethodInfo TrimStartChar = typeof(string).GetRuntimeMethod(nameof(string.TrimStart), [typeof(char)])!;
    private static readonly MethodInfo TrimStartChars = typeof(string).GetRuntimeMethod(nameof(string.TrimStart), [typeof(char[])])!;
    private static readonly MethodInfo TrimEndChar = typeof(string).GetRuntimeMethod(nameof(string.TrimEnd), [typeof(char)])!;
    private static readonly MethodInfo TrimEndChars = typeof(string).GetRuntimeMethod(nameof(string.TrimEnd), [typeof(char[])])!;

    private static readonly MemberInfo LengthMember = typeof(string).GetProperty(nameof(string.Length))!;

    public ClickHouseStringMethodTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (method == ToLower)
            return _sqlExpressionFactory.Function("lowerUTF8", [instance!], true, [true], method.ReturnType, instance!.TypeMapping);

        if (method == ToUpper)
            return _sqlExpressionFactory.Function("upperUTF8", [instance!], true, [true], method.ReturnType, instance!.TypeMapping);

        if (method == Contains)
            return _sqlExpressionFactory.GreaterThan(
                _sqlExpressionFactory.Function("positionUTF8", [instance!, arguments[0]], false, [true, true], typeof(int)),
                _sqlExpressionFactory.Constant(0));

        if (method == StartsWith)
            return _sqlExpressionFactory.Function("startsWith", [instance!, arguments[0]], true, [true, true], method.ReturnType);

        if (method == EndsWith)
            return _sqlExpressionFactory.Function("endsWith", [instance!, arguments[0]], true, [true, true], method.ReturnType);

        // IndexOf: positionUTF8 returns 1-based, .NET is 0-based => subtract 1
        if (method == IndexOf)
            return _sqlExpressionFactory.Subtract(
                _sqlExpressionFactory.Function("positionUTF8", [instance!, arguments[0]], true, [true, true], typeof(int)),
                _sqlExpressionFactory.Constant(1));

        if (method == Replace)
            return _sqlExpressionFactory.Function("replaceAll", [instance!, arguments[0], arguments[1]], true, [true, true, true], method.ReturnType);

        // Substring(startIndex): ClickHouse is 1-based, add 1
        if (method == SubstringOneArg)
            return _sqlExpressionFactory.Function("substring",
                [instance!, _sqlExpressionFactory.Add(arguments[0], _sqlExpressionFactory.Constant(1))],
                true, [true, true], method.ReturnType);

        // Substring(startIndex, length): ClickHouse is 1-based, add 1
        if (method == SubstringTwoArg)
            return _sqlExpressionFactory.Function("substring",
                [instance!, _sqlExpressionFactory.Add(arguments[0], _sqlExpressionFactory.Constant(1)), arguments[1]],
                true, [true, true, true], method.ReturnType);

        if (method == IsNullOrEmpty)
            return _sqlExpressionFactory.OrElse(
                _sqlExpressionFactory.IsNull(arguments[0]),
                _sqlExpressionFactory.Function("empty", [arguments[0]], false, [true], method.ReturnType));

        if (method == Trim || method == TrimStart || method == TrimEnd
            || method == TrimChar || method == TrimChars
            || method == TrimStartChar || method == TrimStartChars
            || method == TrimEndChar || method == TrimEndChars)
        {
            if (arguments.Count == 0)
            {
                return _sqlExpressionFactory.Function(
                    method == TrimStart ? "trimLeft" : method == TrimEnd ? "trimRight" : "trimBoth",
                    [instance!],
                    nullable: true,
                    argumentsPropagateNullability: [true],
                    method.ReturnType);
            }

            var trimChars = GetConstantTrimChars(arguments[0]);
            if (trimChars is null)
            {
                return null;
            }

            return _sqlExpressionFactory.Function(
                method == TrimStartChar || method == TrimStartChars ? "trimLeft"
                    : method == TrimEndChar || method == TrimEndChars ? "trimRight"
                    : "trimBoth",
                [
                    instance!,
                    trimChars.Length == 0
                        ? _sqlExpressionFactory.Constant(Whitespace)
                        : _sqlExpressionFactory.Constant(new string(trimChars))
                ],
                nullable: true,
                argumentsPropagateNullability: [true, true],
                method.ReturnType,
                instance!.TypeMapping);
        }

        return null;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MemberInfo member,
        Type returnType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (member == LengthMember)
            return _sqlExpressionFactory.Function("char_length", [instance!], true, [true], returnType);

        return null;
    }

    private static char[]? GetConstantTrimChars(SqlExpression argument)
    {
        if (argument is not SqlConstantExpression constant)
            return null;

        return constant.Value switch
        {
            char c => [c],
            char[] chars => chars,
            null => [],
            _ => null
        };
    }
}
