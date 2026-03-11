using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Query.ExpressionTranslators.Internal;

public class ClickHouseMathMethodTranslator : IMethodCallTranslator
{
    private static readonly Dictionary<MethodInfo, string> SupportedMethods = new()
    {
        { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), [typeof(decimal)])!, "abs" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), [typeof(double)])!, "abs" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), [typeof(float)])!, "abs" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), [typeof(int)])!, "abs" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), [typeof(long)])!, "abs" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), [typeof(short)])!, "abs" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Abs), [typeof(float)])!, "abs" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Ceiling), [typeof(double)])!, "ceiling" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Ceiling), [typeof(decimal)])!, "ceiling" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Ceiling), [typeof(float)])!, "ceiling" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Floor), [typeof(double)])!, "floor" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Floor), [typeof(decimal)])!, "floor" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Floor), [typeof(float)])!, "floor" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Round), [typeof(double)])!, "round" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Round), [typeof(double), typeof(int)])!, "round" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Round), [typeof(decimal)])!, "round" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Round), [typeof(decimal), typeof(int)])!, "round" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Round), [typeof(float)])!, "round" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Round), [typeof(float), typeof(int)])!, "round" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Truncate), [typeof(double)])!, "truncate" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Truncate), [typeof(decimal)])!, "truncate" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Truncate), [typeof(float)])!, "truncate" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Pow), [typeof(double), typeof(double)])!, "pow" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Pow), [typeof(float), typeof(float)])!, "pow" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Sqrt), [typeof(double)])!, "sqrt" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Sqrt), [typeof(float)])!, "sqrt" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Cbrt), [typeof(double)])!, "cbrt" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Exp), [typeof(double)])!, "exp" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Exp), [typeof(float)])!, "exp" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Log), [typeof(double)])!, "log" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Log), [typeof(float)])!, "log" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Log10), [typeof(double)])!, "log10" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Log10), [typeof(float)])!, "log10" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Sign), [typeof(double)])!, "sign" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Sign), [typeof(float)])!, "sign" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Sign), [typeof(int)])!, "sign" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Sign), [typeof(long)])!, "sign" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Sign), [typeof(decimal)])!, "sign" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Sign), [typeof(short)])!, "sign" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Sign), [typeof(float)])!, "sign" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Sin), [typeof(double)])!, "sin" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Sin), [typeof(float)])!, "sin" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Cos), [typeof(double)])!, "cos" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Cos), [typeof(float)])!, "cos" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Tan), [typeof(double)])!, "tan" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Tan), [typeof(float)])!, "tan" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Asin), [typeof(double)])!, "asin" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Asin), [typeof(float)])!, "asin" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Acos), [typeof(double)])!, "acos" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Acos), [typeof(float)])!, "acos" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Atan), [typeof(double)])!, "atan" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Atan), [typeof(float)])!, "atan" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Atan2), [typeof(double), typeof(double)])!, "atan2" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Atan2), [typeof(float), typeof(float)])!, "atan2" },
        { typeof(double).GetRuntimeMethod(nameof(double.RadiansToDegrees), [typeof(double)])!, "degrees" },
        { typeof(float).GetRuntimeMethod(nameof(float.RadiansToDegrees), [typeof(float)])!, "degrees" },
        { typeof(double).GetRuntimeMethod(nameof(double.DegreesToRadians), [typeof(double)])!, "radians" },
        { typeof(float).GetRuntimeMethod(nameof(float.DegreesToRadians), [typeof(float)])!, "radians" },
    };

    private readonly ISqlExpressionFactory _sqlExpressionFactory;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public ClickHouseMathMethodTranslator(
        ISqlExpressionFactory sqlExpressionFactory,
        IRelationalTypeMappingSource typeMappingSource)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
        _typeMappingSource = typeMappingSource;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (SupportedMethods.TryGetValue(method, out var functionName))
        {
            return _sqlExpressionFactory.Function(
                functionName,
                arguments,
                nullable: false,
                argumentsPropagateNullability: Enumerable.Repeat(true, arguments.Count),
                method.ReturnType);
        }

        // Math.Log(x, newBase) → log(x) / log(newBase)
        if ((method.DeclaringType == typeof(Math) || method.DeclaringType == typeof(MathF))
            && method.Name == nameof(Math.Log)
            && arguments.Count == 2)
        {
            return _sqlExpressionFactory.Divide(
                _sqlExpressionFactory.Function(
                    "log",
                    [arguments[0]],
                    nullable: true,
                    argumentsPropagateNullability: [true],
                    method.ReturnType),
                _sqlExpressionFactory.Function(
                    "log",
                    [arguments[1]],
                    nullable: true,
                    argumentsPropagateNullability: [true],
                    method.ReturnType));
        }

        // double/float.IsNegativeInfinity → isInfinite(x) AND x < 0
        if ((method.DeclaringType == typeof(double) && method.Name == nameof(double.IsNegativeInfinity))
            || (method.DeclaringType == typeof(float) && method.Name == nameof(float.IsNegativeInfinity)))
        {
            var zeroConstant = method.DeclaringType == typeof(float)
                ? _sqlExpressionFactory.Constant(0f, _typeMappingSource.FindMapping(typeof(float)))
                : _sqlExpressionFactory.Constant(0d, _typeMappingSource.FindMapping(typeof(double)));

            return _sqlExpressionFactory.AndAlso(
                _sqlExpressionFactory.Function(
                    "isInfinite",
                    arguments,
                    nullable: false,
                    argumentsPropagateNullability: [true],
                    typeof(bool),
                    _typeMappingSource.FindMapping(typeof(bool))),
                _sqlExpressionFactory.LessThan(arguments[0], zeroConstant));
        }

        // double/float.IsPositiveInfinity → isInfinite(x) AND x > 0
        if ((method.DeclaringType == typeof(double) && method.Name == nameof(double.IsPositiveInfinity))
            || (method.DeclaringType == typeof(float) && method.Name == nameof(float.IsPositiveInfinity)))
        {
            var zeroConstant = method.DeclaringType == typeof(float)
                ? _sqlExpressionFactory.Constant(0f, _typeMappingSource.FindMapping(typeof(float)))
                : _sqlExpressionFactory.Constant(0d, _typeMappingSource.FindMapping(typeof(double)));

            return _sqlExpressionFactory.AndAlso(
                _sqlExpressionFactory.Function(
                    "isInfinite",
                    arguments,
                    nullable: false,
                    argumentsPropagateNullability: [true],
                    typeof(bool),
                    _typeMappingSource.FindMapping(typeof(bool))),
                _sqlExpressionFactory.GreaterThan(arguments[0], zeroConstant));
        }

        return null;
    }
}
