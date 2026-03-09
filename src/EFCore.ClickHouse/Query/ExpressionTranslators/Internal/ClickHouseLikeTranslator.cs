using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace ClickHouse.EntityFrameworkCore.Query.ExpressionTranslators.Internal;

public class ClickHouseLikeTranslator : IMethodCallTranslator
{
    private static readonly MethodInfo LikeMethodInfo = typeof(DbFunctionsExtensions)
        .GetRuntimeMethod(nameof(DbFunctionsExtensions.Like), [typeof(DbFunctions), typeof(string), typeof(string)])!;

    private static readonly MethodInfo LikeWithEscapeMethodInfo = typeof(DbFunctionsExtensions)
        .GetRuntimeMethod(nameof(DbFunctionsExtensions.Like), [typeof(DbFunctions), typeof(string), typeof(string), typeof(string)])!;

    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public ClickHouseLikeTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (method == LikeMethodInfo)
            return _sqlExpressionFactory.Like(arguments[1], arguments[2]);

        if (method == LikeWithEscapeMethodInfo)
            return _sqlExpressionFactory.Like(arguments[1], arguments[2], arguments[3]);

        return null;
    }
}
