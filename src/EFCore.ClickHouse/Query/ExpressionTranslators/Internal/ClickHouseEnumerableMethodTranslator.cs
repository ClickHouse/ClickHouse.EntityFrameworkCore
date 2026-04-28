using System.Reflection;
using ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace ClickHouse.EntityFrameworkCore.Query.ExpressionTranslators.Internal;

public class ClickHouseEnumerableMethodTranslator : IMethodCallTranslator
{
    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    private static readonly MethodInfo EnumerableContains = typeof(Enumerable).GetRuntimeMethods()
        .First(m => m.Name == nameof(Enumerable.Contains) && m.GetParameters().Length == 2);

    private static readonly MethodInfo QueryableContains = typeof(Queryable).GetRuntimeMethods()
        .First(m => m.Name == nameof(Queryable.Contains) && m.GetParameters().Length == 2);

    private static readonly MethodInfo AsQueryableMethod = typeof(Queryable).GetRuntimeMethods()
        .First(m => m.Name == nameof(Queryable.AsQueryable) && m.IsGenericMethod);

    private static readonly MethodInfo AsEnumerableMethod = typeof(Enumerable).GetRuntimeMethods()
        .First(m => m.Name == nameof(Enumerable.AsEnumerable) && m.IsGenericMethod);

    public ClickHouseEnumerableMethodTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (method.Name == nameof(Queryable.AsQueryable)
            || (method.DeclaringType == typeof(Enumerable) && method.Name == nameof(Enumerable.AsEnumerable)))
        {
            return arguments[0];
        }

        SqlExpression? source = null;
        SqlExpression? item = null;

        if (method.IsGenericMethod)
        {
            var genericMethodDefinition = method.GetGenericMethodDefinition();
            if (genericMethodDefinition == EnumerableContains || genericMethodDefinition == QueryableContains)
            {
                source = arguments[0];
                item = arguments[1];
            }
        }
        else if (instance != null
                 && method.Name == nameof(Enumerable.Contains)
                 && arguments.Count == 1)
        {
            var declaringType = method.DeclaringType;
            if (declaringType?.IsGenericType == true)
            {
                var genericTypeDefinition = declaringType.GetGenericTypeDefinition();
                if (genericTypeDefinition == typeof(List<>) || genericTypeDefinition == typeof(ICollection<>))
                {
                    source = instance;
                    item = arguments[0];
                }
            }
        }

        if (source != null && item != null && source.TypeMapping is ClickHouseArrayTypeMapping)
        {
            return _sqlExpressionFactory.Function(
                "has",
                [source, item],
                nullable: true,
                argumentsPropagateNullability: [true, true],
                typeof(bool));
        }

        return null;
    }
}
