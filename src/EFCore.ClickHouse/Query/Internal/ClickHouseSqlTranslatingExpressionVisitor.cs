using System.Linq.Expressions;
using System.Reflection;
using ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace ClickHouse.EntityFrameworkCore.Query.Internal;

public class ClickHouseSqlTranslatingExpressionVisitor : RelationalSqlTranslatingExpressionVisitor
{
    private static readonly MethodInfo EnumerableContainsMethod = typeof(Enumerable).GetRuntimeMethods()
        .Single(m => m.Name == nameof(Enumerable.Contains) && m.GetParameters().Length == 2);

    private static readonly MethodInfo QueryableContainsMethod = typeof(Queryable).GetRuntimeMethods()
        .Single(m => m.Name == nameof(Queryable.Contains) && m.GetParameters().Length == 2);

    private static readonly MethodInfo QueryableAsQueryableMethod = typeof(Queryable).GetRuntimeMethods()
        .Single(m => m.Name == nameof(Queryable.AsQueryable) && m.IsGenericMethod);

    private static readonly MethodInfo EnumerableAsEnumerableMethod = typeof(Enumerable).GetRuntimeMethods()
        .Single(m => m.Name == nameof(Enumerable.AsEnumerable) && m.IsGenericMethod);

    public ClickHouseSqlTranslatingExpressionVisitor(
        RelationalSqlTranslatingExpressionVisitorDependencies dependencies,
        QueryCompilationContext queryCompilationContext,
        QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor)
        : base(dependencies, queryCompilationContext, queryableMethodTranslatingExpressionVisitor)
    {
    }

    public override SqlExpression GenerateLeast(IReadOnlyList<SqlExpression> expressions, Type resultType)
    {
        var resultTypeMapping = ExpressionExtensions.InferTypeMapping(expressions);
        return Dependencies.SqlExpressionFactory.Function(
            "least", expressions, nullable: true,
            Enumerable.Repeat(true, expressions.Count), resultType, resultTypeMapping);
    }

    public override SqlExpression GenerateGreatest(IReadOnlyList<SqlExpression> expressions, Type resultType)
    {
        var resultTypeMapping = ExpressionExtensions.InferTypeMapping(expressions);
        return Dependencies.SqlExpressionFactory.Function(
            "greatest", expressions, nullable: true,
            Enumerable.Repeat(true, expressions.Count), resultType, resultTypeMapping);
    }

    protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
    {
        var method = methodCallExpression.Method;

        if (method.IsGenericMethod)
        {
            var genericMethodDefinition = method.GetGenericMethodDefinition();
            if (genericMethodDefinition == QueryableAsQueryableMethod
                || genericMethodDefinition == EnumerableAsEnumerableMethod)
            {
                return Visit(methodCallExpression.Arguments[0]);
            }
        }

        SqlExpression? source = null;
        SqlExpression? item = null;

        if (method.IsGenericMethod)
        {
            var genericMethodDefinition = method.GetGenericMethodDefinition();
            if (genericMethodDefinition == EnumerableContainsMethod
                || genericMethodDefinition == QueryableContainsMethod)
            {
                source = Visit(methodCallExpression.Arguments[0]) as SqlExpression;
                item = Visit(methodCallExpression.Arguments[1]) as SqlExpression;
            }
        }
        else if (methodCallExpression.Object != null
                 && method.Name == nameof(Enumerable.Contains)
                 && methodCallExpression.Arguments.Count == 1)
        {
            var declaringType = method.DeclaringType;
            if (declaringType?.IsGenericType == true)
            {
                var genericTypeDefinition = declaringType.GetGenericTypeDefinition();
                if (genericTypeDefinition == typeof(List<>)
                    || genericTypeDefinition == typeof(ICollection<>)
                    || genericTypeDefinition == typeof(IList<>))
                {
                    source = Visit(methodCallExpression.Object) as SqlExpression;
                    item = Visit(methodCallExpression.Arguments[0]) as SqlExpression;
                }
            }
        }

        if (source != null && item != null && source.TypeMapping is ClickHouseArrayTypeMapping)
        {
            return Dependencies.SqlExpressionFactory.Function(
                "has",
                [source, item],
                nullable: true,
                argumentsPropagateNullability: [true, true],
                typeof(bool));
        }

        return base.VisitMethodCall(methodCallExpression);
    }
}
