using System.Linq.Expressions;
using System.Reflection;
using ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace ClickHouse.EntityFrameworkCore.Query.Internal;

public class ClickHouseSqlTranslatingExpressionVisitor : RelationalSqlTranslatingExpressionVisitor
{
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
        if (method.Name == nameof(Queryable.AsQueryable)
            || (method.DeclaringType == typeof(Enumerable) && method.Name == nameof(Enumerable.AsEnumerable)))
        {
            return Visit(methodCallExpression.Arguments[0]);
        }

        if (method.IsGenericMethod && method.Name == nameof(Enumerable.Contains))
        {
            var genericMethodDefinition = method.GetGenericMethodDefinition();
            if (genericMethodDefinition == typeof(Enumerable).GetRuntimeMethods().First(m => m.Name == nameof(Enumerable.Contains) && m.GetParameters().Length == 2)
                || genericMethodDefinition == typeof(Queryable).GetRuntimeMethods().First(m => m.Name == nameof(Queryable.Contains) && m.GetParameters().Length == 2))
            {
                var source = Visit(methodCallExpression.Arguments[0]) as SqlExpression;
                var item = Visit(methodCallExpression.Arguments[1]) as SqlExpression;

                if (source != null && item != null && source.TypeMapping is ClickHouseArrayTypeMapping)
                {
                    return Dependencies.SqlExpressionFactory.Function(
                        "has",
                        [source, item],
                        nullable: true,
                        argumentsPropagateNullability: [true, true],
                        typeof(bool));
                }
            }
        }

        return base.VisitMethodCall(methodCallExpression);
    }
}
