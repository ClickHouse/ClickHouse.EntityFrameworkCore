using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace ClickHouse.EntityFrameworkCore.Query.Internal;

public class ClickHouseSqlTranslatingExpressionVisitor : RelationalSqlTranslatingExpressionVisitor
{
    private readonly ClickHouseArrayLinqTranslator _arrayLinqTranslator;

    public ClickHouseSqlTranslatingExpressionVisitor(
        RelationalSqlTranslatingExpressionVisitorDependencies dependencies,
        QueryCompilationContext queryCompilationContext,
        QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor)
        : base(dependencies, queryCompilationContext, queryableMethodTranslatingExpressionVisitor)
    {
        _arrayLinqTranslator = new ClickHouseArrayLinqTranslator(dependencies, Visit);
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

    /// <summary>
    /// Delegates to <see cref="ClickHouseArrayLinqTranslator"/> for mapped-<c>Array(T)</c>
    /// LINQ shapes that EF Core would otherwise normalize beyond reach of the
    /// <c>IMethodCallTranslator</c> chain — <c>Queryable.Contains</c>/<c>Any</c>/etc.,
    /// <c>AsQueryable</c>/<c>AsEnumerable</c> markers on array columns, and
    /// <c>Select(...).Contains(...)</c> lambda patterns.
    /// </summary>
    protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
        => _arrayLinqTranslator.TryTranslate(methodCallExpression, out var translated)
            ? translated
            : base.VisitMethodCall(methodCallExpression);
}
