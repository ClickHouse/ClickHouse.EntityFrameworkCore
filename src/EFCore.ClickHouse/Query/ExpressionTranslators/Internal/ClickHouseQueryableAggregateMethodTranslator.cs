using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using System.Reflection;

namespace ClickHouse.EntityFrameworkCore.Query.ExpressionTranslators.Internal;

/// <summary>
/// Translates grouped LINQ aggregate methods (Count, Sum, Average, Min, Max)
/// into ClickHouse SQL aggregate function calls.
///
/// Scalar aggregates (without GROUP BY) are handled by the base EF Core classes;
/// this translator is needed for grouped aggregates produced by
/// <c>GroupBy().Select(g => g.Count())</c> and similar patterns.
/// </summary>
public class ClickHouseQueryableAggregateMethodTranslator : IAggregateMethodCallTranslator
{
    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public ClickHouseQueryableAggregateMethodTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        MethodInfo method,
        EnumerableExpression source,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (method.DeclaringType != typeof(Queryable))
            return null;

        var methodInfo = method.IsGenericMethod
            ? method.GetGenericMethodDefinition()
            : method;

        switch (methodInfo.Name)
        {
            case nameof(Queryable.Average)
                when (QueryableMethods.IsAverageWithoutSelector(methodInfo)
                    || QueryableMethods.IsAverageWithSelector(methodInfo))
                && source.Selector is SqlExpression averageSqlExpression:
            {
                // ClickHouse avg() on integer columns returns 0 for empty groups;
                // avgOrNull() returns NULL instead, matching LINQ/SQL Server semantics.
                // Cast int/long to double first so avg doesn't do integer division.
                var averageInputType = averageSqlExpression.Type;
                if (averageInputType == typeof(int) || averageInputType == typeof(long))
                {
                    averageSqlExpression = _sqlExpressionFactory.ApplyDefaultTypeMapping(
                        _sqlExpressionFactory.Convert(averageSqlExpression, typeof(double)));
                }

                averageSqlExpression = CombineTerms(source, averageSqlExpression);

                return _sqlExpressionFactory.Function(
                    "avgOrNull",
                    [averageSqlExpression],
                    nullable: true,
                    argumentsPropagateNullability: [false],
                    typeof(double));
            }

            case nameof(Queryable.Count)
                when methodInfo == QueryableMethods.CountWithoutPredicate
                || methodInfo == QueryableMethods.CountWithPredicate:
            {
                var countSqlExpression = (source.Selector as SqlExpression)
                    ?? _sqlExpressionFactory.Fragment("*");
                countSqlExpression = CombineTerms(source, countSqlExpression);

                return _sqlExpressionFactory.Function(
                    "COUNT",
                    [countSqlExpression],
                    nullable: false,
                    argumentsPropagateNullability: [false],
                    typeof(int));
            }

            case nameof(Queryable.LongCount)
                when methodInfo == QueryableMethods.LongCountWithoutPredicate
                || methodInfo == QueryableMethods.LongCountWithPredicate:
            {
                var longCountSqlExpression = (source.Selector as SqlExpression)
                    ?? _sqlExpressionFactory.Fragment("*");
                longCountSqlExpression = CombineTerms(source, longCountSqlExpression);

                return _sqlExpressionFactory.Function(
                    "COUNT",
                    [longCountSqlExpression],
                    nullable: false,
                    argumentsPropagateNullability: [false],
                    typeof(long));
            }

            case nameof(Queryable.Max)
                when (methodInfo == QueryableMethods.MaxWithoutSelector
                    || methodInfo == QueryableMethods.MaxWithSelector)
                && source.Selector is SqlExpression maxSqlExpression:
            {
                maxSqlExpression = CombineTerms(source, maxSqlExpression);

                return _sqlExpressionFactory.Function(
                    "MAX",
                    [maxSqlExpression],
                    nullable: true,
                    argumentsPropagateNullability: [false],
                    maxSqlExpression.Type,
                    maxSqlExpression.TypeMapping);
            }

            case nameof(Queryable.Min)
                when (methodInfo == QueryableMethods.MinWithoutSelector
                    || methodInfo == QueryableMethods.MinWithSelector)
                && source.Selector is SqlExpression minSqlExpression:
            {
                minSqlExpression = CombineTerms(source, minSqlExpression);

                return _sqlExpressionFactory.Function(
                    "MIN",
                    [minSqlExpression],
                    nullable: true,
                    argumentsPropagateNullability: [false],
                    minSqlExpression.Type,
                    minSqlExpression.TypeMapping);
            }

            case nameof(Queryable.Sum)
                when (QueryableMethods.IsSumWithoutSelector(methodInfo)
                    || QueryableMethods.IsSumWithSelector(methodInfo))
                && source.Selector is SqlExpression sumSqlExpression:
            {
                sumSqlExpression = CombineTerms(source, sumSqlExpression);

                return _sqlExpressionFactory.Function(
                    "SUM",
                    [sumSqlExpression],
                    nullable: true,
                    argumentsPropagateNullability: [false],
                    sumSqlExpression.Type,
                    sumSqlExpression.TypeMapping);
            }
        }

        return null;
    }

    /// <summary>
    /// Wraps the aggregate operand to handle predicate filtering and DISTINCT.
    ///
    /// When a predicate is present (e.g. <c>g.Count(x => x.IsActive)</c>), the operand
    /// is wrapped in <c>CASE WHEN predicate THEN expr ELSE NULL END</c> so that only
    /// matching rows contribute to the aggregate. If the operand is <c>*</c> (a fragment),
    /// it's replaced with the constant <c>1</c> since <c>CASE WHEN ... THEN * END</c>
    /// isn't valid SQL.
    ///
    /// When DISTINCT is requested, the operand is wrapped in a <see cref="DistinctExpression"/>
    /// so the SQL generator emits <c>COUNT(DISTINCT expr)</c> etc.
    /// </summary>
    private SqlExpression CombineTerms(EnumerableExpression enumerableExpression, SqlExpression sqlExpression)
    {
        if (enumerableExpression.Predicate != null)
        {
            if (sqlExpression is SqlFragmentExpression)
            {
                sqlExpression = _sqlExpressionFactory.Constant(1);
            }

            sqlExpression = _sqlExpressionFactory.Case(
                [new CaseWhenClause(enumerableExpression.Predicate, sqlExpression)],
                elseResult: null);
        }

        if (enumerableExpression.IsDistinct)
        {
            sqlExpression = new DistinctExpression(sqlExpression);
        }

        return sqlExpression;
    }
}
