using System.Linq.Expressions;
using ClickHouse.EntityFrameworkCore.Query.ExpressionTranslators.Internal;
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
    {
        if (_arrayLinqTranslator.TryTranslate(methodCallExpression, out var arrayTranslation))
        {
            return arrayTranslation;
        }

        var translated = base.VisitMethodCall(methodCallExpression);

        if (translated == QueryCompilationContext.NotTranslatedExpression
            && ClickHouseDateTimeMethodTranslator.IsAddMethod(methodCallExpression.Method)
            && methodCallExpression.Object is { } methodInstance
            && methodCallExpression.Arguments is [var methodArgument]
            && Visit(methodInstance) is SqlExpression sqlInstance
            && Visit(methodArgument) is SqlExpression sqlArgument
            && ClickHouseDateTimeMethodTranslator.GetUnsupportedAddTranslationErrorDetails(
                methodCallExpression.Method, sqlInstance, sqlArgument) is { } errorDetails)
        {
            AddTranslationErrorDetails(errorDetails);
        }

        return translated;
    }

    /// <summary>
    /// Reports a clear reason when two date/time values are added or subtracted.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ClickHouse has no operator for any of these shapes, and each one fails differently:
    /// </para>
    /// <list type="bullet">
    ///   <item><description>
    ///     One date minus another gives a <see cref="TimeSpan"/> in .NET, whereas <c>dateDiff</c>
    ///     returns a count of whole units.
    ///   </description></item>
    ///   <item><description>
    ///     One time of day minus another gives a <see cref="TimeSpan"/> in .NET, whereas ClickHouse
    ///     <c>Time64</c> subtraction gives a <c>Decimal</c> number of seconds.
    ///   </description></item>
    ///   <item><description>
    ///     A date plus or minus a <see cref="TimeSpan"/> keeps the date type in .NET, whereas ClickHouse
    ///     rejects the mixed operands outright (<c>Illegal types ... of arguments of function plus</c>).
    ///   </description></item>
    /// </list>
    /// <para>
    /// Left alone, each of these reaches type-mapping inference or the server and fails with an internal
    /// cast error or raw SQL error that names types the user never wrote. Reporting the reason here turns
    /// that into EF Core's normal "could not be translated" message with an explanation attached — which
    /// also restores client evaluation in a projection, where the .NET result is correct.
    /// </para>
    /// </remarks>
    protected override Expression VisitBinary(BinaryExpression binaryExpression)
    {
        if (binaryExpression.NodeType is ExpressionType.Add or ExpressionType.Subtract
            && IsDateOrTimeType(binaryExpression.Left.Type)
            && IsDateOrTimeType(binaryExpression.Right.Type))
        {
            AddTranslationErrorDetails(
                "Arithmetic on two date or time values is not supported, because ClickHouse has no "
                + "operator that matches the .NET result. Compare the two values directly, or project "
                + "them and do the arithmetic on the client.");

            return QueryCompilationContext.NotTranslatedExpression;
        }

        return base.VisitBinary(binaryExpression);
    }

    private static bool IsDateOrTimeType(Type type)
    {
        var unwrapped = Nullable.GetUnderlyingType(type) ?? type;
        return unwrapped == typeof(DateTime)
               || unwrapped == typeof(DateTimeOffset)
               || unwrapped == typeof(DateOnly)
               || unwrapped == typeof(TimeSpan)
               || unwrapped == typeof(TimeOnly);
    }
}
