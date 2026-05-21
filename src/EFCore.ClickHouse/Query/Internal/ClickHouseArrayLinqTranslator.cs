using System.Linq.Expressions;
using System.Reflection;
using ClickHouse.EntityFrameworkCore.Query.Expressions.Internal;
using ClickHouse.EntityFrameworkCore.Query.ExpressionTranslators.Internal;
using ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Query.Internal;

/// <summary>
/// Intercepts mapped-<c>Array(T)</c>-column method calls at the LINQ-expression level
/// before EF Core's relational pipeline normalizes them into shapes the
/// <see cref="ClickHouseArrayMethodTranslator"/> can't reverse (e.g. <c>Queryable.Contains</c>
/// that never gets normalized to <c>Enumerable.Contains</c>, or
/// <c>Select(...).Contains(...)</c> shapes that need a higher-order lambda).
///
/// Owned by <see cref="ClickHouseSqlTranslatingExpressionVisitor"/>; calls back into the
/// visitor's <c>Visit</c> for recursive scalar translation. Static (LINQ-tree) decisions
/// are made via <see cref="LooksLikeArrayColumnAccess"/> to keep the eager
/// <c>Visit(arguments[0])</c> calls off paths the base queryable pipeline owns
/// (DbSet roots, subqueries, …).
/// </summary>
public class ClickHouseArrayLinqTranslator
{
    private static readonly MethodInfo EnumerableContainsMethod = GetEnumerableMethod(nameof(Enumerable.Contains), parameterCount: 2);
    private static readonly MethodInfo EnumerableAnyMethod = GetEnumerableMethod(nameof(Enumerable.Any));
    private static readonly MethodInfo EnumerableAnyPredicateMethod = GetEnumerableMethod(nameof(Enumerable.Any), parameterCount: 2);
    private static readonly MethodInfo EnumerableCountMethod = GetEnumerableMethod(nameof(Enumerable.Count));
    private static readonly MethodInfo EnumerableCountPredicateMethod = GetEnumerableMethod(nameof(Enumerable.Count), parameterCount: 2);
    private static readonly MethodInfo EnumerableLongCountMethod = GetEnumerableMethod(nameof(Enumerable.LongCount));
    private static readonly MethodInfo EnumerableLongCountPredicateMethod = GetEnumerableMethod(nameof(Enumerable.LongCount), parameterCount: 2);
    private static readonly MethodInfo EnumerableAsEnumerableMethod = GetEnumerableMethod(nameof(Enumerable.AsEnumerable));

    private static readonly MethodInfo QueryableContainsMethod = GetQueryableMethod(nameof(Queryable.Contains), parameterCount: 2);
    private static readonly MethodInfo QueryableAnyMethod = GetQueryableMethod(nameof(Queryable.Any));
    private static readonly MethodInfo QueryableAnyPredicateMethod = GetQueryableMethod(nameof(Queryable.Any), parameterCount: 2);
    private static readonly MethodInfo QueryableCountMethod = GetQueryableMethod(nameof(Queryable.Count));
    private static readonly MethodInfo QueryableCountPredicateMethod = GetQueryableMethod(nameof(Queryable.Count), parameterCount: 2);
    private static readonly MethodInfo QueryableLongCountMethod = GetQueryableMethod(nameof(Queryable.LongCount));
    private static readonly MethodInfo QueryableLongCountPredicateMethod = GetQueryableMethod(nameof(Queryable.LongCount), parameterCount: 2);
    private static readonly MethodInfo QueryableAsQueryableMethod = GetQueryableMethod(nameof(Queryable.AsQueryable));

    private readonly ISqlExpressionFactory _sqlExpressionFactory;
    private readonly IRelationalTypeMappingSource _typeMappingSource;
    private readonly Func<Expression, Expression?> _visit;
    private readonly ClickHouseArrayMethodTranslator _arrayTranslator;

    public ClickHouseArrayLinqTranslator(
        RelationalSqlTranslatingExpressionVisitorDependencies dependencies,
        Func<Expression, Expression?> visit)
    {
        _sqlExpressionFactory = dependencies.SqlExpressionFactory;
        _typeMappingSource = dependencies.TypeMappingSource;
        _visit = visit;
        _arrayTranslator = new ClickHouseArrayMethodTranslator(
            dependencies.SqlExpressionFactory,
            dependencies.TypeMappingSource);
    }

    /// <summary>
    /// Attempts to translate <paramref name="methodCallExpression"/> as a mapped-array-column
    /// LINQ operation. Returns <c>true</c> when the call was recognized and
    /// <paramref name="translated"/> is set to the resulting SQL expression. Returns
    /// <c>false</c> for any non-array LINQ shape; the caller should defer to base
    /// translation.
    /// </summary>
    public bool TryTranslate(MethodCallExpression methodCallExpression, out SqlExpression translated)
    {
        translated = null!;
        var method = methodCallExpression.Method;
        if (!method.IsGenericMethod)
        {
            return false;
        }

        // Every shape we dispatch (AsQueryable strip, Select-then-Contains,
        // Contains/Any/Count/LongCount + predicate overloads) reads `Arguments[0]` as the
        // array source. Zero-argument generic methods (e.g. EF.Functions JSON helpers) can
        // reach us through the SQL translator chain; bail out before the array-source
        // dispatch tries to index an empty argument list.
        if (methodCallExpression.Arguments.Count == 0)
        {
            return false;
        }

        var genericMethodDefinition = method.GetGenericMethodDefinition();

        // Strip AsQueryable/AsEnumerable on mapped arrays; let downstream callers see the
        // underlying SqlExpression so subsequent method dispatch (Contains, Any, …) works
        // as if the wrapper were never there.
        if ((genericMethodDefinition == QueryableAsQueryableMethod
                || genericMethodDefinition == EnumerableAsEnumerableMethod)
            && _visit(methodCallExpression.Arguments[0]) is SqlExpression { TypeMapping: ClickHouseArrayTypeMapping } arraySource)
        {
            translated = arraySource;
            return true;
        }

        // Select(arr, lambda).Contains(value) — array-lambda translation. Apply the same
        // structural pre-filter as TryTranslateMappedArrayCall so we never eagerly visit
        // a queryable-root source (and trip the EnumerableExpression assertion).
        if ((genericMethodDefinition == EnumerableContainsMethod
                || genericMethodDefinition == QueryableContainsMethod)
            && methodCallExpression.Arguments[0] is MethodCallExpression selectCall
            && IsLinqSelect(selectCall.Method)
            && selectCall.Arguments.Count == 2
            && LooksLikeArrayColumnAccess(selectCall.Arguments[0])
            && TryTranslateSelectThenContains(selectCall, methodCallExpression.Arguments[1]) is { } selectContainsResult)
        {
            translated = selectContainsResult;
            return true;
        }

        return TryTranslateMappedArrayCall(methodCallExpression, genericMethodDefinition, out translated);
    }

    /// <summary>
    /// Dispatches the non-Select-wrap LINQ shapes: <c>Contains</c>/<c>Any</c>/
    /// <c>Count</c>/<c>LongCount</c> and their predicate overloads. Bypasses EF Core's
    /// default inline-collection lowering for mapped <c>Array(T)</c> columns.
    /// </summary>
    private bool TryTranslateMappedArrayCall(
        MethodCallExpression methodCallExpression,
        MethodInfo genericMethodDefinition,
        out SqlExpression translated)
    {
        translated = null!;

        // Pre-filter on the static expression shape: only attempt array-column translation
        // when arguments[0] is a property access (or such an access wrapped in
        // AsQueryable/AsEnumerable). Eagerly visiting an EF Core query root expression
        // (e.g. ctx.Set<T>()) trips an EnumerableExpression assertion inside the base
        // queryable visitor, so we must avoid that path for true subqueries.
        if (!LooksLikeArrayColumnAccess(methodCallExpression.Arguments[0]))
        {
            return false;
        }

        if ((genericMethodDefinition == EnumerableContainsMethod
                || genericMethodDefinition == QueryableContainsMethod)
            && _visit(methodCallExpression.Arguments[0]) is SqlExpression containsSource
            && ClickHouseArrayMethodTranslator.IsClickHouseArray(containsSource)
            && _visit(methodCallExpression.Arguments[1]) is SqlExpression containsItem)
        {
            translated = _arrayTranslator.TranslateContains(containsSource, containsItem);
            return true;
        }

        if ((genericMethodDefinition == EnumerableAnyMethod
                || genericMethodDefinition == QueryableAnyMethod)
            && _visit(methodCallExpression.Arguments[0]) is SqlExpression anySource
            && ClickHouseArrayMethodTranslator.IsClickHouseArray(anySource))
        {
            translated = _arrayTranslator.TranslateNotEmpty(anySource);
            return true;
        }

        if ((genericMethodDefinition == EnumerableCountMethod
                || genericMethodDefinition == QueryableCountMethod)
            && _visit(methodCallExpression.Arguments[0]) is SqlExpression countSource
            && ClickHouseArrayMethodTranslator.IsClickHouseArray(countSource))
        {
            translated = _arrayTranslator.TranslateLength(countSource, typeof(int));
            return true;
        }

        if ((genericMethodDefinition == EnumerableLongCountMethod
                || genericMethodDefinition == QueryableLongCountMethod)
            && _visit(methodCallExpression.Arguments[0]) is SqlExpression longCountSource
            && ClickHouseArrayMethodTranslator.IsClickHouseArray(longCountSource))
        {
            translated = _arrayTranslator.TranslateLength(longCountSource, typeof(long));
            return true;
        }

        // Predicate overloads: Any(arr, lambda) / Count(arr, lambda) / LongCount(arr, lambda).
        // Routed to ClickHouse's higher-order array functions arrayExists / arrayCount.
        if (genericMethodDefinition == EnumerableAnyPredicateMethod
            || genericMethodDefinition == QueryableAnyPredicateMethod)
        {
            return TryTranslateArrayHigherOrderPredicate(
                methodCallExpression, "arrayExists", typeof(bool), out translated);
        }

        if (genericMethodDefinition == EnumerableCountPredicateMethod
            || genericMethodDefinition == QueryableCountPredicateMethod)
        {
            return TryTranslateArrayHigherOrderPredicate(
                methodCallExpression, "arrayCount", typeof(int), out translated);
        }

        if (genericMethodDefinition == EnumerableLongCountPredicateMethod
            || genericMethodDefinition == QueryableLongCountPredicateMethod)
        {
            return TryTranslateArrayHigherOrderPredicate(
                methodCallExpression, "arrayCount", typeof(long), out translated);
        }

        return false;
    }

    private bool TryTranslateArrayHigherOrderPredicate(
        MethodCallExpression methodCallExpression,
        string functionName,
        Type returnType,
        out SqlExpression translated)
    {
        translated = null!;

        if (_visit(methodCallExpression.Arguments[0]) is not SqlExpression arraySql
            || arraySql.TypeMapping is not ClickHouseArrayTypeMapping arrayMapping)
        {
            return false;
        }

        var lambda = UnwrapLambda(methodCallExpression.Arguments[1]);
        if (lambda is null || lambda.Parameters.Count != 1)
            return false;

        var arrayLambda = TranslateArrayLambda(lambda, arrayMapping.ElementMapping);
        if (arrayLambda is null)
            return false;

        var resultMapping = _typeMappingSource.FindMapping(returnType);
        translated = _sqlExpressionFactory.Function(
            functionName,
            [arrayLambda, arraySql],
            nullable: false,
            argumentsPropagateNullability: [false, false],
            returnType,
            resultMapping);
        return true;
    }

    private SqlExpression? TryTranslateSelectThenContains(MethodCallExpression selectCall, Expression itemExpr)
    {
        if (_visit(selectCall.Arguments[0]) is not SqlExpression arraySql
            || arraySql.TypeMapping is not ClickHouseArrayTypeMapping arrayMapping)
        {
            return null;
        }

        var lambda = UnwrapLambda(selectCall.Arguments[1]);
        if (lambda is null || lambda.Parameters.Count != 1)
            return null;

        var arrayLambda = TranslateArrayLambda(lambda, arrayMapping.ElementMapping);
        if (arrayLambda is null)
            return null;

        if (_visit(itemExpr) is not SqlExpression itemSql)
            return null;

        // arrayMap's result is Array(<body-type>); give it an explicit ClickHouseArrayTypeMapping
        // so downstream consumers (TranslateContains) can read off the element mapping the
        // same way they would for a plain array column.
        var resultElementMapping = arrayLambda.Body.TypeMapping
            ?? _typeMappingSource.FindMapping(arrayLambda.Body.Type);
        if (resultElementMapping is null)
            return null;

        var arrayMapMapping = new ClickHouseArrayTypeMapping(resultElementMapping);
        var arrayMap = _sqlExpressionFactory.Function(
            "arrayMap",
            [arrayLambda, arraySql],
            nullable: false,
            argumentsPropagateNullability: [false, false],
            arrayLambda.Body.Type.MakeArrayType(),
            arrayMapMapping);

        return _arrayTranslator.TranslateContains(arrayMap, itemSql);
    }

    /// <summary>
    /// Routes a single-parameter LINQ lambda body through the standard scalar translator
    /// chain by substituting the <see cref="ParameterExpression"/> with a
    /// <see cref="ClickHouseArrayLambdaReferenceExpression"/> sentinel (which is itself a
    /// <see cref="SqlExpression"/>, so the base translating visitor passes it through
    /// unchanged when it encounters it).
    /// </summary>
    private ClickHouseArrayLambdaExpression? TranslateArrayLambda(
        LambdaExpression lambda,
        RelationalTypeMapping elementMapping)
    {
        var parameter = lambda.Parameters[0];
        var parameterRef = new ClickHouseArrayLambdaReferenceExpression(
            parameter.Name ?? "x",
            parameter.Type,
            elementMapping);

        var substitutedBody = new ParameterReplacingVisitor(parameter, parameterRef).Visit(lambda.Body);
        if (_visit(substitutedBody!) is not SqlExpression bodySql)
            return null;

        // Translators sometimes leave intermediate SqlExpressions (e.g. CASE expressions
        // built for string.CompareTo) without a TypeMapping — the postprocessor later
        // assigns one. ClickHouseArrayLambdaExpression flows through that postprocessor as
        // an opaque extension, so we must give the body a mapping up front. Fall back to
        // the default CLR-type mapping when the translator didn't assign one.
        var bodyWithMapping = bodySql.TypeMapping is null
            ? _sqlExpressionFactory.ApplyTypeMapping(
                bodySql,
                _typeMappingSource.FindMapping(bodySql.Type))
            : bodySql;
        if (bodyWithMapping.TypeMapping is null)
            return null;

        return new ClickHouseArrayLambdaExpression(parameterRef, bodyWithMapping);
    }

    private static bool LooksLikeArrayColumnAccess(Expression source)
    {
        // Strip AsQueryable/AsEnumerable wrappers structurally before deciding.
        while (source is MethodCallExpression { Method.IsGenericMethod: true } call
               && (call.Method.GetGenericMethodDefinition() == QueryableAsQueryableMethod
                   || call.Method.GetGenericMethodDefinition() == EnumerableAsEnumerableMethod))
        {
            source = call.Arguments[0];
        }

        // A direct member access (e.IntArray) is the obvious column-access shape, but EF
        // Core's preprocessor rewrites entity property reads as `EF.Property<T>(entity,
        // "Name")` before the visitor runs, so we recognize that shape too. Anything else
        // (DbSet/IQueryable roots, subqueries, navigation expansions…) is left to EF Core's
        // standard handling.
        return source is MemberExpression
            || (source is MethodCallExpression { Method: { IsStatic: true, Name: "Property", DeclaringType.Name: "EF" } } propertyCall
                && propertyCall.Method.IsGenericMethod);
    }

    private static LambdaExpression? UnwrapLambda(Expression expression)
    {
        if (expression is UnaryExpression { NodeType: ExpressionType.Quote } quoted)
            expression = quoted.Operand;
        return expression as LambdaExpression;
    }

    private static bool IsLinqSelect(MethodInfo method)
        => method.Name == nameof(Enumerable.Select)
            && (method.DeclaringType == typeof(Enumerable) || method.DeclaringType == typeof(Queryable));

    private static MethodInfo GetEnumerableMethod(string name, int parameterCount = 1)
        => typeof(Enumerable).GetRuntimeMethods()
            .Single(m => m.Name == name && m.IsGenericMethod && m.GetParameters().Length == parameterCount);

    private static MethodInfo GetQueryableMethod(string name, int parameterCount = 1)
        => typeof(Queryable).GetRuntimeMethods()
            .Single(m => m.Name == name && m.IsGenericMethod && m.GetParameters().Length == parameterCount);

    private sealed class ParameterReplacingVisitor : ExpressionVisitor
    {
        private readonly ParameterExpression _toReplace;
        private readonly Expression _replacement;

        public ParameterReplacingVisitor(ParameterExpression toReplace, Expression replacement)
        {
            _toReplace = toReplace;
            _replacement = replacement;
        }

        protected override Expression VisitParameter(ParameterExpression node)
            => node == _toReplace ? _replacement : node;
    }
}
