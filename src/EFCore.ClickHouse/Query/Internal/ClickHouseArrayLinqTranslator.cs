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
/// <para>
/// Dispatch is driven by a static <see cref="Dictionary{TKey, TValue}"/> keyed on the
/// generic method definition. The dictionary keys double as the whitelist of recognized
/// methods, so unknown method calls (including zero-argument generic methods like
/// <c>JsonNode.GetValue&lt;T&gt;()</c>) miss the lookup and fall through to base
/// translation. New translations register a single entry in <see cref="Dispatch"/>.
/// </para>
///
/// <para>
/// <b>Scope:</b> this translator handles <i>generic</i> static extension methods on
/// <see cref="Enumerable"/> and <see cref="Queryable"/>. Non-generic instance methods on
/// concrete collection types (e.g. an indexer or a future <c>List&lt;T&gt;.IndexOf</c>
/// translation) can't be reached by the <c>IsGenericMethod</c>-gated dispatch and must go
/// through <see cref="ClickHouseArrayMethodTranslator"/>'s <c>IMethodCallTranslator</c>
/// path instead. EF Core normalizes the common shapes (e.g. <c>List&lt;T&gt;.Contains</c>
/// becomes <c>Enumerable.Contains</c>) before our visitor runs, so most patterns light up
/// here regardless of where the user-side instance method lived.
/// </para>
///
/// <para>
/// Owned by <see cref="ClickHouseSqlTranslatingExpressionVisitor"/>; calls back into the
/// visitor's <c>Visit</c> for recursive scalar translation. Static (LINQ-tree) decisions
/// are made via <see cref="LooksLikeArrayColumnAccess"/> to keep the eager
/// <c>Visit(...)</c> calls off paths the base queryable pipeline owns (DbSet roots,
/// subqueries, …).
/// </para>
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
    private static readonly MethodInfo EnumerableFirstMethod = GetEnumerableMethod(nameof(Enumerable.First));
    private static readonly MethodInfo EnumerableFirstOrDefaultMethod = GetEnumerableMethod(nameof(Enumerable.FirstOrDefault));
    private static readonly MethodInfo EnumerableLastMethod = GetEnumerableMethod(nameof(Enumerable.Last));
    private static readonly MethodInfo EnumerableLastOrDefaultMethod = GetEnumerableMethod(nameof(Enumerable.LastOrDefault));
    private static readonly MethodInfo EnumerableElementAtMethod = GetEnumerableElementAtMethod(nameof(Enumerable.ElementAt));
    private static readonly MethodInfo EnumerableElementAtOrDefaultMethod = GetEnumerableElementAtMethod(nameof(Enumerable.ElementAtOrDefault));
    private static readonly MethodInfo EnumerableSkipMethod = GetEnumerableMethod(nameof(Enumerable.Skip), parameterCount: 2);
    private static readonly MethodInfo EnumerableTakeMethod = GetEnumerableTakeIntMethod();
    private static readonly MethodInfo EnumerableReverseMethod = GetEnumerableReverseMethod(typeof(IEnumerable<>));

    /// <summary>
    /// .NET 9 added <c>Enumerable.Reverse&lt;T&gt;(T[])</c> alongside the classic
    /// <c>Enumerable.Reverse&lt;T&gt;(IEnumerable&lt;T&gt;)</c>. C# overload resolution picks
    /// the array overload for <c>int[]</c>-typed sources, so we register and dispatch on it
    /// in addition to the IEnumerable form.
    /// </summary>
    private static readonly MethodInfo? EnumerableReverseArrayMethod = TryGetEnumerableReverseArrayMethod();
    private static readonly MethodInfo EnumerableDistinctMethod = GetEnumerableMethod(nameof(Enumerable.Distinct));
    private static readonly MethodInfo EnumerableOrderByMethod = GetEnumerableMethod(nameof(Enumerable.OrderBy), parameterCount: 2);
    private static readonly MethodInfo EnumerableOrderByDescendingMethod = GetEnumerableMethod(nameof(Enumerable.OrderByDescending), parameterCount: 2);

    private static readonly MethodInfo QueryableContainsMethod = GetQueryableMethod(nameof(Queryable.Contains), parameterCount: 2);
    private static readonly MethodInfo QueryableAnyMethod = GetQueryableMethod(nameof(Queryable.Any));
    private static readonly MethodInfo QueryableAnyPredicateMethod = GetQueryableMethod(nameof(Queryable.Any), parameterCount: 2);
    private static readonly MethodInfo QueryableCountMethod = GetQueryableMethod(nameof(Queryable.Count));
    private static readonly MethodInfo QueryableCountPredicateMethod = GetQueryableMethod(nameof(Queryable.Count), parameterCount: 2);
    private static readonly MethodInfo QueryableLongCountMethod = GetQueryableMethod(nameof(Queryable.LongCount));
    private static readonly MethodInfo QueryableLongCountPredicateMethod = GetQueryableMethod(nameof(Queryable.LongCount), parameterCount: 2);
    private static readonly MethodInfo QueryableAsQueryableMethod = GetQueryableMethod(nameof(Queryable.AsQueryable));
    private static readonly MethodInfo QueryableFirstMethod = GetQueryableMethod(nameof(Queryable.First));
    private static readonly MethodInfo QueryableFirstOrDefaultMethod = GetQueryableMethod(nameof(Queryable.FirstOrDefault));
    private static readonly MethodInfo QueryableLastMethod = GetQueryableMethod(nameof(Queryable.Last));
    private static readonly MethodInfo QueryableLastOrDefaultMethod = GetQueryableMethod(nameof(Queryable.LastOrDefault));
    private static readonly MethodInfo QueryableElementAtMethod = GetQueryableElementAtMethod(nameof(Queryable.ElementAt));
    private static readonly MethodInfo QueryableElementAtOrDefaultMethod = GetQueryableElementAtMethod(nameof(Queryable.ElementAtOrDefault));
    private static readonly MethodInfo QueryableSkipMethod = GetQueryableMethod(nameof(Queryable.Skip), parameterCount: 2);
    private static readonly MethodInfo QueryableTakeMethod = GetQueryableTakeIntMethod();
    private static readonly MethodInfo QueryableReverseMethod = GetQueryableMethod(nameof(Queryable.Reverse));
    private static readonly MethodInfo QueryableDistinctMethod = GetQueryableMethod(nameof(Queryable.Distinct));
    private static readonly MethodInfo QueryableOrderByMethod = GetQueryableMethod(nameof(Queryable.OrderBy), parameterCount: 2);
    private static readonly MethodInfo QueryableOrderByDescendingMethod = GetQueryableMethod(nameof(Queryable.OrderByDescending), parameterCount: 2);

    /// <summary>
    /// Generic method definitions whose result is an <c>Array(T)</c> SqlExpression with a
    /// <see cref="ClickHouseArrayTypeMapping"/>. Chained patterns whose outer call is one
    /// of these (e.g. <c>arr.Skip(1).Contains(x)</c>) pass
    /// <see cref="LooksLikeArrayColumnAccess"/> as legitimate sources.
    /// </summary>
    private static readonly HashSet<MethodInfo> ArrayProducingMethods = BuildArrayProducingMethods();

    private static HashSet<MethodInfo> BuildArrayProducingMethods()
    {
        var set = new HashSet<MethodInfo>
        {
            EnumerableSkipMethod, QueryableSkipMethod,
            EnumerableTakeMethod, QueryableTakeMethod,
            EnumerableReverseMethod, QueryableReverseMethod,
            EnumerableDistinctMethod, QueryableDistinctMethod,
            EnumerableOrderByMethod, QueryableOrderByMethod,
            EnumerableOrderByDescendingMethod, QueryableOrderByDescendingMethod,
            EnumerableAsEnumerableMethod, QueryableAsQueryableMethod,
        };
        if (EnumerableReverseArrayMethod is not null)
            set.Add(EnumerableReverseArrayMethod);
        return set;
    }

    private delegate bool DispatchFn(ClickHouseArrayLinqTranslator self, MethodCallExpression call, out SqlExpression translated);

    /// <summary>
    /// Static dispatch table built once for the process. Handlers are static methods that
    /// receive the translator instance via the <see cref="DispatchFn"/> delegate's
    /// <c>self</c> parameter — avoids per-query dictionary construction and delegate
    /// allocation (EF Core creates a fresh visitor on every query compile). The conditional
    /// <see cref="EnumerableReverseArrayMethod"/> entry (.NET 9+) is added via
    /// <see cref="BuildDispatch"/> below so the table is built in a single pass.
    /// </summary>
    private static readonly Dictionary<MethodInfo, DispatchFn> Dispatch = BuildDispatch();

    private static Dictionary<MethodInfo, DispatchFn> BuildDispatch()
    {
        var dispatch = new Dictionary<MethodInfo, DispatchFn>
        {
            [EnumerableAsEnumerableMethod] = TranslateAsMarker,
            [QueryableAsQueryableMethod] = TranslateAsMarker,

            [EnumerableContainsMethod] = TranslateContains,
            [QueryableContainsMethod] = TranslateContains,

            [EnumerableAnyMethod] = TranslateAny,
            [QueryableAnyMethod] = TranslateAny,

            [EnumerableCountMethod] = TranslateCountInt32,
            [QueryableCountMethod] = TranslateCountInt32,

            [EnumerableLongCountMethod] = TranslateCountInt64,
            [QueryableLongCountMethod] = TranslateCountInt64,

            [EnumerableAnyPredicateMethod] = TranslateAnyPredicate,
            [QueryableAnyPredicateMethod] = TranslateAnyPredicate,

            [EnumerableCountPredicateMethod] = TranslateCountPredicateInt32,
            [QueryableCountPredicateMethod] = TranslateCountPredicateInt32,

            [EnumerableLongCountPredicateMethod] = TranslateCountPredicateInt64,
            [QueryableLongCountPredicateMethod] = TranslateCountPredicateInt64,

            [EnumerableFirstMethod] = TranslateFirst,
            [QueryableFirstMethod] = TranslateFirst,
            [EnumerableFirstOrDefaultMethod] = TranslateFirst,
            [QueryableFirstOrDefaultMethod] = TranslateFirst,

            [EnumerableLastMethod] = TranslateLast,
            [QueryableLastMethod] = TranslateLast,
            [EnumerableLastOrDefaultMethod] = TranslateLast,
            [QueryableLastOrDefaultMethod] = TranslateLast,

            [EnumerableElementAtMethod] = TranslateElementAt,
            [QueryableElementAtMethod] = TranslateElementAt,
            [EnumerableElementAtOrDefaultMethod] = TranslateElementAt,
            [QueryableElementAtOrDefaultMethod] = TranslateElementAt,

            [EnumerableSkipMethod] = TranslateSkip,
            [QueryableSkipMethod] = TranslateSkip,

            [EnumerableTakeMethod] = TranslateTake,
            [QueryableTakeMethod] = TranslateTake,

            [EnumerableReverseMethod] = TranslateReverse,
            [QueryableReverseMethod] = TranslateReverse,

            [EnumerableDistinctMethod] = TranslateDistinct,
            [QueryableDistinctMethod] = TranslateDistinct,

            [EnumerableOrderByMethod] = TranslateOrderBy,
            [QueryableOrderByMethod] = TranslateOrderBy,
            [EnumerableOrderByDescendingMethod] = TranslateOrderByDescending,
            [QueryableOrderByDescendingMethod] = TranslateOrderByDescending,
        };
        if (EnumerableReverseArrayMethod is not null)
            dispatch[EnumerableReverseArrayMethod] = TranslateReverse;
        return dispatch;
    }

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

        // Fast-reject anything that isn't a generic Enumerable/Queryable extension. Saves
        // the GetGenericMethodDefinition reflection + dictionary lookup for the vast
        // majority of method calls the visitor sees (string methods, math methods, EF
        // functions, JsonNode access, instance methods, …).
        if (!method.IsGenericMethod
            || (method.DeclaringType != typeof(Enumerable) && method.DeclaringType != typeof(Queryable)))
        {
            return false;
        }

        // Dictionary lookup IS the gate. Methods we don't recognize fall through.
        return Dispatch.TryGetValue(method.GetGenericMethodDefinition(), out var handler)
            && handler(this, methodCallExpression, out translated);
    }

    // ─── Dispatch handlers ────────────────────────────────────────────────

    /// <summary>
    /// Strip <c>AsQueryable()</c>/<c>AsEnumerable()</c> on a mapped array; downstream
    /// callers see the underlying SqlExpression so subsequent method dispatch (Contains,
    /// Any, …) works as if the wrapper were never there.
    /// </summary>
    private static bool TranslateAsMarker(ClickHouseArrayLinqTranslator self, MethodCallExpression call, out SqlExpression translated)
    {
        translated = null!;
        if (GetArraySourceCandidate(call) is not { } source
            || !LooksLikeArrayColumnAccess(source))
        {
            return false;
        }

        if (self._visit(source) is SqlExpression { TypeMapping: ClickHouseArrayTypeMapping } arraySource)
        {
            translated = arraySource;
            return true;
        }
        return false;
    }

    /// <summary>
    /// <c>arr.Contains(value)</c> — also catches the <c>arr.Select(x =&gt; f(x)).Contains(value)</c>
    /// shape and routes it to <see cref="TryTranslateSelectThenContains"/>.
    /// </summary>
    private static bool TranslateContains(ClickHouseArrayLinqTranslator self, MethodCallExpression call, out SqlExpression translated)
    {
        translated = null!;
        if (GetArraySourceCandidate(call) is not { } sourceExpr)
            return false;

        // Select(arr, lambda).Contains(value) — array-lambda translation.
        if (sourceExpr is MethodCallExpression selectCall
            && IsLinqSelect(selectCall.Method)
            && selectCall.Arguments.Count == 2
            && LooksLikeArrayColumnAccess(selectCall.Arguments[0])
            && self.TryTranslateSelectThenContains(selectCall, call.Arguments[1]) is { } selectContainsResult)
        {
            translated = selectContainsResult;
            return true;
        }

        if (!LooksLikeArrayColumnAccess(sourceExpr)
            || self._visit(sourceExpr) is not SqlExpression arraySql
            || !ClickHouseArrayMethodTranslator.IsClickHouseArray(arraySql)
            || self._visit(call.Arguments[1]) is not SqlExpression itemSql)
        {
            return false;
        }

        translated = self._arrayTranslator.TranslateContains(arraySql, itemSql);
        return true;
    }

    private static bool TranslateAny(ClickHouseArrayLinqTranslator self, MethodCallExpression call, out SqlExpression translated)
        => self.TryTranslateUnaryArrayCall(call, self._arrayTranslator.TranslateNotEmpty, out translated);

    private static bool TranslateCountInt32(ClickHouseArrayLinqTranslator self, MethodCallExpression call, out SqlExpression translated)
        => self.TryTranslateUnaryArrayCall(call, src => self._arrayTranslator.TranslateLength(src, typeof(int)), out translated);

    private static bool TranslateCountInt64(ClickHouseArrayLinqTranslator self, MethodCallExpression call, out SqlExpression translated)
        => self.TryTranslateUnaryArrayCall(call, src => self._arrayTranslator.TranslateLength(src, typeof(long)), out translated);

    private static bool TranslateAnyPredicate(ClickHouseArrayLinqTranslator self, MethodCallExpression call, out SqlExpression translated)
        => self.TryTranslateArrayHigherOrderPredicate(call, "arrayExists", typeof(bool), out translated);

    private static bool TranslateCountPredicateInt32(ClickHouseArrayLinqTranslator self, MethodCallExpression call, out SqlExpression translated)
        => self.TryTranslateArrayHigherOrderPredicate(call, "arrayCount", typeof(int), out translated);

    private static bool TranslateCountPredicateInt64(ClickHouseArrayLinqTranslator self, MethodCallExpression call, out SqlExpression translated)
        => self.TryTranslateArrayHigherOrderPredicate(call, "arrayCount", typeof(long), out translated);

    /// <summary>
    /// <c>arr.First()</c> / <c>arr.FirstOrDefault()</c> → <c>arrayElement(arr, 1)</c>.
    /// Both share an emission because ClickHouse never raises on empty arrays — it returns
    /// the element type's default. For <c>First()</c> on an empty array this is a documented
    /// divergence from .NET LINQ (which throws <see cref="InvalidOperationException"/>).
    /// </summary>
    private static bool TranslateFirst(ClickHouseArrayLinqTranslator self, MethodCallExpression call, out SqlExpression translated)
        => self.TryTranslateUnaryArrayCall(
            call,
            src => self._arrayTranslator.TranslateElementAt(src, self._sqlExpressionFactory.Constant(1)),
            out translated);

    /// <summary>
    /// <c>arr.Last()</c> / <c>arr.LastOrDefault()</c> → <c>arrayElement(arr, -1)</c>.
    /// ClickHouse supports negative indices as offsets from the end of the array; index
    /// <c>-1</c> is the last element. Empty-array semantics same as <see cref="TranslateFirst"/>.
    /// </summary>
    private static bool TranslateLast(ClickHouseArrayLinqTranslator self, MethodCallExpression call, out SqlExpression translated)
        => self.TryTranslateUnaryArrayCall(
            call,
            src => self._arrayTranslator.TranslateElementAt(src, self._sqlExpressionFactory.Constant(-1)),
            out translated);

    /// <summary>
    /// <c>arr.ElementAt(i)</c> / <c>arr.ElementAtOrDefault(i)</c> → <c>arrayElement(arr, i + 1)</c>.
    /// ClickHouse indices are 1-based, so the LINQ-side index gets <c>+1</c> applied. Diverges
    /// from .NET in two documented ways: out-of-range positive indices return the element
    /// type's default (no exception), and negative LINQ indices map to ClickHouse's from-end
    /// addressing instead of yielding default.
    /// </summary>
    private static bool TranslateElementAt(ClickHouseArrayLinqTranslator self, MethodCallExpression call, out SqlExpression translated)
        => self.TryTranslateUnaryArrayCallWithIntArg(
            call,
            (src, index) => self._arrayTranslator.TranslateElementAt(src, self.IndexPlusOne(index)),
            out translated);

    /// <summary>
    /// <c>arr.Skip(n)</c> → <c>arraySlice(arr, n + 1)</c>. The result is itself a mapped
    /// array, so chained operations (<c>Skip(n).Contains(x)</c>) light up via
    /// <see cref="ArrayProducingMethods"/>.
    /// </summary>
    private static bool TranslateSkip(ClickHouseArrayLinqTranslator self, MethodCallExpression call, out SqlExpression translated)
        => self.TryTranslateUnaryArrayCallWithIntArg(
            call,
            (src, count) => self._arrayTranslator.TranslateSlice(src, self.IndexPlusOne(count), length: null),
            out translated);

    /// <summary>
    /// Widens an Int32 LINQ-side index to Int64 before adding 1, so the arithmetic doesn't
    /// wrap on <c>int.MaxValue</c>. ClickHouse parses bare integer literals as the narrowest
    /// type that fits, so <c>index + 1</c> stays Int32 unless the cast is on the index side;
    /// without it, <c>ElementAt(int.MaxValue)</c> wraps to a from-end address (returns a real
    /// element instead of the documented OOB default).
    /// </summary>
    private SqlExpression IndexPlusOne(SqlExpression index)
    {
        var indexAsLong = _sqlExpressionFactory.Function(
            "toInt64",
            [index],
            nullable: false,
            argumentsPropagateNullability: [false],
            typeof(long),
            _typeMappingSource.FindMapping(typeof(long)));
        return _sqlExpressionFactory.Add(indexAsLong, _sqlExpressionFactory.Constant(1));
    }

    /// <summary>
    /// <c>arr.Take(n)</c> → <c>arraySlice(arr, 1, n)</c>.
    /// </summary>
    private static bool TranslateTake(ClickHouseArrayLinqTranslator self, MethodCallExpression call, out SqlExpression translated)
        => self.TryTranslateUnaryArrayCallWithIntArg(
            call,
            (src, count) => self._arrayTranslator.TranslateSlice(
                src,
                self._sqlExpressionFactory.Constant(1),
                count),
            out translated);

    private static bool TranslateReverse(ClickHouseArrayLinqTranslator self, MethodCallExpression call, out SqlExpression translated)
        => self.TryTranslateUnaryArrayCall(call, self._arrayTranslator.TranslateReverse, out translated);

    private static bool TranslateDistinct(ClickHouseArrayLinqTranslator self, MethodCallExpression call, out SqlExpression translated)
        => self.TryTranslateUnaryArrayCall(call, self._arrayTranslator.TranslateDistinct, out translated);

    private static bool TranslateOrderBy(ClickHouseArrayLinqTranslator self, MethodCallExpression call, out SqlExpression translated)
        => self.TryTranslateArrayOrderBy(call, ascending: true, out translated);

    private static bool TranslateOrderByDescending(ClickHouseArrayLinqTranslator self, MethodCallExpression call, out SqlExpression translated)
        => self.TryTranslateArrayOrderBy(call, ascending: false, out translated);

    // ─── Shared dispatch helpers ──────────────────────────────────────────

    /// <summary>
    /// Common shape for one-argument <c>Enumerable</c>-style helpers: get the array source,
    /// pre-filter on shape, visit, type-check, then build the result SqlExpression.
    /// </summary>
    private bool TryTranslateUnaryArrayCall(
        MethodCallExpression call,
        Func<SqlExpression, SqlExpression> build,
        out SqlExpression translated)
    {
        translated = null!;
        if (GetArraySourceCandidate(call) is not { } sourceExpr
            || !LooksLikeArrayColumnAccess(sourceExpr)
            || _visit(sourceExpr) is not SqlExpression arraySql
            || !ClickHouseArrayMethodTranslator.IsClickHouseArray(arraySql))
        {
            return false;
        }

        translated = build(arraySql);
        return true;
    }

    /// <summary>
    /// Two-argument shape: the source array followed by an integer argument (index/count).
    /// Visits both, then hands off to the builder. The integer argument flows through the
    /// standard scalar translator chain so parameter and constant literals materialize via
    /// their normal Int32 mapping.
    /// </summary>
    private bool TryTranslateUnaryArrayCallWithIntArg(
        MethodCallExpression call,
        Func<SqlExpression, SqlExpression, SqlExpression> build,
        out SqlExpression translated)
    {
        translated = null!;
        if (GetArraySourceCandidate(call) is not { } sourceExpr
            || !LooksLikeArrayColumnAccess(sourceExpr)
            || _visit(sourceExpr) is not SqlExpression arraySql
            || !ClickHouseArrayMethodTranslator.IsClickHouseArray(arraySql)
            || _visit(call.Arguments[1]) is not SqlExpression intArgSql)
        {
            return false;
        }

        translated = build(arraySql, intArgSql);
        return true;
    }

    private bool TryTranslateArrayOrderBy(
        MethodCallExpression methodCallExpression,
        bool ascending,
        out SqlExpression translated)
    {
        translated = null!;
        if (GetArraySourceCandidate(methodCallExpression) is not { } sourceExpr
            || !LooksLikeArrayColumnAccess(sourceExpr)
            || _visit(sourceExpr) is not SqlExpression arraySql
            || arraySql.TypeMapping is not ClickHouseArrayTypeMapping arrayMapping)
        {
            return false;
        }

        var lambda = UnwrapLambda(methodCallExpression.Arguments[1]);
        if (lambda is null || lambda.Parameters.Count != 1)
            return false;

        // Identity lambda (x => x) translates to the no-lambda form arraySort(arr) /
        // arrayReverseSort(arr). ClickHouse's no-lambda form sorts by the elements
        // themselves, which is what `OrderBy(x => x)` requests — and emitting it without a
        // synthetic lambda keeps the SQL compact and avoids forcing a type mapping on the
        // body when the identity translator would otherwise need one.
        if (IsIdentityLambda(lambda))
        {
            translated = ascending
                ? _arrayTranslator.TranslateSort(arraySql, keyLambda: null)
                : _arrayTranslator.TranslateReverseSort(arraySql, keyLambda: null);
            return true;
        }

        var arrayLambda = TranslateArrayLambda(lambda, arrayMapping.ElementMapping);
        if (arrayLambda is null)
            return false;

        translated = ascending
            ? _arrayTranslator.TranslateSort(arraySql, arrayLambda)
            : _arrayTranslator.TranslateReverseSort(arraySql, arrayLambda);
        return true;
    }

    private static bool IsIdentityLambda(LambdaExpression lambda)
        => lambda.Body is ParameterExpression p && p == lambda.Parameters[0];

    private bool TryTranslateArrayHigherOrderPredicate(
        MethodCallExpression methodCallExpression,
        string functionName,
        Type returnType,
        out SqlExpression translated)
    {
        translated = null!;
        if (GetArraySourceCandidate(methodCallExpression) is not { } sourceExpr
            || !LooksLikeArrayColumnAccess(sourceExpr)
            || _visit(sourceExpr) is not SqlExpression arraySql
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

    // ─── Source-shape utilities ───────────────────────────────────────────

    /// <summary>
    /// Returns the LINQ expression that represents the array source for
    /// <paramref name="call"/>, or <c>null</c> if the call shape doesn't expose one. Static
    /// <c>Enumerable</c>/<c>Queryable</c> extensions carry the source as <c>Arguments[0]</c>;
    /// instance methods (e.g. a future <c>List&lt;T&gt;.IndexOf</c> translation) would carry
    /// it as <c>Object</c>. <c>Object</c>-first precedence keeps instance dispatch correct
    /// once registered — invariant: a given method call cannot meaningfully expose both
    /// (instance methods have null arguments[0] for the receiver, statics have null Object).
    /// </summary>
    private static Expression? GetArraySourceCandidate(MethodCallExpression call)
        => call.Object ?? (call.Arguments.Count > 0 ? call.Arguments[0] : null);

    /// <summary>
    /// Structural pre-filter: only attempt array-column translation when the source is a
    /// shape we can safely visit. Eagerly visiting an EF Core query root expression
    /// (e.g. <c>ctx.Set&lt;T&gt;()</c>) trips an <c>EnumerableExpression</c> assertion
    /// inside the base queryable visitor, so we restrict to:
    /// <list type="bullet">
    ///   <item>Direct member access (<c>e.IntArray</c>) with a collection-shaped CLR type.</item>
    ///   <item><c>EF.Property&lt;T&gt;(entity, "Name")</c> — EF Core's preprocessor rewrite of property reads.</item>
    ///   <item>Calls to known array-producing helpers in <see cref="ArrayProducingMethods"/>.</item>
    /// </list>
    /// </summary>
    private static bool LooksLikeArrayColumnAccess(Expression source)
    {
        // Strip AsQueryable/AsEnumerable wrappers structurally before deciding.
        while (source is MethodCallExpression { Method.IsGenericMethod: true } call
               && (call.Method.GetGenericMethodDefinition() == QueryableAsQueryableMethod
                   || call.Method.GetGenericMethodDefinition() == EnumerableAsEnumerableMethod))
        {
            source = call.Arguments[0];
        }

        return source switch
        {
            MemberExpression member when IsCollectionShape(member.Type) => true,
            MethodCallExpression { Method: { IsStatic: true, Name: "Property", DeclaringType.Name: "EF" } } efProp
                when efProp.Method.IsGenericMethod => true,
            MethodCallExpression { Method.IsGenericMethod: true } chained
                when ArrayProducingMethods.Contains(chained.Method.GetGenericMethodDefinition()) => true,
            _ => false
        };
    }

    /// <summary>
    /// Recognizes the CLR-side surfaces that <c>ClickHouseArrayTypeMapping</c> covers
    /// via <c>EnumerableToArrayConverter&lt;TCollection, T&gt;</c>: arrays, <c>List&lt;T&gt;</c>,
    /// and the interfaces that ClickHouseTypeMappingSource maps to <c>Array(T)</c>
    /// (<c>IEnumerable&lt;T&gt;</c>, <c>IList&lt;T&gt;</c>, <c>ICollection&lt;T&gt;</c>,
    /// <c>IReadOnlyList&lt;T&gt;</c>, <c>IReadOnlyCollection&lt;T&gt;</c>). Used as a fast,
    /// type-mapping-free narrowing before we eagerly visit the source.
    /// </summary>
    private static bool IsCollectionShape(Type type)
    {
        if (type.IsArray)
            return true;

        if (!type.IsGenericType)
            return false;

        var def = type.GetGenericTypeDefinition();
        return def == typeof(List<>)
            || def == typeof(IEnumerable<>)
            || def == typeof(IList<>)
            || def == typeof(ICollection<>)
            || def == typeof(IReadOnlyList<>)
            || def == typeof(IReadOnlyCollection<>);
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

    /// <summary>
    /// <c>Enumerable.ElementAt</c> / <c>ElementAtOrDefault</c> ship two overloads — one
    /// taking <c>int</c> and one taking <see cref="Index"/>. We only translate the <c>int</c>
    /// form; the <see cref="Index"/> form falls through to base translation.
    /// </summary>
    private static MethodInfo GetEnumerableElementAtMethod(string name)
        => typeof(Enumerable).GetRuntimeMethods()
            .Single(m => m.Name == name && m.IsGenericMethod
                && m.GetParameters() is { Length: 2 } ps && ps[1].ParameterType == typeof(int));

    private static MethodInfo GetQueryableElementAtMethod(string name)
        => typeof(Queryable).GetRuntimeMethods()
            .Single(m => m.Name == name && m.IsGenericMethod
                && m.GetParameters() is { Length: 2 } ps && ps[1].ParameterType == typeof(int));

    /// <summary>
    /// <c>Enumerable.Take</c> ships an <c>int</c> overload and (since .NET 6) a
    /// <see cref="Range"/> overload. Match only the <c>int</c> form.
    /// </summary>
    private static MethodInfo GetEnumerableTakeIntMethod()
        => typeof(Enumerable).GetRuntimeMethods()
            .Single(m => m.Name == nameof(Enumerable.Take) && m.IsGenericMethod
                && m.GetParameters() is { Length: 2 } ps && ps[1].ParameterType == typeof(int));

    private static MethodInfo GetQueryableTakeIntMethod()
        => typeof(Queryable).GetRuntimeMethods()
            .Single(m => m.Name == nameof(Queryable.Take) && m.IsGenericMethod
                && m.GetParameters() is { Length: 2 } ps && ps[1].ParameterType == typeof(int));

    /// <summary>
    /// Picks <c>Enumerable.Reverse&lt;T&gt;</c> by source parameter shape — either
    /// <c>IEnumerable&lt;T&gt;</c> (classic) or <c>T[]</c> (.NET 9+). C# picks whichever
    /// matches at the call site, so dispatch must match the actually-bound MethodInfo.
    /// </summary>
    private static MethodInfo GetEnumerableReverseMethod(Type sourceDefinition)
        => typeof(Enumerable).GetRuntimeMethods()
            .Single(m => m.Name == nameof(Enumerable.Reverse) && m.IsGenericMethod
                && m.GetParameters() is { Length: 1 } ps
                && (sourceDefinition == typeof(IEnumerable<>)
                    ? ps[0].ParameterType.IsGenericType && ps[0].ParameterType.GetGenericTypeDefinition() == typeof(IEnumerable<>)
                    : ps[0].ParameterType.IsArray));

    private static MethodInfo? TryGetEnumerableReverseArrayMethod()
        => typeof(Enumerable).GetRuntimeMethods()
            .SingleOrDefault(m => m.Name == nameof(Enumerable.Reverse) && m.IsGenericMethod
                && m.GetParameters() is { Length: 1 } ps && ps[0].ParameterType.IsArray);

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
