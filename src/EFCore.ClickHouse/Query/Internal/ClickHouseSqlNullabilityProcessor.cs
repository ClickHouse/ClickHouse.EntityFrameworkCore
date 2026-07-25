using System.Linq.Expressions;
using ClickHouse.EntityFrameworkCore.Query.Expressions.Internal;
using ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Query.Internal;

public class ClickHouseSqlNullabilityProcessor : SqlNullabilityProcessor
{
    // Element CLR types whose collections ClickHouse.Driver serializes correctly as a single bound
    // array parameter value (verified empirically against the pinned driver). Anything not listed here
    // falls back to EF Core's per-element expansion, which serializes each element on its own — so an
    // unlisted or newly-mapped type stays correct (just unoptimized) rather than failing at runtime.
    //   • DateTime covers DateTime/DateTime64; DateOnly covers Date/Date32; BigInteger covers
    //     Int128/Int256/UInt128/UInt256; IPAddress covers IPv4/IPv6.
    //   • TimeSpan (Time/Time64) is deliberately excluded: the driver emits its array elements without
    //     the quoting ClickHouse needs, so `Array(Time)` parameters fail to parse (CANNOT_READ_ARRAY).
    private static readonly HashSet<Type> ArrayParameterElementTypes =
    [
        typeof(byte), typeof(sbyte), typeof(short), typeof(ushort),
        typeof(int), typeof(uint), typeof(long), typeof(ulong),
        typeof(float), typeof(double), typeof(decimal),
        typeof(bool), typeof(string), typeof(Guid),
        typeof(DateTime), typeof(DateOnly),
        typeof(System.Numerics.BigInteger), typeof(System.Net.IPAddress),
    ];

    public ClickHouseSqlNullabilityProcessor(
        RelationalParameterBasedSqlProcessorDependencies dependencies,
        RelationalParameterBasedSqlProcessorParameters parameters)
        : base(dependencies, parameters)
    {
    }

    /// <summary>
    /// Rewrites <c>column IN {collectionParameter}</c> — a captured <c>int[]</c>/<c>List&lt;T&gt;</c>/etc.
    /// used with <c>Contains</c> — into <c>has({p:Array(T)}, column)</c>, binding the whole collection
    /// as a single native ClickHouse array parameter instead of EF Core's default one-scalar-parameter-
    /// per-element expansion (<c>IN (p1, …, pN)</c>).
    /// <para>
    /// A single bound array avoids the parameter-count / query-size ceilings that large <c>IN</c> lists
    /// hit, and keeps the query text (and plan-cache key) independent of the collection size — ClickHouse
    /// is OLAP and does not reuse plans by parameterization, so there is no downside to a bound array over
    /// inlined constants.
    /// </para>
    /// <para>
    /// This is the provider default for a plain captured collection. Per-query overrides win: EF Core
    /// wires the marker methods to <see cref="SqlParameterExpression.TranslationMode"/>, so
    /// <c>EF.MultipleParameters(...)</c> keeps the one-parameter-per-element expansion and
    /// <c>EF.Constant(...)</c> inlines the values as literals — both handled by the base implementation.
    /// The model-wide <c>UseParameterizedCollectionMode</c> knob is intentionally not consulted here: it
    /// also governs the collection-as-queryable path (joins, <c>Where(...).Contains(...)</c>), which
    /// ClickHouse translates via <c>SELECT … UNION ALL …</c> and which does not support
    /// <see cref="ParameterTranslationMode.Parameter"/>. Inline value lists and subquery <c>IN</c> are
    /// likewise left to the base implementation.
    /// </para>
    /// </summary>
    protected override SqlExpression VisitIn(
        InExpression inExpression,
        bool allowOptimizedExpansion,
        out bool nullable)
    {
        // An unmarked collection parameter (TranslationMode == null) takes the provider default of a
        // single array parameter; an explicit EF.Parameter(...) selects it too. EF.MultipleParameters
        // and EF.Constant carry a non-matching TranslationMode and fall through to the base expansion.
        if (inExpression.ValuesParameter is { } valuesParameter
            && valuesParameter.TranslationMode is null or ParameterTranslationMode.Parameter)
        {
            return VisitCollectionParameterIn(inExpression, valuesParameter, allowOptimizedExpansion, out nullable);
        }

        return base.VisitIn(inExpression, allowOptimizedExpansion, out nullable);
    }

    private SqlExpression VisitCollectionParameterIn(
        InExpression inExpression,
        SqlParameterExpression valuesParameter,
        bool allowOptimizedExpansion,
        out bool nullable)
    {
        // Process the tested item (usually a column) for nullability first, mirroring the base
        // VisitIn contract.
        var item = Visit(inExpression.Item, out var itemNullable);

        // The tested item carries the authoritative element store type (the column's mapping). Align
        // the array parameter's element type to it so the parameter serializes with the column's
        // ClickHouse type (Int64 vs Int32, FixedString(N) vs String, …).
        var elementMapping = (item.TypeMapping ?? valuesParameter.TypeMapping?.ElementTypeMapping) as RelationalTypeMapping;

        // Bail to the base per-element expansion when a single native array parameter would be wrong
        // or unserializable:
        //  - nullable item: `has(arr, item)` yields a concrete 0 for a NULL item rather than NULL, so
        //    `NOT has(...)` would KEEP NULL rows whereas `x NOT IN (...)` drops them (SQL 3-valued
        //    logic). The base path handles null compensation, so defer to it for nullable columns; the
        //    common large-list case (non-nullable keys) still gets the array parameter.
        //  - no element mapping → no store type to build Array(T) from;
        //  - the element needs a value converter (e.g. a CLR enum → Enum8) → the whole collection is
        //    handed to the driver un-converted, which it can't serialize;
        //  - the element CLR type isn't one the driver serializes correctly inside an array (see
        //    ArrayParameterElementTypes — notably TimeSpan/Time is excluded).
        // The base expansion serializes each element individually, so all these cases still work.
        if (itemNullable
            || elementMapping is null
            || elementMapping.Converter is not null
            || !ArrayParameterElementTypes.Contains(Nullable.GetUnderlyingType(elementMapping.ClrType) ?? elementMapping.ClrType))
        {
            return base.VisitIn(inExpression, allowOptimizedExpansion, out nullable);
        }

        // `has(array, non-null item)` is never NULL and matches `item IN (...)` for a non-null item.
        nullable = false;

        var arrayMapping = new ClickHouseArrayTypeMapping(elementMapping);
        var arrayParameter = valuesParameter.ApplyTypeMapping(arrayMapping);
        var alignedItem = Dependencies.SqlExpressionFactory.ApplyTypeMapping(item, elementMapping)!;

        return Dependencies.SqlExpressionFactory.Function(
            "has",
            [arrayParameter, alignedItem],
            nullable: false,
            argumentsPropagateNullability: [false, false],
            typeof(bool),
            Dependencies.TypeMappingSource.FindMapping(typeof(bool)));
    }

    protected override SqlExpression VisitSqlBinary(
        SqlBinaryExpression sqlBinaryExpression,
        bool allowOptimizedExpansion,
        out bool nullable)
    {
        return sqlBinaryExpression switch
        {
            {
                OperatorType: ExpressionType.Equal or ExpressionType.NotEqual,
                Left: ClickHouseRowValueExpression leftRowValue,
                Right: ClickHouseRowValueExpression rightRowValue
            }
                => VisitRowValueComparison(sqlBinaryExpression.OperatorType, leftRowValue, rightRowValue, out nullable),

            _ => base.VisitSqlBinary(sqlBinaryExpression, allowOptimizedExpansion, out nullable)
        };

        SqlExpression VisitRowValueComparison(
            ExpressionType operatorType,
            ClickHouseRowValueExpression leftRowValue,
            ClickHouseRowValueExpression rightRowValue,
            out bool nullable)
        {
            if (leftRowValue.Values.Count != rightRowValue.Values.Count)
                throw new InvalidOperationException("Row value comparison requires matching tuple lengths.");

            var count = leftRowValue.Values.Count;

            SqlExpression? expandedExpression = null;
            List<SqlExpression>? visitedLeftValues = null;
            List<SqlExpression>? visitedRightValues = null;

            for (var i = 0; i < count; i++)
            {
                var leftValue = leftRowValue.Values[i];
                var rightValue = rightRowValue.Values[i];
                var visitedLeftValue = VisitRowValueOperand(leftValue, out var leftNullable);
                var visitedRightValue = VisitRowValueOperand(rightValue, out var rightNullable);

                if (!leftNullable && !rightNullable
                    || allowOptimizedExpansion && operatorType is ExpressionType.Equal && (!leftNullable || !rightNullable))
                {
                    if (visitedLeftValue != leftValue && visitedLeftValues is null)
                        visitedLeftValues = SliceToList(leftRowValue.Values, count, i);

                    visitedLeftValues?.Add(visitedLeftValue);

                    if (visitedRightValue != rightValue && visitedRightValues is null)
                        visitedRightValues = SliceToList(rightRowValue.Values, count, i);

                    visitedRightValues?.Add(visitedRightValue);

                    continue;
                }

                var valueBinary = Dependencies.SqlExpressionFactory.MakeBinary(
                    operatorType, visitedLeftValue, visitedRightValue, typeMapping: null, existingExpression: sqlBinaryExpression)!;

                var valueBinaryExpression = ParametersDecorator is null
                    ? valueBinary
                    : Visit(valueBinary, allowOptimizedExpansion, out _);

                if (expandedExpression is null)
                {
                    visitedLeftValues = SliceToList(leftRowValue.Values, count, i);
                    visitedRightValues = SliceToList(rightRowValue.Values, count, i);

                    expandedExpression = valueBinaryExpression;
                }
                else
                {
                    expandedExpression = operatorType switch
                    {
                        ExpressionType.Equal => Dependencies.SqlExpressionFactory.AndAlso(expandedExpression, valueBinaryExpression),
                        ExpressionType.NotEqual => Dependencies.SqlExpressionFactory.OrElse(expandedExpression, valueBinaryExpression),
                        _ => throw new InvalidOperationException("Only row-value equality operators are supported.")
                    };
                }
            }

            // Row comparison expressions themselves are not nullable; null compensation is represented
            // in the expanded binary expressions above.
            nullable = false;

            if (expandedExpression is null)
            {
                return visitedLeftValues is null && visitedRightValues is null
                    ? sqlBinaryExpression
                    : Dependencies.SqlExpressionFactory.MakeBinary(
                        operatorType,
                        visitedLeftValues is null
                            ? leftRowValue
                            : new ClickHouseRowValueExpression(visitedLeftValues, leftRowValue.Type, leftRowValue.TypeMapping),
                        visitedRightValues is null
                            ? rightRowValue
                            : new ClickHouseRowValueExpression(visitedRightValues, rightRowValue.Type, rightRowValue.TypeMapping),
                        typeMapping: null,
                        existingExpression: sqlBinaryExpression)!;
            }

            if (visitedLeftValues is null || visitedRightValues is null)
                throw new InvalidOperationException("Internal row-value expansion state is invalid.");

            if (visitedLeftValues.Count is 0)
                return expandedExpression;

            var unexpandedExpression = visitedLeftValues.Count is 1
                ? Dependencies.SqlExpressionFactory.MakeBinary(operatorType, visitedLeftValues[0], visitedRightValues[0], typeMapping: null)!
                : Dependencies.SqlExpressionFactory.MakeBinary(
                    operatorType,
                    new ClickHouseRowValueExpression(visitedLeftValues, leftRowValue.Type, leftRowValue.TypeMapping),
                    new ClickHouseRowValueExpression(visitedRightValues, rightRowValue.Type, rightRowValue.TypeMapping),
                    typeMapping: null)!;

            return Dependencies.SqlExpressionFactory.MakeBinary(
                operatorType: operatorType switch
                {
                    ExpressionType.Equal => ExpressionType.AndAlso,
                    ExpressionType.NotEqual => ExpressionType.OrElse,
                    _ => throw new InvalidOperationException("Only row-value equality operators are supported.")
                },
                unexpandedExpression,
                expandedExpression,
                typeMapping: null)!;

            static List<SqlExpression> SliceToList(IReadOnlyList<SqlExpression> source, int capacity, int count)
            {
                var list = new List<SqlExpression>(capacity);

                for (var i = 0; i < count; i++)
                    list.Add(source[i]);

                return list;
            }
        }
    }

    protected override SqlExpression VisitCustomSqlExpression(
        SqlExpression sqlExpression,
        bool allowOptimizedExpansion,
        out bool nullable)
        => sqlExpression switch
        {
            ClickHouseJsonPathExpression e => VisitJsonPathExpression(e, allowOptimizedExpansion, out nullable),
            ClickHouseJsonArrayIndexExpression e => VisitJsonArrayIndexExpression(e, allowOptimizedExpansion, out nullable),
            ClickHouseRowValueExpression e => VisitRowValueExpression(e, out nullable),
            ClickHouseArrayLambdaReferenceExpression e => VisitArrayLambdaReference(e, out nullable),
            ClickHouseArrayLambdaExpression e => VisitArrayLambda(e, allowOptimizedExpansion, out nullable),
            _ => base.VisitCustomSqlExpression(sqlExpression, allowOptimizedExpansion, out nullable)
        };
    
    private SqlExpression VisitJsonPathExpression(
        ClickHouseJsonPathExpression expression,
        bool allowOptimizedExpansion,
        out bool nullable)
    {
        var newInstance = Visit(expression.Instance, allowOptimizedExpansion, out _);
        nullable = true;
        return expression.Update(newInstance);
    }

    private SqlExpression VisitJsonArrayIndexExpression(
        ClickHouseJsonArrayIndexExpression expression,
        bool allowOptimizedExpansion,
        out bool nullable)
    {
        var newInstance = Visit(expression.Instance, allowOptimizedExpansion, out _);
        nullable = true;
        return expression.Update(newInstance);
    }

    /// <summary>
    /// The lambda parameter inside an array lambda iterates over array elements; it has no
    /// distinct "row null" of its own. Element nullability is captured by the enclosing array
    /// column's type mapping, not at the parameter reference site.
    /// </summary>
    private static SqlExpression VisitArrayLambdaReference(
        ClickHouseArrayLambdaReferenceExpression referenceExpression,
        out bool nullable)
    {
        nullable = false;
        return referenceExpression;
    }

    /// <summary>
    /// A lambda is not itself a value — it's an argument to higher-order array functions.
    /// Visit the body so any nested null-handling rewrites apply, but report
    /// <paramref name="nullable"/> against the lambda as non-nullable.
    /// </summary>
    private SqlExpression VisitArrayLambda(
        ClickHouseArrayLambdaExpression lambdaExpression,
        bool allowOptimizedExpansion,
        out bool nullable)
    {
        var visitedBody = Visit(lambdaExpression.Body, allowOptimizedExpansion, out _);
        nullable = false;
        return lambdaExpression.Update(lambdaExpression.Parameter, visitedBody);
    }


    private SqlExpression VisitRowValueExpression(ClickHouseRowValueExpression rowValueExpression, out bool nullable)
    {
        SqlExpression[]? newValues = null;

        for (var i = 0; i < rowValueExpression.Values.Count; i++)
        {
            var value = rowValueExpression.Values[i];
            var newValue = VisitRowValueOperand(value, out _);
            if (newValue != value && newValues is null)
            {
                newValues = new SqlExpression[rowValueExpression.Values.Count];
                for (var j = 0; j < i; j++)
                    newValues[j] = rowValueExpression.Values[j];
            }
            if (newValues is not null)
                newValues[i] = newValue;
        }

        nullable = false;
        return rowValueExpression.Update(newValues ?? rowValueExpression.Values);
    }

    private SqlExpression VisitRowValueOperand(SqlExpression operand, out bool nullable)
    {
        if (ParametersDecorator is null && operand is SqlParameterExpression parameterExpression)
        {
            nullable = parameterExpression.IsNullable;
            return parameterExpression;
        }

        return Visit(operand, out nullable);
    }
}
