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
    // Element CLR types that the driver formats correctly inside an array parameter. Other types use the
    // EF Core expansion, which formats each element on its own. DateTime covers DateTime and DateTime64,
    // DateOnly covers Date and Date32, BigInteger covers the 128-bit and 256-bit integers, and IPAddress
    // covers IPv4 and IPv6. TimeSpan (Time, Time64) is not in the list: the driver does not quote these
    // elements, and the server fails to read the array (CANNOT_READ_ARRAY_FROM_TEXT).
    private static readonly HashSet<Type> ArrayParameterElementTypes =
    [
        typeof(byte), typeof(sbyte), typeof(short), typeof(ushort),
        typeof(int), typeof(uint), typeof(long), typeof(ulong),
        typeof(float), typeof(double), typeof(decimal),
        typeof(bool), typeof(string), typeof(Guid),
        typeof(DateTime), typeof(DateOnly),
        typeof(System.Numerics.BigInteger), typeof(System.Net.IPAddress),
    ];

    private readonly ParameterTranslationMode? _explicitCollectionParameterTranslationMode;

    public ClickHouseSqlNullabilityProcessor(
        RelationalParameterBasedSqlProcessorDependencies dependencies,
        RelationalParameterBasedSqlProcessorParameters parameters)
        : this(dependencies, parameters, explicitCollectionParameterTranslationMode: null)
    {
    }

    /// <param name="dependencies">The processor dependencies.</param>
    /// <param name="parameters">The processor parameters.</param>
    /// <param name="explicitCollectionParameterTranslationMode">
    /// The mode that the application set with <c>UseParameterizedCollectionMode</c>, or <see langword="null"/>
    /// when it did not set one.
    /// </param>
    public ClickHouseSqlNullabilityProcessor(
        RelationalParameterBasedSqlProcessorDependencies dependencies,
        RelationalParameterBasedSqlProcessorParameters parameters,
        ParameterTranslationMode? explicitCollectionParameterTranslationMode)
        : base(dependencies, parameters)
    {
        _explicitCollectionParameterTranslationMode = explicitCollectionParameterTranslationMode;
    }

    /// <summary>
    /// Sends a captured collection in <c>Contains</c> (<c>ids.Contains(x.Id)</c>) as one array parameter:
    /// <c>x.Id IN {ids:Array(Int64)}</c>. EF Core otherwise sends one parameter for each element,
    /// <c>x.Id IN ({ids1:Int64}, {ids2:Int64}, ...)</c>, and ClickHouse refuses a request with too many
    /// parameters. ClickHouse uses the primary key for <c>IN</c> with an array.
    /// </summary>
    /// <remarks>
    /// The provider uses the array parameter when the collection has no translation mode. The EF Core
    /// expansion is used for <c>EF.Constant(...)</c> and <c>EF.MultipleParameters(...)</c>, and for all
    /// collections when the application sets <c>UseParameterizedCollectionMode</c> to
    /// <see cref="ParameterTranslationMode.Constant"/> or <see cref="ParameterTranslationMode.MultipleParameters"/>.
    /// <c>EF.Parameter(...)</c> and <see cref="ParameterTranslationMode.Parameter"/> do not get here:
    /// EF Core translates such a collection as a table, which the provider does not support.
    /// </remarks>
    protected override SqlExpression VisitIn(
        InExpression inExpression,
        bool allowOptimizedExpansion,
        out bool nullable)
    {
        if (inExpression.ValuesParameter is { } valuesParameter
            && (valuesParameter.TranslationMode ?? _explicitCollectionParameterTranslationMode) is null)
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
        var item = Visit(inExpression.Item, out var itemNullable);

        // The mapping of the item, usually a column, gives the element store type, so that the array has
        // the type of the column (Int64 or Int32, FixedString(N) or String, ...).
        var elementMapping = (item.TypeMapping ?? valuesParameter.TypeMapping?.ElementTypeMapping) as RelationalTypeMapping;

        // Use the EF Core expansion in these cases:
        //  - The item can be null. The expansion adds the null checks that give .NET semantics.
        //    ClickHouseArrayParameterTypeMapping also removes null elements, which is correct only for an
        //    item that is never null.
        //  - There is no element mapping, so there is no store type for Array(T).
        //  - The element has a value converter, for example an enum stored as Enum8. The driver gets the
        //    collection before conversion and cannot format it.
        //  - The driver does not format the element type correctly inside an array.
        if (itemNullable
            || elementMapping is null
            || elementMapping.Converter is not null
            || !ArrayParameterElementTypes.Contains(Nullable.GetUnderlyingType(elementMapping.ClrType) ?? elementMapping.ClrType))
        {
            return base.VisitIn(inExpression, allowOptimizedExpansion, out nullable);
        }

        // The item is never null and ClickHouseArrayParameterTypeMapping removes null elements, so the
        // result is never null.
        nullable = false;

        var arrayParameter = (SqlParameterExpression)valuesParameter.ApplyTypeMapping(
            new ClickHouseArrayParameterTypeMapping(elementMapping));
        var alignedItem = Dependencies.SqlExpressionFactory.ApplyTypeMapping(item, elementMapping)!;

        // ClickHouseQuerySqlGenerator writes an IN expression with a values parameter as
        // `item IN {p:Array(T)}`.
        return inExpression.Update(alignedItem, arrayParameter);
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
