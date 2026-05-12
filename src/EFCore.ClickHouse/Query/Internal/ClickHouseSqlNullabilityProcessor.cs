using System.Linq.Expressions;
using ClickHouse.EntityFrameworkCore.Query.Expressions.Internal;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace ClickHouse.EntityFrameworkCore.Query.Internal;

public class ClickHouseSqlNullabilityProcessor : SqlNullabilityProcessor
{
    public ClickHouseSqlNullabilityProcessor(
        RelationalParameterBasedSqlProcessorDependencies dependencies,
        RelationalParameterBasedSqlProcessorParameters parameters)
        : base(dependencies, parameters)
    {
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
            _ => base.VisitCustomSqlExpression(sqlExpression, allowOptimizedExpansion, out nullable)
        };
    
    private SqlExpression VisitJsonPathExpression(
        ClickHouseJsonPathExpression expression, 
        bool allowOptimizedExpansion, 
        out bool nullable)
    {

        var newInstance = Visit(expression.Instance, allowOptimizedExpansion, out var instanceNullable);
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
