using System.Linq.Expressions;
using ClickHouse.EntityFrameworkCore.Query.Expressions.Internal;
using ClickHouse.EntityFrameworkCore.Storage.Internal;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Query.Internal;

public class ClickHouseQuerySqlGenerator : QuerySqlGenerator
{
    public ClickHouseQuerySqlGenerator(QuerySqlGeneratorDependencies dependencies)
        : base(dependencies)
    {
    }

    protected override Expression VisitExtension(Expression extensionExpression)
        => extensionExpression switch
        {
            ClickHouseRowValueExpression e => VisitRowValue(e),
            ScalarSubqueryExpression e when IsNonNullableZeroDefaultAggregateSubquery(e) => VisitNonNullableScalarSubquery(e),
            _ => base.VisitExtension(extensionExpression)
        };

    private static bool IsNonNullableValueType(Type type)
        => type.IsValueType && Nullable.GetUnderlyingType(type) == null;

    /// <summary>
    /// Returns true when a scalar subquery projects an aggregate whose LINQ semantic for an
    /// empty input is "0" — specifically <c>Count</c>/<c>LongCount</c>/<c>Sum</c>, possibly
    /// wrapped in EF Core's <c>COALESCE(aggregate, 0)</c> pattern. Only these cases are safe
    /// to wrap with <c>ifNull(..., 0)</c>; other aggregates (Min/Max/Average) and non-aggregate
    /// projections (FirstOrDefault on DateTime, Guid, tuples, …) must NOT be wrapped,
    /// because (a) 0 is type-incorrect for non-numeric types and (b) "no row" ≠ "zero" for
    /// Min/Max/Average semantics.
    /// </summary>
    private static bool IsNonNullableZeroDefaultAggregateSubquery(ScalarSubqueryExpression expression)
    {
        if (!IsNonNullableValueType(expression.Type))
            return false;

        var projections = expression.Subquery.Projection;
        if (projections.Count != 1)
            return false;

        return IsZeroDefaultAggregate(projections[0].Expression);
    }

    private static bool IsZeroDefaultAggregate(SqlExpression expression)
    {
        if (expression is not SqlFunctionExpression fn)
            return false;

        if (IsCountOrSumName(fn.Name))
            return true;

        // EF Core wraps non-nullable numeric aggregates as COALESCE(aggregate, 0). ClickHouse's
        // scalar subquery still returns NULL for empty input despite the inner COALESCE, so we
        // still need the outer ifNull wrap.
        if (string.Equals(fn.Name, "COALESCE", StringComparison.OrdinalIgnoreCase)
            && fn.Arguments is { Count: > 0 } args)
        {
            return IsZeroDefaultAggregate(args[0]);
        }

        return false;
    }

    private static bool IsCountOrSumName(string name)
        => string.Equals(name, "COUNT", StringComparison.OrdinalIgnoreCase)
        || string.Equals(name, "SUM", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// ClickHouse requires explicit ALL or DISTINCT for UNION
    /// (<c>union_default_mode</c> is empty by default, rejecting bare <c>UNION</c>).
    /// Always emit an explicit modifier for all set operations.
    /// </summary>
    protected override void GenerateSetOperation(SetOperationBase setOperation)
    {
        GenerateSetOperationOperand(setOperation, setOperation.Source1);
        Sql.AppendLine();

        var keyword = setOperation switch
        {
            ExceptExpression => "EXCEPT",
            IntersectExpression => "INTERSECT",
            UnionExpression => "UNION",
            _ => throw new InvalidOperationException(
                $"Unknown set operation type: {setOperation.GetType().Name}")
        };

        Sql.AppendLine(keyword + (setOperation.IsDistinct ? " DISTINCT" : " ALL"));
        GenerateSetOperationOperand(setOperation, setOperation.Source2);
    }

    protected override void GenerateLimitOffset(SelectExpression selectExpression)
    {
        switch (selectExpression)
        {
            case { Limit: not null, Offset: not null }:
                Sql.AppendLine().Append("LIMIT ");
                Visit(selectExpression.Limit);
                Sql.AppendLine().Append(" OFFSET ");
                Visit(selectExpression.Offset);
                break;

            case { Limit: not null }:
                Sql.AppendLine().Append("LIMIT ");
                Visit(selectExpression.Limit);
                break;

            case { Offset: not null }:
                Sql.AppendLine().Append("OFFSET ");
                Visit(selectExpression.Offset);
                break;
        }
    }

    protected override void GenerateTop(SelectExpression selectExpression)
    {
        // ClickHouse uses LIMIT, not TOP
    }

    protected override Expression VisitSqlParameter(SqlParameterExpression sqlParameterExpression)
    {
        var parameterName = sqlParameterExpression.Name;

        if (Sql.Parameters.All(p => p.InvariantName != parameterName))
        {
            Sql.AddParameter(
                parameterName,
                Dependencies.SqlGenerationHelper.GenerateParameterName(parameterName),
                sqlParameterExpression.TypeMapping!,
                sqlParameterExpression.IsNullable);
        }

        var storeType = sqlParameterExpression.TypeMapping!.StoreType;
        var helper = (ClickHouseSqlGenerationHelper)Dependencies.SqlGenerationHelper;
        Sql.Append(helper.GenerateParameterName(parameterName, storeType));

        return sqlParameterExpression;
    }

    /// <summary>
    /// ClickHouse <c>COUNT()</c> returns <c>UInt64</c>, but EF Core expects <c>Int32</c> or <c>Int64</c>.
    /// Emit an explicit <c>CAST</c> so that set operations (UNION/INTERSECT/EXCEPT) can find
    /// a common supertype between COUNT results and other integer columns.
    /// </summary>
    protected override Expression VisitSqlFunction(SqlFunctionExpression sqlFunctionExpression)
    {
        if (string.Equals(sqlFunctionExpression.Name, "COUNT", StringComparison.OrdinalIgnoreCase))
        {
            var targetType = sqlFunctionExpression.Type == typeof(long) ? "Int64" : "Int32";
            Sql.Append("CAST(");
            base.VisitSqlFunction(sqlFunctionExpression);
            Sql.Append($" AS {targetType})");
            return sqlFunctionExpression;
        }

        return base.VisitSqlFunction(sqlFunctionExpression);
    }

    /// <summary>
    /// ClickHouse sorts NULLs last in ascending order and first in descending order,
    /// which is the opposite of .NET/SQL Server semantics (null &lt; non-null).
    /// Emit explicit <c>NULLS FIRST</c> / <c>NULLS LAST</c> for nullable expressions only.
    ///
    /// Skip non-nullable value types: ClickHouse treats <c>NaN</c> in float columns the
    /// same as <c>NULL</c> for ordering purposes, so adding <c>NULLS FIRST</c> on a
    /// non-nullable <c>double</c> column would move <c>NaN</c> to the front — a semantic
    /// change unrelated to the null-handling fix we're making for nullable columns.
    /// </summary>
    protected override Expression VisitOrdering(OrderingExpression orderingExpression)
    {
        var result = base.VisitOrdering(orderingExpression);

        if (IsNullable(orderingExpression.Expression))
            Sql.Append(orderingExpression.IsAscending ? " NULLS FIRST" : " NULLS LAST");

        return result;
    }

    private static bool IsNullable(SqlExpression expression)
    {
        var type = expression.Type;
        if (!type.IsValueType)
            return true;
        return Nullable.GetUnderlyingType(type) is not null;
    }

    protected override Expression VisitSqlBinary(SqlBinaryExpression sqlBinaryExpression)
    {
        if (sqlBinaryExpression.OperatorType == ExpressionType.Add
            && sqlBinaryExpression.Type == typeof(string))
        {
            Sql.Append("concat(");
            Visit(sqlBinaryExpression.Left);
            Sql.Append(", ");
            Visit(sqlBinaryExpression.Right);
            Sql.Append(")");
            return sqlBinaryExpression;
        }

        return base.VisitSqlBinary(sqlBinaryExpression);
    }

    /// <summary>
    /// ClickHouse returns NULL for scalar subqueries whose inner query produces no rows,
    /// even for aggregates like <c>COUNT</c> / <c>SUM</c> that return 0 in standard SQL.
    /// Wraps the non-nullable zero-default aggregate scalar-subquery shape with
    /// <c>ifNull(subquery, 0)</c>. The fallback literal 0 is only valid because the caller
    /// guarantees the inner projection is a COUNT or SUM (possibly inside an EF-emitted
    /// <c>COALESCE(..., 0)</c>) with a non-nullable numeric return type — see
    /// <see cref="IsNonNullableZeroDefaultAggregateSubquery"/>.
    /// </summary>
    protected virtual Expression VisitNonNullableScalarSubquery(ScalarSubqueryExpression expression)
    {
        Sql.Append("ifNull(");
        base.VisitExtension(expression);
        Sql.Append(", 0)");
        return expression;
    }

    /// <summary>
    /// ClickHouse does not accept SQL standard <c>VALUES (...)</c> as a subquery source.
    /// Emit each row as a separate <c>SELECT ... UNION ALL SELECT ...</c> instead.
    /// </summary>
    protected override void GenerateValues(ValuesExpression valuesExpression)
    {
        if (valuesExpression.RowValues is null)
            throw new InvalidOperationException(
                "ValuesExpression.RowValues is null; parameter-based VALUES expansion should run before SQL generation.");

        if (valuesExpression.RowValues.Count == 0)
            throw new InvalidOperationException(RelationalStrings.EmptyCollectionNotSupportedAsInlineQueryRoot);

        var rowValues = valuesExpression.RowValues;
        var columnNames = valuesExpression.ColumnNames;

        for (var i = 0; i < rowValues.Count; i++)
        {
            if (i > 0)
                Sql.AppendLine().Append("UNION ALL ");

            Sql.Append("SELECT ");
            var values = rowValues[i].Values;
            for (var j = 0; j < values.Count; j++)
            {
                if (j > 0)
                    Sql.Append(", ");
                Visit(values[j]);
                // Emit column aliases on every row; ClickHouse permits this and
                // it keeps the UNION ALL branches structurally identical.
                Sql.Append(AliasSeparator)
                   .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(columnNames[j]));
            }
        }
    }

    protected virtual Expression VisitRowValue(ClickHouseRowValueExpression rowValueExpression)
    {
        Sql.Append("(");
        var values = rowValueExpression.Values;
        for (var i = 0; i < values.Count; i++)
        {
            Visit(values[i]);
            if (i < values.Count - 1)
                Sql.Append(", ");
        }
        Sql.Append(")");
        return rowValueExpression;
    }
}
