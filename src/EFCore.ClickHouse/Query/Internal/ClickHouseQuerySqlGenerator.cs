using System.Linq.Expressions;
using ClickHouse.EntityFrameworkCore.Query.Expressions.Internal;
using ClickHouse.EntityFrameworkCore.Storage.Internal;
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
            _ => base.VisitExtension(extensionExpression)
        };

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
