using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace ClickHouse.EntityFrameworkCore.Query.Internal;

public class ClickHouseQueryableMethodTranslatingExpressionVisitor
    : RelationalQueryableMethodTranslatingExpressionVisitor
{
    private readonly RelationalSqlTranslatingExpressionVisitor _sqlTranslator;

    public ClickHouseQueryableMethodTranslatingExpressionVisitor(
        QueryableMethodTranslatingExpressionVisitorDependencies dependencies,
        RelationalQueryableMethodTranslatingExpressionVisitorDependencies relationalDependencies,
        RelationalQueryCompilationContext queryCompilationContext)
        : base(dependencies, relationalDependencies, queryCompilationContext)
    {
        _sqlTranslator = relationalDependencies.RelationalSqlTranslatingExpressionVisitorFactory
            .Create(queryCompilationContext, this);
    }

    protected override ShapedQueryExpression? TranslateTake(ShapedQueryExpression source, Expression count)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(count);

        var selectExpression = (SelectExpression)source.QueryExpression;
        var translation = _sqlTranslator.Translate(count);

        if (translation != null)
        {
            selectExpression.ApplyLimit(translation);
            return source;
        }

        return null;
    }
}
