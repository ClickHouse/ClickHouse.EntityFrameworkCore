using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;

namespace ClickHouse.EntityFrameworkCore.Query.Internal;

public class ClickHouseEvaluatableExpressionFilter : RelationalEvaluatableExpressionFilter
{
    public ClickHouseEvaluatableExpressionFilter(
        EvaluatableExpressionFilterDependencies dependencies,
        RelationalEvaluatableExpressionFilterDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
    }

    public override bool IsEvaluatableExpression(Expression expression, IModel model)
    {
        if (expression is NewExpression newExpression)
            return !newExpression.Type.IsAssignableTo(typeof(ITuple));

        return base.IsEvaluatableExpression(expression, model);
    }
}
