using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore;
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

    public override bool IsEvaluatableExpression(Expression expression, IModel model) => expression switch
    {
        MethodCallExpression methodCallExpression when methodCallExpression.Method.DeclaringType ==
                                                       typeof(ClickHouseJsonDbFunctionsExtensions) => false,
        NewExpression newExpression => !newExpression.Type.IsAssignableTo(typeof(ITuple)),
        _ => base.IsEvaluatableExpression(expression, model)
    };
}