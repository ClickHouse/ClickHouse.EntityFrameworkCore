using System.Linq.Expressions;
using ClickHouse.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;

namespace ClickHouse.EntityFrameworkCore.Metadata.Conventions;

/// <summary>
/// Rewrites <see cref="ClickHouseDbSetExtensions.Final{TEntity}"/> calls in query filters to a
/// <see cref="ClickHouseFinalQueryRootExpression"/>, as the relational convention does for <c>FromSql</c>.
/// </summary>
public class ClickHouseQueryFilterRewritingConvention : RelationalQueryFilterRewritingConvention
{
    public ClickHouseQueryFilterRewritingConvention(
        ProviderConventionSetBuilderDependencies dependencies,
        RelationalConventionSetBuilderDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
        DbSetAccessRewriter = new ClickHouseDbSetAccessRewritingExpressionVisitor(Dependencies.ContextType);
    }

    protected class ClickHouseDbSetAccessRewritingExpressionVisitor : RelationalDbSetAccessRewritingExpressionVisitor
    {
        public ClickHouseDbSetAccessRewritingExpressionVisitor(Type contextType)
            : base(contextType)
        {
        }

        protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
        {
            if (methodCallExpression.Method.DeclaringType == typeof(ClickHouseDbSetExtensions)
                && methodCallExpression.Method.Name == nameof(ClickHouseDbSetExtensions.Final))
            {
                var source = (EntityQueryRootExpression)Visit(methodCallExpression.Arguments[0]);

                return new ClickHouseFinalQueryRootExpression(source.EntityType);
            }

            return base.VisitMethodCall(methodCallExpression);
        }
    }
}
