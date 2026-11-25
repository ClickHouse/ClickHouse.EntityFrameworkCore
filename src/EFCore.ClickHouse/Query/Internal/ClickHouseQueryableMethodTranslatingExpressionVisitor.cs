using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Query.Internal;

public class ClickHouseQueryableMethodTranslatingExpressionVisitor
    : RelationalQueryableMethodTranslatingExpressionVisitor
{
    private readonly RelationalSqlTranslatingExpressionVisitor _sqlTranslator;
    private readonly bool _isSubquery;

    public ClickHouseQueryableMethodTranslatingExpressionVisitor(
        QueryableMethodTranslatingExpressionVisitorDependencies dependencies,
        RelationalQueryableMethodTranslatingExpressionVisitorDependencies relationalDependencies,
        RelationalQueryCompilationContext queryCompilationContext)
        : base(dependencies, relationalDependencies, queryCompilationContext)
    {
        _sqlTranslator = relationalDependencies.RelationalSqlTranslatingExpressionVisitorFactory
            .Create(queryCompilationContext, this);
    }

    protected ClickHouseQueryableMethodTranslatingExpressionVisitor(
        ClickHouseQueryableMethodTranslatingExpressionVisitor parentVisitor)
        : base(parentVisitor)
    {
        _sqlTranslator = parentVisitor._sqlTranslator;
        _isSubquery = true;
    }

    protected override QueryableMethodTranslatingExpressionVisitor CreateSubqueryVisitor()
        => new ClickHouseQueryableMethodTranslatingExpressionVisitor(this);

    protected override Expression VisitExtension(Expression extensionExpression)
        => extensionExpression is ClickHouseFinalQueryRootExpression finalRoot
            ? TranslateFinalQueryRoot(finalRoot)
            : base.VisitExtension(extensionExpression);

    protected override ShapedQueryExpression? TranslateTake(ShapedQueryExpression source, Expression count)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(count);

        // Only the top-level query skips the relational row-limit warning and limit folding.
        if (_isSubquery)
        {
            return base.TranslateTake(source, count);
        }

        var selectExpression = (SelectExpression)source.QueryExpression;
        var translation = _sqlTranslator.Translate(count);

        if (translation != null)
        {
            selectExpression.ApplyLimit(translation);
            return source;
        }

        return null;
    }

    private ShapedQueryExpression TranslateFinalQueryRoot(ClickHouseFinalQueryRootExpression finalRoot)
    {
        var entityType = finalRoot.EntityType;

        // CreateSelect does not handle ToSqlQuery mappings. The base visitor reads them through FromSqlExpression.
        if (entityType.GetSqlQueryMappings().Any(m => m.IsDefaultSqlQueryMapping))
        {
            throw FinalRequiresTable(entityType);
        }

        // Mark every table of the entity type: TPT and entity splitting join tables, TPC unions them.
        var finalApplier = new FinalApplyingExpressionVisitor();
        var selectExpression = (SelectExpression)finalApplier.Visit(CreateSelect(entityType));
        if (!finalApplier.Applied)
        {
            throw FinalRequiresTable(entityType);
        }

        return new ShapedQueryExpression(
            selectExpression,
            new RelationalStructuralTypeShaperExpression(
                entityType,
                new ProjectionBindingExpression(selectExpression, new ProjectionMember(), typeof(ValueBuffer)),
                nullable: false));
    }

    private static InvalidOperationException FinalRequiresTable(IEntityType entityType)
        => new($"Final() requires the entity type '{entityType.DisplayName()}' to be mapped to a table or view.");

    private sealed class FinalApplyingExpressionVisitor : ExpressionVisitor
    {
        public bool Applied { get; private set; }

        [return: NotNullIfNotNull(nameof(expression))]
        public override Expression? Visit(Expression? expression)
        {
            switch (expression)
            {
                // EF copies the owner table annotations to the tables of owned types, so the
                // value names the table that FINAL applies to.
                case TableExpression tableExpression:
                    Applied = true;
                    return tableExpression.AddAnnotation(
                        ClickHouseAnnotationNames.Final, tableExpression.Table.SchemaQualifiedName);

#pragma warning disable EF1001 // TpcTablesExpression is internal EF Core API
                // VisitChildren does not visit the TPC sub-selects, but Clone does.
                case TpcTablesExpression tpcTablesExpression:
                    return tpcTablesExpression.Clone(tpcTablesExpression.Alias, this);
#pragma warning restore EF1001

                default:
                    return base.Visit(expression);
            }
        }
    }
}
