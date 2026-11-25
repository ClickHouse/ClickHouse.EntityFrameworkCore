using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;

namespace ClickHouse.EntityFrameworkCore.Query.Internal;

/// <summary>
/// Query root for an entity type that is read with the ClickHouse <c>FINAL</c> modifier.
/// </summary>
public class ClickHouseFinalQueryRootExpression : EntityQueryRootExpression
{
    public ClickHouseFinalQueryRootExpression(IAsyncQueryProvider queryProvider, IEntityType entityType)
        : base(queryProvider, entityType)
    {
    }

    public ClickHouseFinalQueryRootExpression(IEntityType entityType)
        : base(entityType)
    {
    }

    public override Expression DetachQueryProvider()
        => new ClickHouseFinalQueryRootExpression(EntityType);

    public override EntityQueryRootExpression UpdateEntityType(IEntityType entityType)
        => entityType.ClrType != EntityType.ClrType || entityType.Name != EntityType.Name
            ? throw new InvalidOperationException(CoreStrings.QueryRootDifferentEntityType(entityType.DisplayName()))
            : new ClickHouseFinalQueryRootExpression(entityType);

    protected override void Print(ExpressionPrinter expressionPrinter)
    {
        base.Print(expressionPrinter);
        expressionPrinter.Append(".Final()");
    }

    public override bool Equals(object? obj)
        => obj is ClickHouseFinalQueryRootExpression && base.Equals(obj);

    // EntityQueryRootExpression.Equals also accepts derived roots. A different hash code keeps
    // the queries with and without FINAL apart in the compiled query cache.
    public override int GetHashCode()
        => HashCode.Combine(base.GetHashCode(), typeof(ClickHouseFinalQueryRootExpression));
}
