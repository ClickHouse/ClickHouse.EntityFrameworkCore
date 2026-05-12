using System.Globalization;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Query.Expressions.Internal;

public class ClickHouseJsonArrayIndexExpression : SqlExpression
{
    public ClickHouseJsonArrayIndexExpression(
        SqlExpression instance,
        int index, 
        Type type,
        RelationalTypeMapping? typeMapping)
        : base(type, typeMapping)
    {
        Instance = instance;
        Index = index;
    }

    public SqlExpression Instance { get; } 
    public int Index { get; }

    public override Expression Quote()
    {
        return new ClickHouseJsonArrayIndexExpression(Instance, Index, Type, TypeMapping);
    }

    protected override void Print(ExpressionPrinter expressionPrinter)
    {
        expressionPrinter.Visit(Instance);
        expressionPrinter.Append("[");
        expressionPrinter.Append(Index.ToString(CultureInfo.InvariantCulture));
        expressionPrinter.Append("]");
    }
    
    public virtual ClickHouseJsonArrayIndexExpression Update(SqlExpression instance)
    {
        return !ReferenceEquals(instance, Instance)
            ? new ClickHouseJsonArrayIndexExpression(instance, Index, Type, TypeMapping)
            : this;
    }

    protected override Expression VisitChildren(ExpressionVisitor visitor)
    {
        var instance = (SqlExpression)visitor.Visit(Instance);
        return Update(instance);
    }

    public override bool Equals(object? obj)
    {
        return obj is ClickHouseJsonArrayIndexExpression other && Equals(other);
    }

    private bool Equals(ClickHouseJsonArrayIndexExpression other)
    {
        return base.Equals(other) && Instance.Equals(other.Instance) && Index == other.Index;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(base.GetHashCode(), Instance, Index);
    }
}