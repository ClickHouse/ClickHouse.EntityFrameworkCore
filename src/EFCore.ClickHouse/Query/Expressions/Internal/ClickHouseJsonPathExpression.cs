using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Query.Expressions.Internal;

public class ClickHouseJsonPathExpression : SqlExpression
{
    public ClickHouseJsonPathExpression(
        SqlExpression instance,
        string propertyName,
        Type type,
        RelationalTypeMapping? typeMapping)
        : base(type, typeMapping)
    {
        Instance = instance;
        PropertyName = propertyName;
    }

    public SqlExpression Instance { get; }
    public string PropertyName { get; }

    public override Expression Quote()
    {
        return new ClickHouseJsonPathExpression(Instance, PropertyName, Type, TypeMapping);
    }

    protected override void Print(ExpressionPrinter expressionPrinter)
    {
        expressionPrinter.Visit(Instance);
        expressionPrinter.Append(".");
        expressionPrinter.Append(PropertyName);
    }

    public virtual ClickHouseJsonPathExpression Update(SqlExpression instance)
    {
        return !ReferenceEquals(instance, Instance)
            ? new ClickHouseJsonPathExpression(instance, PropertyName, Type, TypeMapping)
            : this;
    }

    protected override Expression VisitChildren(ExpressionVisitor visitor)
    {
        var instance = (SqlExpression)visitor.Visit(Instance);
        return Update(instance);
    }

    public override bool Equals(object? obj)
    {
        return obj is ClickHouseJsonPathExpression other && Equals(other);
    }

    private bool Equals(ClickHouseJsonPathExpression other)
    {
        return base.Equals(other) && Instance.Equals(other.Instance) && PropertyName == other.PropertyName;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(base.GetHashCode(), Instance, PropertyName);
    }
}