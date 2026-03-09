using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

#pragma warning disable EF9100 // RelationalExpressionQuotingUtilities is experimental

namespace ClickHouse.EntityFrameworkCore.Query.Expressions.Internal;

public class ClickHouseRowValueExpression : SqlExpression, IEquatable<ClickHouseRowValueExpression>
{
    private static ConstructorInfo? _quotingConstructor;

    public ClickHouseRowValueExpression(
        IReadOnlyList<SqlExpression> values,
        Type type,
        RelationalTypeMapping? typeMapping = null)
        : base(type, typeMapping)
    {
        Values = values;
    }

    public virtual IReadOnlyList<SqlExpression> Values { get; }

    protected override Expression VisitChildren(ExpressionVisitor visitor)
    {
        SqlExpression[]? newValues = null;

        for (var i = 0; i < Values.Count; i++)
        {
            var value = Values[i];
            var visited = (SqlExpression)visitor.Visit(value);
            if (visited != value && newValues is null)
            {
                newValues = new SqlExpression[Values.Count];
                for (var j = 0; j < i; j++)
                    newValues[j] = Values[j];
            }

            if (newValues is not null)
                newValues[i] = visited;
        }

        return newValues is null ? this : new ClickHouseRowValueExpression(newValues, Type);
    }

    public virtual ClickHouseRowValueExpression Update(IReadOnlyList<SqlExpression> values)
        => values.Count == Values.Count && values.Zip(Values, (x, y) => (x, y)).All(tup => tup.x == tup.y)
            ? this
            : new ClickHouseRowValueExpression(values, Type);

    public override Expression Quote()
        => New(
            _quotingConstructor ??= typeof(ClickHouseRowValueExpression).GetConstructor(
                [typeof(IReadOnlyList<SqlExpression>), typeof(Type), typeof(RelationalTypeMapping)])!,
            NewArrayInit(typeof(SqlExpression), initializers: Values.Select(a => a.Quote())),
            Constant(Type),
            RelationalExpressionQuotingUtilities.QuoteTypeMapping(TypeMapping));

    protected override void Print(ExpressionPrinter expressionPrinter)
    {
        expressionPrinter.Append("(");
        for (var i = 0; i < Values.Count; i++)
        {
            expressionPrinter.Visit(Values[i]);
            if (i < Values.Count - 1)
                expressionPrinter.Append(", ");
        }
        expressionPrinter.Append(")");
    }

    public override bool Equals(object? obj)
        => obj is ClickHouseRowValueExpression other && Equals(other);

    public bool Equals(ClickHouseRowValueExpression? other)
    {
        if (other is null || !base.Equals(other) || other.Values.Count != Values.Count)
            return false;
        if (ReferenceEquals(this, other))
            return true;
        for (var i = 0; i < Values.Count; i++)
        {
            if (!other.Values[i].Equals(Values[i]))
                return false;
        }
        return true;
    }

    public override int GetHashCode()
    {
        var hashCode = new HashCode();
        foreach (var value in Values)
            hashCode.Add(value);
        return hashCode.ToHashCode();
    }
}
