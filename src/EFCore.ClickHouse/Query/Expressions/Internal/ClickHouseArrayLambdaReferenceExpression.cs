using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

#pragma warning disable EF9100 // RelationalExpressionQuotingUtilities is experimental

namespace ClickHouse.EntityFrameworkCore.Query.Expressions.Internal;

/// <summary>
/// Stands in for the lambda parameter inside a ClickHouse array lambda, e.g. the <c>x</c>
/// in <c>arrayMap(x -> lowerUTF8(x), arr)</c>. The lambda parameter is not a column, so
/// scalar translators that expect <see cref="ColumnExpression"/> on the instance side will
/// still work because this is a <see cref="SqlExpression"/> with a real type mapping.
/// </summary>
public class ClickHouseArrayLambdaReferenceExpression : SqlExpression, IEquatable<ClickHouseArrayLambdaReferenceExpression>
{
    private static ConstructorInfo? _quotingConstructor;

    public ClickHouseArrayLambdaReferenceExpression(string name, Type type, RelationalTypeMapping? typeMapping)
        : base(type, typeMapping)
    {
        Name = name;
    }

    public virtual string Name { get; }

    protected override Expression VisitChildren(ExpressionVisitor visitor) => this;

    public override Expression Quote()
        => New(
            _quotingConstructor ??= typeof(ClickHouseArrayLambdaReferenceExpression).GetConstructor(
                [typeof(string), typeof(Type), typeof(RelationalTypeMapping)])!,
            Constant(Name),
            Constant(Type),
            RelationalExpressionQuotingUtilities.QuoteTypeMapping(TypeMapping));

    protected override void Print(ExpressionPrinter expressionPrinter)
        => expressionPrinter.Append(Name);

    public override bool Equals(object? obj)
        => obj is ClickHouseArrayLambdaReferenceExpression other && Equals(other);

    public bool Equals(ClickHouseArrayLambdaReferenceExpression? other)
        => other is not null && base.Equals(other) && Name == other.Name;

    public override int GetHashCode() => HashCode.Combine(base.GetHashCode(), Name);
}
