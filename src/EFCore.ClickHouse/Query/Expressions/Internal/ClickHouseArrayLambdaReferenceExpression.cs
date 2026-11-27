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

    /// <param name="name">The name of the lambda parameter.</param>
    /// <param name="type">The CLR type of an array element.</param>
    /// <param name="typeMapping">The type mapping of an array element.</param>
    /// <param name="isNullable">
    /// <see langword="true"/> when an array element can be NULL, for example an element of
    /// <c>Array(Nullable(String))</c>.
    /// </param>
    public ClickHouseArrayLambdaReferenceExpression(
        string name,
        Type type,
        RelationalTypeMapping? typeMapping,
        bool isNullable = false)
        : base(type, typeMapping)
    {
        Name = name;
        IsNullable = isNullable;
    }

    public virtual string Name { get; }

    /// <summary>
    /// <see langword="true"/> when an array element can be NULL. The nullability processor uses this
    /// value to add the null checks that give .NET semantics in the lambda body.
    /// </summary>
    public virtual bool IsNullable { get; }

    protected override Expression VisitChildren(ExpressionVisitor visitor) => this;

    public override Expression Quote()
        => New(
            _quotingConstructor ??= typeof(ClickHouseArrayLambdaReferenceExpression).GetConstructor(
                [typeof(string), typeof(Type), typeof(RelationalTypeMapping), typeof(bool)])!,
            Constant(Name),
            Constant(Type),
            RelationalExpressionQuotingUtilities.QuoteTypeMapping(TypeMapping),
            Constant(IsNullable));

    protected override void Print(ExpressionPrinter expressionPrinter)
        => expressionPrinter.Append(Name);

    public override bool Equals(object? obj)
        => obj is ClickHouseArrayLambdaReferenceExpression other && Equals(other);

    public bool Equals(ClickHouseArrayLambdaReferenceExpression? other)
        => other is not null && base.Equals(other) && Name == other.Name && IsNullable == other.IsNullable;

    public override int GetHashCode() => HashCode.Combine(base.GetHashCode(), Name, IsNullable);
}
