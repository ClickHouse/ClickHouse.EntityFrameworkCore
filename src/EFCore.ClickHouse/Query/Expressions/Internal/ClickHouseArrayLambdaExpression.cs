using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

#pragma warning disable EF9100 // RelationalExpressionQuotingUtilities is experimental

namespace ClickHouse.EntityFrameworkCore.Query.Expressions.Internal;

/// <summary>
/// Represents a single-argument ClickHouse array lambda such as <c>x -> lowerUTF8(x)</c>.
/// Used as the first argument to higher-order array functions (<c>arrayMap</c>,
/// <c>arrayFilter</c>, <c>arrayExists</c>, …). The body is a translated
/// <see cref="SqlExpression"/> built by routing the LINQ lambda body through the existing
/// scalar translator chain, with the parameter substituted by a
/// <see cref="ClickHouseArrayLambdaReferenceExpression"/>.
/// </summary>
public class ClickHouseArrayLambdaExpression : SqlExpression, IEquatable<ClickHouseArrayLambdaExpression>
{
    private static ConstructorInfo? _quotingConstructor;

    public ClickHouseArrayLambdaExpression(
        ClickHouseArrayLambdaReferenceExpression parameter,
        SqlExpression body)
        // The lambda is a higher-order argument, not a value — but EF Core's
        // RelationalTypeMappingPostprocessor still requires a non-null TypeMapping on every
        // SqlExpression it walks. Surface the body's type mapping so the postprocessor is
        // satisfied; consumers like arrayMap discard it anyway.
        : base(body.Type, body.TypeMapping)
    {
        Parameter = parameter;
        Body = body;
    }

    public new virtual ClickHouseArrayLambdaReferenceExpression Parameter { get; }

    public virtual SqlExpression Body { get; }

    /// <summary>
    /// Visiting the parameter independently from the body would leave the body holding the
    /// original <see cref="ClickHouseArrayLambdaReferenceExpression"/> instance, which is
    /// undetectable for reference-equality consumers (the SQL generator) but a real hazard
    /// if a future visitor decides to rewrite the parameter (e.g., to rename it). Substitute
    /// the visited parameter into the body before visiting the body so the two stay in sync.
    /// </summary>
    protected override Expression VisitChildren(ExpressionVisitor visitor)
    {
        var newParameter = (ClickHouseArrayLambdaReferenceExpression)visitor.Visit(Parameter);
        var bodyToVisit = ReferenceEquals(newParameter, Parameter)
            ? Body
            : (SqlExpression)new ReplacingExpressionVisitor(
                originals: [Parameter],
                replacements: [newParameter]).Visit(Body);
        var newBody = (SqlExpression)visitor.Visit(bodyToVisit);
        return newParameter == Parameter && newBody == Body
            ? this
            : new ClickHouseArrayLambdaExpression(newParameter, newBody);
    }

    public virtual ClickHouseArrayLambdaExpression Update(
        ClickHouseArrayLambdaReferenceExpression parameter,
        SqlExpression body)
        => parameter == Parameter && body == Body
            ? this
            : new ClickHouseArrayLambdaExpression(parameter, body);

    public override Expression Quote()
        => New(
            _quotingConstructor ??= typeof(ClickHouseArrayLambdaExpression).GetConstructor(
                [typeof(ClickHouseArrayLambdaReferenceExpression), typeof(SqlExpression)])!,
            Parameter.Quote(),
            Body.Quote());

    protected override void Print(ExpressionPrinter expressionPrinter)
    {
        expressionPrinter.Visit(Parameter);
        expressionPrinter.Append(" -> ");
        expressionPrinter.Visit(Body);
    }

    public override bool Equals(object? obj)
        => obj is ClickHouseArrayLambdaExpression other && Equals(other);

    public bool Equals(ClickHouseArrayLambdaExpression? other)
        => other is not null
            && base.Equals(other)
            && Parameter.Equals(other.Parameter)
            && Body.Equals(other.Body);

    public override int GetHashCode() => HashCode.Combine(base.GetHashCode(), Parameter, Body);
}
