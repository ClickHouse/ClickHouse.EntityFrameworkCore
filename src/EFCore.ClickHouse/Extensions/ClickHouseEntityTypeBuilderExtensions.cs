using System.Linq.Expressions;
using System.Reflection;
using ClickHouse.EntityFrameworkCore.Metadata.Builders;
using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace ClickHouse.EntityFrameworkCore.Extensions;

public static class ClickHouseEntityTypeBuilderExtensions
{
    public static ClickHouseMergeTreeEngineBuilder HasMergeTreeEngine(this TableBuilder tableBuilder)
    {
        ArgumentNullException.ThrowIfNull(tableBuilder);
        return new(GetEntityType(tableBuilder));
    }

    public static ClickHouseReplacingMergeTreeEngineBuilder HasReplacingMergeTreeEngine(
        this TableBuilder tableBuilder, string? version = null, string? isDeleted = null)
    {
        ArgumentNullException.ThrowIfNull(tableBuilder);
        return new(GetEntityType(tableBuilder), version, isDeleted);
    }

    public static ClickHouseSummingMergeTreeEngineBuilder HasSummingMergeTreeEngine(
        this TableBuilder tableBuilder, params string[] columns)
    {
        ArgumentNullException.ThrowIfNull(tableBuilder);
        return new(GetEntityType(tableBuilder), columns);
    }

    public static ClickHouseAggregatingMergeTreeEngineBuilder HasAggregatingMergeTreeEngine(
        this TableBuilder tableBuilder)
    {
        ArgumentNullException.ThrowIfNull(tableBuilder);
        return new(GetEntityType(tableBuilder));
    }

    public static ClickHouseCollapsingMergeTreeEngineBuilder HasCollapsingMergeTreeEngine(
        this TableBuilder tableBuilder, string sign)
    {
        ArgumentNullException.ThrowIfNull(tableBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(sign);
        return new(GetEntityType(tableBuilder), sign);
    }

    public static ClickHouseVersionedCollapsingMergeTreeEngineBuilder HasVersionedCollapsingMergeTreeEngine(
        this TableBuilder tableBuilder, string sign, string version)
    {
        ArgumentNullException.ThrowIfNull(tableBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(sign);
        ArgumentException.ThrowIfNullOrWhiteSpace(version);
        return new(GetEntityType(tableBuilder), sign, version);
    }

    public static ClickHouseGraphiteMergeTreeEngineBuilder HasGraphiteMergeTreeEngine(
        this TableBuilder tableBuilder, string configSection)
    {
        ArgumentNullException.ThrowIfNull(tableBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(configSection);
        return new(GetEntityType(tableBuilder), configSection);
    }

    public static ClickHouseSimpleEngineBuilder HasTinyLogEngine(this TableBuilder tableBuilder)
    {
        ArgumentNullException.ThrowIfNull(tableBuilder);
        return new(GetEntityType(tableBuilder), ClickHouseAnnotationNames.TinyLog);
    }

    public static ClickHouseSimpleEngineBuilder HasStripeLogEngine(this TableBuilder tableBuilder)
    {
        ArgumentNullException.ThrowIfNull(tableBuilder);
        return new(GetEntityType(tableBuilder), ClickHouseAnnotationNames.StripeLog);
    }

    public static ClickHouseSimpleEngineBuilder HasLogEngine(this TableBuilder tableBuilder)
    {
        ArgumentNullException.ThrowIfNull(tableBuilder);
        return new(GetEntityType(tableBuilder), ClickHouseAnnotationNames.Log);
    }

    public static ClickHouseSimpleEngineBuilder HasMemoryEngine(this TableBuilder tableBuilder)
    {
        ArgumentNullException.ThrowIfNull(tableBuilder);
        return new(GetEntityType(tableBuilder), ClickHouseAnnotationNames.Memory);
    }

    // ── Generic (lambda-based) overloads for TableBuilder<TEntity> ─────────

    public static ClickHouseReplacingMergeTreeEngineBuilder HasReplacingMergeTreeEngine<TEntity>(
        this TableBuilder<TEntity> tableBuilder,
        Expression<Func<TEntity, object?>>? version = null,
        Expression<Func<TEntity, object?>>? isDeleted = null)
        where TEntity : class
        => tableBuilder.HasReplacingMergeTreeEngine(
            GetPropertyName(version), GetPropertyName(isDeleted));

    public static ClickHouseSummingMergeTreeEngineBuilder HasSummingMergeTreeEngine<TEntity>(
        this TableBuilder<TEntity> tableBuilder,
        params Expression<Func<TEntity, object?>>[] columns)
        where TEntity : class
        => tableBuilder.HasSummingMergeTreeEngine(
            columns.Select(GetPropertyName).ToArray()!);

    public static ClickHouseCollapsingMergeTreeEngineBuilder HasCollapsingMergeTreeEngine<TEntity>(
        this TableBuilder<TEntity> tableBuilder,
        Expression<Func<TEntity, object?>> sign)
        where TEntity : class
        => tableBuilder.HasCollapsingMergeTreeEngine(GetPropertyName(sign)!);

    public static ClickHouseVersionedCollapsingMergeTreeEngineBuilder HasVersionedCollapsingMergeTreeEngine<TEntity>(
        this TableBuilder<TEntity> tableBuilder,
        Expression<Func<TEntity, object?>> sign,
        Expression<Func<TEntity, object?>> version)
        where TEntity : class
        => tableBuilder.HasVersionedCollapsingMergeTreeEngine(
            GetPropertyName(sign)!, GetPropertyName(version)!);

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static IMutableEntityType GetEntityType(TableBuilder tableBuilder)
        => (IMutableEntityType)tableBuilder.Metadata;

    private static string? GetPropertyName<TEntity>(Expression<Func<TEntity, object?>>? expression)
    {
        if (expression is null)
            return null;

        var body = expression.Body;

        // Unwrap Convert() that the compiler adds for value types boxed to object
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } unary)
            body = unary.Operand;

        if (body is MemberExpression { Member: PropertyInfo property })
            return property.Name;

        throw new ArgumentException(
            $"Expression '{expression}' does not refer to a property. " +
            "Use a simple property access like 'e => e.MyProperty'.");
    }
}
