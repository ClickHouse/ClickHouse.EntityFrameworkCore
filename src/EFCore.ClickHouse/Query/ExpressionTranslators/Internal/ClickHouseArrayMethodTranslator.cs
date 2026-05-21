using System.Reflection;
using ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Query.ExpressionTranslators.Internal;

/// <summary>
/// Translates LINQ helpers on mapped ClickHouse <c>Array(T)</c> columns to their native
/// ClickHouse equivalents — <c>has()</c>, <c>length()</c>, <c>notEmpty()</c>. Only fires when
/// the operand's type mapping is <see cref="ClickHouseArrayTypeMapping"/>, so local
/// in-memory collections continue to flow through EF Core's standard inline-collection path.
/// </summary>
public class ClickHouseArrayMethodTranslator : IMethodCallTranslator, IMemberTranslator
{
    private static readonly MethodInfo EnumerableAny = GetEnumerableMethod(nameof(Enumerable.Any));
    private static readonly MethodInfo EnumerableCount = GetEnumerableMethod(nameof(Enumerable.Count));
    private static readonly MethodInfo EnumerableLongCount = GetEnumerableMethod(nameof(Enumerable.LongCount));
    private static readonly MethodInfo EnumerableContains = GetEnumerableMethod(nameof(Enumerable.Contains), parameterCount: 2);

    private readonly ISqlExpressionFactory _sqlExpressionFactory;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public ClickHouseArrayMethodTranslator(
        ISqlExpressionFactory sqlExpressionFactory,
        IRelationalTypeMappingSource typeMappingSource)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
        _typeMappingSource = typeMappingSource;
    }

    /// <summary>
    /// Backup translator for the <c>IMethodCallTranslator</c> chain. In current EF Core
    /// versions, <see cref="ClickHouseArrayLinqTranslator"/> intercepts every shape covered
    /// here before the translator chain runs — these branches exist defensively for shapes
    /// EF might stop normalizing in the future, and so that an <c>IMethodCallTranslator</c>
    /// implementation registered alongside (third-party providers, plugins) sees the
    /// expected behavior.
    /// </summary>
    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (method.IsGenericMethod)
        {
            var genericMethodDefinition = method.GetGenericMethodDefinition();

            if (genericMethodDefinition == EnumerableContains
                && arguments is [var source, var item]
                && IsClickHouseArray(source))
            {
                return TranslateContains(source, item);
            }

            if (genericMethodDefinition == EnumerableAny
                && arguments is [var anySource]
                && IsClickHouseArray(anySource))
            {
                return TranslateNotEmpty(anySource);
            }

            if (genericMethodDefinition == EnumerableCount
                && arguments is [var countSource]
                && IsClickHouseArray(countSource))
            {
                return TranslateLength(countSource, typeof(int));
            }

            if (genericMethodDefinition == EnumerableLongCount
                && arguments is [var longCountSource]
                && IsClickHouseArray(longCountSource))
            {
                return TranslateLength(longCountSource, typeof(long));
            }
        }

        if (instance != null
            && IsClickHouseArray(instance)
            && method.Name == nameof(List<int>.Contains)
            && arguments is [var instanceContainsItem])
        {
            return TranslateContains(instance, instanceContainsItem);
        }

        return null;
    }

    /// <summary>
    /// IMemberTranslator path for <c>Array.Length</c> / <c>List&lt;T&gt;.Count</c> /
    /// <c>IList&lt;T&gt;.Count</c> / <c>IReadOnlyList&lt;T&gt;.Count</c> / <c>ICollection&lt;T&gt;.Count</c>.
    /// Unlike the method-call paths above, this is the live translator for member-access
    /// expressions — EF Core does dispatch them through the IMemberTranslator chain.
    /// </summary>

    public SqlExpression? Translate(
        SqlExpression? instance,
        MemberInfo member,
        Type returnType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (instance != null
            && IsClickHouseArray(instance)
            && member.Name is nameof(Array.Length) or nameof(List<int>.Count))
        {
            return TranslateLength(instance, returnType);
        }

        return null;
    }

    public SqlExpression TranslateContains(SqlExpression source, SqlExpression item)
    {
        // Align the search item with the array element's type mapping so the parameter
        // serializes with the column's element store type (Int64 vs Int32, FixedString(N)
        // vs String, Enum8 vs String, …) — otherwise LINQ-provided literals carry .NET-
        // default mappings and ClickHouse does an implicit conversion at best.
        var arrayTypeMapping = (ClickHouseArrayTypeMapping)source.TypeMapping!;
        var alignedItem = _sqlExpressionFactory.ApplyTypeMapping(item, arrayTypeMapping.ElementMapping);

        // has() returns UInt8 (0/1) and never NULL — it does not propagate null per element,
        // and ClickHouse Array columns themselves can't be NULL. Marking nullable: false lets
        // the nullability processor skip redundant null-compensation around the predicate.
        return _sqlExpressionFactory.Function(
            "has",
            [source, alignedItem],
            nullable: false,
            argumentsPropagateNullability: [false, false],
            typeof(bool),
            _typeMappingSource.FindMapping(typeof(bool)));
    }

    public SqlExpression TranslateNotEmpty(SqlExpression source)
        => _sqlExpressionFactory.Function(
            "notEmpty",
            [source],
            nullable: false,
            argumentsPropagateNullability: [false],
            typeof(bool),
            _typeMappingSource.FindMapping(typeof(bool)));

    public SqlExpression TranslateLength(SqlExpression source, Type returnType)
        => _sqlExpressionFactory.Function(
            "length",
            [source],
            nullable: false,
            argumentsPropagateNullability: [false],
            returnType,
            _typeMappingSource.FindMapping(returnType));

    public static bool IsClickHouseArray(SqlExpression expression)
        => expression.TypeMapping is ClickHouseArrayTypeMapping;

    private static MethodInfo GetEnumerableMethod(string name, int parameterCount = 1)
        => typeof(Enumerable).GetRuntimeMethods()
            .Single(m => m.Name == name && m.IsGenericMethod && m.GetParameters().Length == parameterCount);
}
