using System.Reflection;
using ClickHouse.EntityFrameworkCore.Query.Expressions.Internal;
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

    /// <summary>
    /// <c>arrayElement(arr, i)</c> — semantically identical to ClickHouse's <c>arr[i]</c>
    /// indexer syntax. Used for <c>First</c>/<c>Last</c>/<c>ElementAt</c> translations.
    /// </summary>
    /// <param name="source">The array column SqlExpression. Must carry a
    /// <see cref="ClickHouseArrayTypeMapping"/>.</param>
    /// <param name="oneBasedIndex">The 1-based ClickHouse index. Callers handle the
    /// 0→1 offset for <c>ElementAt(i)</c>; <c>First</c>/<c>Last</c> pass literal 1/-1.</param>
    /// <remarks>
    /// ClickHouse returns the element type's default (0, "", etc.) for out-of-bounds
    /// indices rather than raising. This is a documented divergence from .NET LINQ's
    /// <c>InvalidOperationException</c>/<c>ArgumentOutOfRangeException</c> on empty/OOB.
    /// </remarks>
    public SqlExpression TranslateElementAt(SqlExpression source, SqlExpression oneBasedIndex)
    {
        var arrayTypeMapping = (ClickHouseArrayTypeMapping)source.TypeMapping!;
        return _sqlExpressionFactory.Function(
            "arrayElement",
            [source, oneBasedIndex],
            nullable: false,
            argumentsPropagateNullability: [false, false],
            arrayTypeMapping.ElementMapping.ClrType,
            arrayTypeMapping.ElementMapping);
    }

    /// <summary>
    /// <c>arraySlice(arr, offset[, length])</c>. Both offset and length follow ClickHouse's
    /// 1-based / positive-direction conventions — callers add 1 to LINQ-side 0-based offsets
    /// (<c>Skip(n)</c> → offset n+1; <c>Take(m)</c> → offset 1, length m).
    /// The result is itself <c>Array(T)</c> with the same element mapping as the source, so
    /// chained helpers (<c>arr.Skip(1).Contains(x)</c>) compose naturally.
    /// </summary>
    public SqlExpression TranslateSlice(SqlExpression source, SqlExpression offset, SqlExpression? length)
    {
        var arrayTypeMapping = (ClickHouseArrayTypeMapping)source.TypeMapping!;
        SqlExpression[] args = length is null ? [source, offset] : [source, offset, length];
        bool[] propagate = length is null ? [false, false] : [false, false, false];

        return _sqlExpressionFactory.Function(
            "arraySlice",
            args,
            nullable: false,
            argumentsPropagateNullability: propagate,
            source.Type,
            arrayTypeMapping);
    }

    public SqlExpression TranslateReverse(SqlExpression source)
        => UnaryArrayProducing("arrayReverse", source);

    public SqlExpression TranslateDistinct(SqlExpression source)
        => UnaryArrayProducing("arrayDistinct", source);

    public SqlExpression TranslateSort(SqlExpression source, ClickHouseArrayLambdaExpression? keyLambda)
        => SortFunction("arraySort", source, keyLambda);

    public SqlExpression TranslateReverseSort(SqlExpression source, ClickHouseArrayLambdaExpression? keyLambda)
        => SortFunction("arrayReverseSort", source, keyLambda);

    private SqlExpression UnaryArrayProducing(string functionName, SqlExpression source)
    {
        var arrayTypeMapping = (ClickHouseArrayTypeMapping)source.TypeMapping!;
        return _sqlExpressionFactory.Function(
            functionName,
            [source],
            nullable: false,
            argumentsPropagateNullability: [false],
            source.Type,
            arrayTypeMapping);
    }

    private SqlExpression SortFunction(string functionName, SqlExpression source, ClickHouseArrayLambdaExpression? keyLambda)
    {
        var arrayTypeMapping = (ClickHouseArrayTypeMapping)source.TypeMapping!;
        SqlExpression[] args = keyLambda is null ? [source] : [keyLambda, source];
        bool[] propagate = keyLambda is null ? [false] : [false, false];

        return _sqlExpressionFactory.Function(
            functionName,
            args,
            nullable: false,
            argumentsPropagateNullability: propagate,
            source.Type,
            arrayTypeMapping);
    }

    public static bool IsClickHouseArray(SqlExpression expression)
        => expression.TypeMapping is ClickHouseArrayTypeMapping;

    private static MethodInfo GetEnumerableMethod(string name, int parameterCount = 1)
        => typeof(Enumerable).GetRuntimeMethods()
            .Single(m => m.Name == name && m.IsGenericMethod && m.GetParameters().Length == parameterCount);
}
