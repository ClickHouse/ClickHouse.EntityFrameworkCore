using System.Reflection;
using ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace ClickHouse.EntityFrameworkCore.Query.ExpressionTranslators.Internal;

public class ClickHouseArrayTranslator : IMethodCallTranslator, IMemberTranslator
{
    private static readonly MethodInfo EnumerableContainsMethod = typeof(Enumerable).GetRuntimeMethods()
        .Single(m => m.Name == nameof(Enumerable.Contains) && m.GetParameters().Length == 2);

    private static readonly MethodInfo QueryableContainsMethod = typeof(Queryable).GetRuntimeMethods()
        .Single(m => m.Name == nameof(Queryable.Contains) && m.GetParameters().Length == 2);

    private static readonly MemberInfo ArrayLengthMember = typeof(Array).GetProperty(nameof(Array.Length))!;

    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public ClickHouseArrayTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (!TryGetContainsArguments(instance, method, arguments, out var source, out var item)
            || !IsMappedArraySource(source))
        {
            return null;
        }

        return _sqlExpressionFactory.Function(
            "has",
            [source, item],
            nullable: true,
            argumentsPropagateNullability: [true, true],
            typeof(bool));
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MemberInfo member,
        Type returnType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (instance is null || !IsMappedArraySource(instance))
            return null;

        if (member == ArrayLengthMember || IsCountMember(member))
        {
            return _sqlExpressionFactory.Function(
                "length",
                [instance],
                nullable: true,
                argumentsPropagateNullability: [true],
                returnType);
        }

        return null;
    }

    private static bool TryGetContainsArguments(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        out SqlExpression source,
        out SqlExpression item)
    {
        if (method.IsGenericMethod)
        {
            var genericMethodDefinition = method.GetGenericMethodDefinition();
            if (genericMethodDefinition == EnumerableContainsMethod
                || genericMethodDefinition == QueryableContainsMethod)
            {
                source = arguments[0];
                item = arguments[1];
                return true;
            }
        }
        else if (instance != null
                 && method.Name == nameof(List<int>.Contains)
                 && arguments.Count == 1
                 && IsCollectionContainsMethod(method))
        {
            source = instance;
            item = arguments[0];
            return true;
        }

        source = null!;
        item = null!;
        return false;
    }

    internal static bool IsMappedArraySource(SqlExpression source)
        => source.TypeMapping is ClickHouseArrayTypeMapping
            && source switch
            {
                ColumnExpression => true,
                SqlUnaryExpression { Operand: var operand } => IsMappedArraySource(operand),
                _ => false
            };

    private static bool IsCollectionContainsMethod(MethodInfo method)
    {
        var declaringType = method.DeclaringType;
        if (declaringType?.IsGenericType != true)
            return false;

        var genericTypeDefinition = declaringType.GetGenericTypeDefinition();
        return genericTypeDefinition == typeof(List<>)
            || genericTypeDefinition == typeof(ICollection<>)
            || genericTypeDefinition == typeof(IList<>)
            || genericTypeDefinition == typeof(IReadOnlyCollection<>)
            || genericTypeDefinition == typeof(IReadOnlyList<>);
    }

    private static bool IsCountMember(MemberInfo member)
    {
        if (member.Name != nameof(ICollection<int>.Count))
            return false;

        var declaringType = member.DeclaringType;
        if (declaringType?.IsGenericType != true)
            return false;

        var genericTypeDefinition = declaringType.GetGenericTypeDefinition();
        return genericTypeDefinition == typeof(ICollection<>)
            || genericTypeDefinition == typeof(IList<>)
            || genericTypeDefinition == typeof(IReadOnlyCollection<>)
            || genericTypeDefinition == typeof(IReadOnlyList<>);
    }
}
