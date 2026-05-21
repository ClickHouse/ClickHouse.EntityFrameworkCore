using System.Reflection;
using System.Text.Json.Nodes;
using ClickHouse.EntityFrameworkCore.Query.Expressions.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Query.ExpressionTranslators.Internal;

public class ClickHouseJsonNodeTranslator : IMethodCallTranslator
{
    private readonly ISqlExpressionFactory _sqlExpressionFactory;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public ClickHouseJsonNodeTranslator(
        ISqlExpressionFactory sqlExpressionFactory,
        IRelationalTypeMappingSource typeMappingSource)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
        _typeMappingSource = typeMappingSource;
    }

    private static readonly MethodInfo GetItemMethod = typeof(JsonNode).GetRuntimeMethod("get_Item", [typeof(string)])!;
    private static readonly MethodInfo GetItemIndexMethod = typeof(JsonNode).GetRuntimeMethod("get_Item", [typeof(int)])!;
    
    private static readonly MethodInfo GetValueMethod = typeof(JsonNode).GetMethods()
        .First(m => m is { Name: nameof(JsonNode.GetValue), IsGenericMethod: true });
    
    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        // JsonNode: Data[]
        if (instance != null)
        {
            // Data["tags"] -> Data.tags
            if (method.Equals(GetItemMethod))
            {
                if (arguments[0] is SqlConstantExpression { Value: string propertyName })
                {
                    return new ClickHouseJsonPathExpression(
                        instance,
                        propertyName,
                        method.ReturnType, 
                        instance.TypeMapping);
                }
            }
        
            // Data["tags"][0] -> Data.tags[1]
            if (method.Equals(GetItemIndexMethod))
            {
                if (arguments[0] is SqlConstantExpression { Value: int index })
                {
                    return new ClickHouseJsonArrayIndexExpression(
                        instance, 
                        index + 1,
                        method.ReturnType,
                        instance.TypeMapping);
                }
            }
        }

        // JsonNode: .GetValue<T>()
        var methodToCheck = method.IsGenericMethod ? method.GetGenericMethodDefinition() : method;
        if (instance is ClickHouseJsonPathExpression or ClickHouseJsonArrayIndexExpression &&
            methodToCheck.Equals(GetValueMethod))
        {
            var targetType = method.IsGenericMethod
                ? method.GetGenericArguments()[0]
                : method.ReturnType;

            var mapping = _typeMappingSource.FindMapping(targetType);

            return _sqlExpressionFactory.Convert(instance, targetType, mapping);
        }

        return null;
    }
}