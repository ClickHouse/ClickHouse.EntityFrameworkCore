using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using JsonDbFunctions = Microsoft.EntityFrameworkCore.ClickHouseJsonDbFunctionsExtensions;

namespace ClickHouse.EntityFrameworkCore.Query.ExpressionTranslators.Internal;

public class ClickHouseJsonDbFunctionsTranslator : IMethodCallTranslator
{
    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public ClickHouseJsonDbFunctionsTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    private static readonly Dictionary<string, string> SupportedMethods = new()
    {
        { nameof(JsonDbFunctions.SimpleJsonExtractBool), "simpleJSONExtractBool" },
        { nameof(JsonDbFunctions.SimpleJsonExtractFloat), "simpleJSONExtractFloat" },
        { nameof(JsonDbFunctions.SimpleJsonExtractInt), "simpleJSONExtractInt" },
        { nameof(JsonDbFunctions.SimpleJsonExtractRaw), "simpleJSONExtractRaw" },
        { nameof(JsonDbFunctions.SimpleJsonExtractString), "simpleJSONExtractString" },
        { nameof(JsonDbFunctions.SimpleJsonExtractUInt), "simpleJSONExtractUInt" },
        { nameof(JsonDbFunctions.SimpleJsonHas), "simpleJSONHas" },
    };

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (method.DeclaringType != typeof(JsonDbFunctions))
        {
            return null;
        }
        
        if (SupportedMethods.TryGetValue(method.Name, out var function))
        {
            var sqlArguments = arguments.Skip(1).ToList();
            
            return _sqlExpressionFactory.Function(
                name: function,
                arguments: sqlArguments,
                nullable: true,
                argumentsPropagateNullability: sqlArguments.Select(_ => false),
                returnType: method.ReturnType);
        }

        return null;
    }
}