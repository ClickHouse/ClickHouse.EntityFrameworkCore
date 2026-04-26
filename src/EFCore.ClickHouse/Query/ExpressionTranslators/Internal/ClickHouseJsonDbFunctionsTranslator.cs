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

    private static readonly Dictionary<MethodInfo, string> SupportedMethods;

    static ClickHouseJsonDbFunctionsTranslator()
    {
        var type = typeof(JsonDbFunctions);
        SupportedMethods = new Dictionary<MethodInfo, string>();

        void RegisterSimpleJson(string methodName, string sqlFunction)
        {
            var method = type.GetMethods().FirstOrDefault(m =>
            {
                if (m.Name != methodName || !m.IsGenericMethod) return false;

                var parameters = m.GetParameters();
                return parameters.Length == 3
                       && parameters[0].ParameterType == typeof(DbFunctions)
                       && parameters[1].ParameterType.IsGenericParameter
                       && parameters[2].ParameterType == typeof(string);
            }) ?? throw new InvalidOperationException($"Method {methodName} with strict signature not found.");

            SupportedMethods.Add(method, sqlFunction);
        }

        RegisterSimpleJson(nameof(JsonDbFunctions.SimpleJsonExtractBool), "simpleJSONExtractBool");
        RegisterSimpleJson(nameof(JsonDbFunctions.SimpleJsonExtractFloat), "simpleJSONExtractFloat");
        RegisterSimpleJson(nameof(JsonDbFunctions.SimpleJsonExtractInt), "simpleJSONExtractInt");
        RegisterSimpleJson(nameof(JsonDbFunctions.SimpleJsonExtractRaw), "simpleJSONExtractRaw");
        RegisterSimpleJson(nameof(JsonDbFunctions.SimpleJsonExtractString), "simpleJSONExtractString");
        RegisterSimpleJson(nameof(JsonDbFunctions.SimpleJsonExtractUInt), "simpleJSONExtractUInt");
        RegisterSimpleJson(nameof(JsonDbFunctions.SimpleJsonHas), "simpleJSONHas");
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        var genericMethod = method.IsGenericMethod ? method.GetGenericMethodDefinition() : method;

        if (SupportedMethods.TryGetValue(genericMethod, out var function))
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