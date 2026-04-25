using Microsoft.EntityFrameworkCore.Diagnostics;

namespace Microsoft.EntityFrameworkCore;

public static class ClickHouseJsonDbFunctionsExtensions
{
    /// <summary>
    /// Parses a JSON string and extracts a value as a boolean.
    /// Maps to ClickHouse: simpleJSONExtractBool(json, name)
    /// </summary>
    /// <param name="_">DbFunctions instance</param>
    /// <param name="matchExpression">The property of entity that is to be matched.</param>
    /// <param name="name">The name of the field to extract.</param>
    [DbFunction("simpleJSONExtractBool")]
    public static bool SimpleJsonExtractBool<T>(this DbFunctions _, T matchExpression, string name) =>
        throw new InvalidOperationException(CoreStrings.FunctionOnClient(nameof(SimpleJsonExtractBool)));

    /// <summary>
    /// Parses a JSON string and extracts a value as a double-precision float.
    /// Maps to ClickHouse: simpleJSONExtractFloat(json, name)
    /// </summary>
    /// <param name="_">DbFunctions instance</param>
    /// <param name="matchExpression">The property of entity that is to be matched.</param>
    /// <param name="name">The name of the field to extract.</param>
    [DbFunction("simpleJSONExtractFloat")]
    public static double SimpleJsonExtractFloat<T>(this DbFunctions _, T matchExpression, string name) =>
        throw new InvalidOperationException(CoreStrings.FunctionOnClient(nameof(SimpleJsonExtractFloat)));
    
    /// <summary>
    /// Parses a JSON string and extracts a value as an integer.
    /// Maps to ClickHouse: simpleJSONExtractInt(json, name)
    /// </summary>
    /// <param name="_">DbFunctions instance</param>
    /// <param name="matchExpression">The property of entity that is to be matched.</param>
    /// <param name="name">The name of the field to extract.</param>
    [DbFunction("simpleJSONExtractInt")]
    public static long SimpleJsonExtractInt<T>(this DbFunctions _, T matchExpression, string name) =>
        throw new InvalidOperationException(CoreStrings.FunctionOnClient(nameof(SimpleJsonExtractInt)));
    
    /// <summary>
    /// Extracts a part of a JSON string without unescaping, returning the raw JSON fragment.
    /// Maps to ClickHouse: simpleJSONExtractRaw(json, name)
    /// </summary>
    /// <param name="_">DbFunctions instance</param>
    /// <param name="matchExpression">The property of entity that is to be matched.</param>
    /// <param name="name">The name of the field to extract.</param>
    [DbFunction("simpleJSONExtractRaw")]
    public static string SimpleJsonExtractRaw<T>(this DbFunctions _, T matchExpression, string name) =>
        throw new InvalidOperationException(CoreStrings.FunctionOnClient(nameof(SimpleJsonExtractRaw)));
    
    /// <summary>
    /// Parses a JSON string and extracts a value as a string. 
    /// Fast implementation for simple, non-nested JSON objects.
    /// Maps to ClickHouse: simpleJSONExtractString(json, name)
    /// </summary>
    /// <param name="_">DbFunctions instance</param>
    /// <param name="matchExpression">The property of entity that is to be matched.</param>
    /// <param name="name">The name of the field to extract.</param>
    [DbFunction("simpleJSONExtractString")]
    public static string SimpleJsonExtractString<T>(this DbFunctions _, T matchExpression, string name) =>
        throw new InvalidOperationException(CoreStrings.FunctionOnClient(nameof(SimpleJsonExtractString)));

    /// <summary>
    /// Parses a JSON string and extracts a value as an unsigned integer.
    /// Maps to ClickHouse: simpleJSONExtractUInt(json, name)
    /// </summary>
    /// <param name="_">DbFunctions instance</param>
    /// <param name="matchExpression">The property of entity that is to be matched.</param>
    /// <param name="name">The name of the field to extract.</param>
    [DbFunction("simpleJSONExtractUInt")]
    public static ulong SimpleJsonExtractUInt<T>(this DbFunctions _, T matchExpression, string name) =>
        throw new InvalidOperationException(CoreStrings.FunctionOnClient(nameof(SimpleJsonExtractUInt)));

    /// <summary>
    /// Checks whether a field named <paramref name="name"/> exists in the JSON string.
    /// Maps to ClickHouse: simpleJSONHas(json, name)
    /// </summary>
    /// <param name="_">DbFunctions instance</param>
    /// <param name="matchExpression">The property of entity that is to be matched.</param>
    /// <param name="name">The name of the field to check for existence.</param>
    [DbFunction("simpleJSONHas")]
    public static bool SimpleJsonHas<T>(this DbFunctions _, T matchExpression, string name) =>
        throw new InvalidOperationException(CoreStrings.FunctionOnClient(nameof(SimpleJsonHas)));
}