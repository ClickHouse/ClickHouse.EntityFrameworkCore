using System.Data.Common;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

public class ClickHouseJsonTypeMapping : RelationalTypeMapping
{
    private static readonly MethodInfo GetValueMethod =
        typeof(DbDataReader).GetRuntimeMethod(nameof(DbDataReader.GetValue), [typeof(int)])!;

    private static readonly MethodInfo ToJsonStringMethod =
        typeof(JsonNode).GetMethod(nameof(JsonNode.ToJsonString), [typeof(JsonSerializerOptions)])!;

    public ClickHouseJsonTypeMapping()
        : this(typeof(JsonNode))
    {
    }

    public ClickHouseJsonTypeMapping(Type clrType)
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    clrType,
                    comparer: clrType == typeof(string)
                        ? new ValueComparer<string?>(
                            (a, b) => a == b,
                            o => o == null ? 0 : o.GetHashCode(),
                            source => source)
                        : CreateJsonNodeComparer()),
                "Json",
                dbType: System.Data.DbType.Object))
    {
    }

    protected ClickHouseJsonTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseJsonTypeMapping(parameters);

    public override MethodInfo GetDataReaderMethod()
        => GetValueMethod;

    public override Expression CustomizeDataReaderExpression(Expression expression)
    {
        // Driver returns JsonObject (subclass of JsonNode) from GetValue().
        var asJsonNode = Expression.Convert(expression, typeof(JsonNode));

        if (ClrType == typeof(string))
        {
            // Call JsonNode.ToJsonString(null) to produce a raw JSON string.
            // No ValueConverter needed — the driver accepts string directly for writes.
            return Expression.Call(
                asJsonNode,
                ToJsonStringMethod,
                Expression.Constant(null, typeof(JsonSerializerOptions)));
        }

        return asJsonNode;
    }

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var json = value switch
        {
            JsonNode node => node.ToJsonString(),
            string s => s,
            _ => throw new InvalidOperationException(
                $"Cannot generate SQL literal for JSON column from CLR type '{value.GetType().Name}'.")
        };

        return $"'{EscapeSqlLiteral(json)}'";
    }

    private static string EscapeSqlLiteral(string literal)
        => literal.Replace("\\", "\\\\").Replace("'", "\\'");

    private static ValueComparer<JsonNode?> CreateJsonNodeComparer()
        => new(
            (a, b) => (a == null && b == null) || (a != null && b != null && JsonNode.DeepEquals(a, b)),
            o => o == null ? 0 : o.ToJsonString().GetHashCode(),
            source => source == null ? null : JsonNode.Parse(source.ToJsonString()));
}
