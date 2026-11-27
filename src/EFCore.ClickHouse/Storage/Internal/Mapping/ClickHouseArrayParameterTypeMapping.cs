using System.Collections;
using System.Data;
using System.Data.Common;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

/// <summary>
/// The <c>Array(T)</c> mapping of a captured collection that the provider sends as one query parameter,
/// for <c>column IN {p:Array(T)}</c>. <c>ClickHouseSqlNullabilityProcessor</c> applies it.
/// </summary>
/// <remarks>
/// <para>
/// The mapping changes the collection into an array without null elements when it binds the parameter.
/// Binding is the only step that sees the values without stopping EF Core from caching the SQL. A null
/// collection becomes an empty array, as in the EF Core expansion. A lazy sequence is read one time.
/// </para>
/// <para>
/// The driver cannot send a null element in an <c>Array(T)</c> of a type that is not <c>Nullable</c>: it
/// sends <c>0</c> for a number and fails for a string. The provider uses this mapping only when the tested
/// item is never null, so a null element cannot match and removing it does not change the result of
/// <c>Contains</c> or of a negated <c>Contains</c>.
/// </para>
/// </remarks>
public class ClickHouseArrayParameterTypeMapping : ClickHouseArrayTypeMapping
{
    public ClickHouseArrayParameterTypeMapping(RelationalTypeMapping elementMapping)
        : base(elementMapping)
    {
    }

    protected ClickHouseArrayParameterTypeMapping(RelationalTypeMappingParameters parameters, RelationalTypeMapping elementMapping)
        : base(parameters, elementMapping)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseArrayParameterTypeMapping(parameters, ElementMapping);

    public override DbParameter CreateParameter(
        DbCommand command,
        string name,
        object? value,
        bool? nullable = null,
        ParameterDirection direction = ParameterDirection.Input)
        => base.CreateParameter(command, name, ToParameterValue(value), nullable, direction);

    private static object ToParameterValue(object? value)
    {
        switch (value)
        {
            case null or DBNull:
                return Array.Empty<object>();

            // An array of a value type that is not Nullable<T> cannot contain null and is already read.
            case Array array when array.GetType().GetElementType() is { IsValueType: true } elementType
                && Nullable.GetUnderlyingType(elementType) is null:
                return array;

            case IEnumerable elements:
                var result = new List<object>();
                foreach (var element in elements)
                {
                    if (element is not null)
                        result.Add(element);
                }

                return result;

            default:
                return value;
        }
    }
}
