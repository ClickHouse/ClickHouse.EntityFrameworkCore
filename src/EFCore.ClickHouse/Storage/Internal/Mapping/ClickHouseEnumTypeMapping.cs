using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

/// <summary>
/// Maps C# enum types to ClickHouse Enum8/Enum16/String columns via EnumToStringConverter.
/// The ClickHouse driver reads/writes enum values as strings, so the conversion is string-based.
/// </summary>
public class ClickHouseEnumTypeMapping : ClickHouseStringTypeMapping
{
    public ClickHouseEnumTypeMapping(Type enumType)
        : base(CreateParameters(enumType))
    {
    }

    protected ClickHouseEnumTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseEnumTypeMapping(parameters);

    private static RelationalTypeMappingParameters CreateParameters(Type enumType)
    {
        var converterType = typeof(EnumToStringConverter<>).MakeGenericType(enumType);
        var converter = (ValueConverter)Activator.CreateInstance(converterType)!;

        return new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(enumType, converter: converter),
            "String",
            dbType: System.Data.DbType.String);
    }
}
