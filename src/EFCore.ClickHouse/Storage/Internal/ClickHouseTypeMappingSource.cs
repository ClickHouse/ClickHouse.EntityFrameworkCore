using System.Data;
using System.Text.RegularExpressions;
using ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal;

public class ClickHouseTypeMappingSource : RelationalTypeMappingSource
{
    private static readonly RelationalTypeMapping StringMapping = new ClickHouseStringTypeMapping();
    private static readonly RelationalTypeMapping BoolMapping = new ClickHouseBoolTypeMapping();
    private static readonly RelationalTypeMapping ByteMapping = new ClickHouseIntegerTypeMapping("UInt8", typeof(byte), DbType.Byte);
    private static readonly RelationalTypeMapping SByteMapping = new ClickHouseIntegerTypeMapping("Int8", typeof(sbyte), DbType.SByte);
    private static readonly RelationalTypeMapping Int16Mapping = new ClickHouseIntegerTypeMapping("Int16", typeof(short), DbType.Int16);
    private static readonly RelationalTypeMapping UInt16Mapping = new ClickHouseIntegerTypeMapping("UInt16", typeof(ushort), DbType.UInt16);
    private static readonly RelationalTypeMapping Int32Mapping = new ClickHouseIntegerTypeMapping("Int32", typeof(int), DbType.Int32);
    private static readonly RelationalTypeMapping UInt32Mapping = new ClickHouseIntegerTypeMapping("UInt32", typeof(uint), DbType.UInt32);
    private static readonly RelationalTypeMapping Int64Mapping = new ClickHouseIntegerTypeMapping("Int64", typeof(long), DbType.Int64);
    private static readonly RelationalTypeMapping UInt64Mapping = new ClickHouseIntegerTypeMapping("UInt64", typeof(ulong), DbType.UInt64);
    private static readonly RelationalTypeMapping Float32Mapping = new ClickHouseFloatTypeMapping();
    private static readonly RelationalTypeMapping Float64Mapping = new ClickHouseDoubleTypeMapping();
    private static readonly RelationalTypeMapping DateTimeMapping = new ClickHouseDateTimeTypeMapping();
    private static readonly RelationalTypeMapping DateTime64Mapping = new ClickHouseDateTime64TypeMapping();
    private static readonly RelationalTypeMapping DateOnlyMapping = new ClickHouseDateOnlyTypeMapping();
    private static readonly RelationalTypeMapping GuidMapping = new ClickHouseGuidTypeMapping();

    private static readonly Dictionary<Type, RelationalTypeMapping> ClrTypeMappings = new()
    {
        { typeof(string), StringMapping },
        { typeof(bool), BoolMapping },
        { typeof(byte), ByteMapping },
        { typeof(sbyte), SByteMapping },
        { typeof(short), Int16Mapping },
        { typeof(ushort), UInt16Mapping },
        { typeof(int), Int32Mapping },
        { typeof(uint), UInt32Mapping },
        { typeof(long), Int64Mapping },
        { typeof(ulong), UInt64Mapping },
        { typeof(float), Float32Mapping },
        { typeof(double), Float64Mapping },
        { typeof(DateTime), DateTimeMapping },
        { typeof(DateOnly), DateOnlyMapping },
        { typeof(Guid), GuidMapping },
        { typeof(char), StringMapping },
    };

    private static readonly Dictionary<string, RelationalTypeMapping> StoreTypeMappings =
        new(StringComparer.OrdinalIgnoreCase)
        {
            ["String"] = StringMapping,

            ["Int8"] = SByteMapping,
            ["Int16"] = Int16Mapping,
            ["Int32"] = Int32Mapping,
            ["Int64"] = Int64Mapping,
            ["UInt8"] = ByteMapping,
            ["UInt16"] = UInt16Mapping,
            ["UInt32"] = UInt32Mapping,
            ["UInt64"] = UInt64Mapping,

            ["Float32"] = Float32Mapping,
            ["Float64"] = Float64Mapping,

            ["Bool"] = BoolMapping,
            ["UUID"] = GuidMapping,

            ["Date"] = DateOnlyMapping,
            ["Date32"] = DateOnlyMapping,
            ["DateTime"] = DateTimeMapping,
            ["DateTime64"] = DateTime64Mapping,
        };

    // Matches a single-quoted string like 'UTC' or 'Asia/Tokyo'
    private static readonly Regex TimezoneRegex = new(@"'([^']+)'", RegexOptions.Compiled);

    public ClickHouseTypeMappingSource(
        TypeMappingSourceDependencies dependencies,
        RelationalTypeMappingSourceDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
    }

    protected override string? ParseStoreTypeName(
        string? storeTypeName,
        ref bool? unicode,
        ref int? size,
        ref int? precision,
        ref int? scale)
    {
        if (string.IsNullOrWhiteSpace(storeTypeName))
            return null;

        var openParen = storeTypeName.IndexOf('(');
        if (openParen < 0)
            return storeTypeName.Trim();

        var baseName = storeTypeName[..openParen].Trim();
        var closeParen = storeTypeName.LastIndexOf(')');
        if (closeParen <= openParen)
            return baseName;

        var args = storeTypeName[(openParen + 1)..closeParen].Trim();

        if (string.Equals(baseName, "FixedString", StringComparison.OrdinalIgnoreCase))
        {
            if (int.TryParse(args, out var fixedSize))
                size = fixedSize;
            return baseName;
        }

        if (string.Equals(baseName, "DateTime64", StringComparison.OrdinalIgnoreCase))
        {
            // Args can be "6" or "6, 'UTC'"
            var parts = args.Split(',', 2);
            if (int.TryParse(parts[0].Trim(), out var p))
                precision = p;
            // Timezone is extracted later from the full StoreTypeName
            return baseName;
        }

        if (string.Equals(baseName, "DateTime", StringComparison.OrdinalIgnoreCase))
        {
            // Args is just "'UTC'" — no numeric parts, timezone extracted later
            return baseName;
        }

        // For everything else (Decimal, etc.), let the base handle it
        return base.ParseStoreTypeName(storeTypeName, ref unicode, ref size, ref precision, ref scale);
    }

    protected override RelationalTypeMapping? FindMapping(in RelationalTypeMappingInfo mappingInfo)
        // Call base first so plugin/extension type mappings can intercept before our defaults.
        // This follows the Npgsql pattern and ensures custom ITypeMappingSourcePlugin
        // implementations registered in DI are respected.
        => base.FindMapping(in mappingInfo)
           ?? FindDateTime64Mapping(mappingInfo)
           ?? FindDateTimeMapping(mappingInfo)
           ?? FindFixedStringMapping(mappingInfo)
           ?? FindExistingMapping(mappingInfo)
           ?? FindDecimalMapping(mappingInfo);

    private RelationalTypeMapping? FindDateTime64Mapping(in RelationalTypeMappingInfo mappingInfo)
    {
        if (!string.Equals(mappingInfo.StoreTypeNameBase, "DateTime64", StringComparison.OrdinalIgnoreCase))
            return null;

        // Only activate when there are actual parameters to handle
        var storeTypeName = mappingInfo.StoreTypeName;
        if (storeTypeName is null || !storeTypeName.Contains('('))
            return null;

        var precision = mappingInfo.Precision ?? 3;
        var timezone = ExtractTimezone(storeTypeName);
        return new ClickHouseDateTime64TypeMapping(precision, timezone);
    }

    private RelationalTypeMapping? FindDateTimeMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        if (!string.Equals(mappingInfo.StoreTypeNameBase, "DateTime", StringComparison.OrdinalIgnoreCase))
            return null;

        var storeTypeName = mappingInfo.StoreTypeName;
        if (storeTypeName is null || !storeTypeName.Contains('('))
            return null;

        var timezone = ExtractTimezone(storeTypeName);
        return new ClickHouseDateTimeTypeMapping(timezone);
    }

    private static RelationalTypeMapping? FindFixedStringMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        if (!string.Equals(mappingInfo.StoreTypeNameBase, "FixedString", StringComparison.OrdinalIgnoreCase))
            return null;

        var size = mappingInfo.Size;
        if (size is null)
            return null;

        return new ClickHouseFixedStringTypeMapping(size.Value);
    }

    private static RelationalTypeMapping? FindExistingMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        if (!string.IsNullOrWhiteSpace(mappingInfo.StoreTypeNameBase) &&
            StoreTypeMappings.TryGetValue(mappingInfo.StoreTypeNameBase, out var aliasMapping1))
        {
            return aliasMapping1;
        }

        if (!string.IsNullOrWhiteSpace(mappingInfo.StoreTypeName) &&
            StoreTypeMappings.TryGetValue(mappingInfo.StoreTypeName, out var aliasMapping2))
        {
            return aliasMapping2;
        }

        if (mappingInfo.ClrType != null &&
            ClrTypeMappings.TryGetValue(mappingInfo.ClrType, out var clrMapping))
        {
            return clrMapping;
        }

        return null;
    }

    private static RelationalTypeMapping? FindDecimalMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        if (mappingInfo.ClrType == typeof(decimal) ||
            string.Equals(mappingInfo.StoreTypeNameBase, "Decimal", StringComparison.OrdinalIgnoreCase))
        {
            return new ClickHouseDecimalTypeMapping(mappingInfo.Precision, mappingInfo.Scale);
        }

        return null;
    }

    private static string? ExtractTimezone(string storeTypeName)
    {
        var match = TimezoneRegex.Match(storeTypeName);
        return match.Success ? match.Groups[1].Value : null;
    }
}
