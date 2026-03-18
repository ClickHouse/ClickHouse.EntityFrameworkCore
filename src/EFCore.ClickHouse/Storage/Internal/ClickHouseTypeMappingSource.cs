using System.Collections.Concurrent;
using System.Data;
using System.Net;
using System.Numerics;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using ClickHouse.Driver.Numerics;
using ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

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
    private static readonly RelationalTypeMapping IPv4Mapping = new ClickHouseIPAddressTypeMapping("IPv4");
    private static readonly RelationalTypeMapping IPv6Mapping = new ClickHouseIPAddressTypeMapping("IPv6");
    private static readonly RelationalTypeMapping Int128Mapping = new ClickHouseBigIntegerTypeMapping("Int128");
    private static readonly RelationalTypeMapping Int256Mapping = new ClickHouseBigIntegerTypeMapping("Int256");
    private static readonly RelationalTypeMapping UInt128Mapping = new ClickHouseBigIntegerTypeMapping("UInt128");
    private static readonly RelationalTypeMapping UInt256Mapping = new ClickHouseBigIntegerTypeMapping("UInt256");
    private static readonly RelationalTypeMapping TimeMapping = new ClickHouseTimeSpanTypeMapping();
    private static readonly RelationalTypeMapping JsonMapping = new ClickHouseJsonTypeMapping();

    // Geo types — structural aliases composed from existing Tuple/Array/Variant mappings.
    // Driver returns Tuple<double, double> (reference tuple) for Point; array-based geo types
    // require reference tuples because Expression.Convert cannot convert Tuple<>[] to ValueTuple<>[].
    private static readonly RelationalTypeMapping GeoPointMapping =
        new ClickHouseTupleTypeMapping([Float64Mapping, Float64Mapping], useValueTuple: false);
    private static readonly RelationalTypeMapping GeoRingMapping =
        new ClickHouseArrayTypeMapping(GeoPointMapping);
    private static readonly RelationalTypeMapping GeoLineStringMapping =
        new ClickHouseArrayTypeMapping(GeoPointMapping);
    private static readonly RelationalTypeMapping GeoPolygonMapping =
        new ClickHouseArrayTypeMapping(GeoRingMapping);
    private static readonly RelationalTypeMapping GeoMultiLineStringMapping =
        new ClickHouseArrayTypeMapping(GeoLineStringMapping);
    private static readonly RelationalTypeMapping GeoMultiPolygonMapping =
        new ClickHouseArrayTypeMapping(GeoPolygonMapping);
    // Alphabetical order matches driver's GeometryType discriminator indices
    private static readonly RelationalTypeMapping GeoGeometryMapping =
        new ClickHouseVariantTypeMapping([
            GeoLineStringMapping,      // 0
            GeoMultiLineStringMapping, // 1
            GeoMultiPolygonMapping,    // 2
            GeoPointMapping,           // 3
            GeoPolygonMapping,         // 4
            GeoRingMapping,            // 5
        ]);

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
        { typeof(IPAddress), IPv4Mapping },
        { typeof(BigInteger), Int128Mapping },
        { typeof(TimeSpan), TimeMapping },
        { typeof(ClickHouseDecimal), new ClickHouseBigDecimalTypeMapping() },
        { typeof(JsonNode), JsonMapping },
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

            ["Int128"] = Int128Mapping,
            ["Int256"] = Int256Mapping,
            ["UInt128"] = UInt128Mapping,
            ["UInt256"] = UInt256Mapping,

            ["Float32"] = Float32Mapping,
            ["Float64"] = Float64Mapping,
            ["BFloat16"] = Float32Mapping,

            ["Bool"] = BoolMapping,
            ["UUID"] = GuidMapping,

            ["Date"] = DateOnlyMapping,
            ["Date32"] = DateOnlyMapping,
            ["DateTime"] = DateTimeMapping,
            ["DateTime64"] = DateTime64Mapping,
            ["Time"] = TimeMapping,

            ["Enum8"] = StringMapping,
            ["Enum16"] = StringMapping,
            ["Enum"] = StringMapping,

            ["IPv4"] = IPv4Mapping,
            ["IPv6"] = IPv6Mapping,

            ["Json"] = JsonMapping,

            // Geo types (structural aliases for Tuple/Array/Variant)
            ["Point"] = GeoPointMapping,
            ["Ring"] = GeoRingMapping,
            ["LineString"] = GeoLineStringMapping,
            ["Polygon"] = GeoPolygonMapping,
            ["MultiLineString"] = GeoMultiLineStringMapping,
            ["MultiPolygon"] = GeoMultiPolygonMapping,
            ["Geometry"] = GeoGeometryMapping,
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

        // Unwrap Nullable(...) and LowCardinality(...) wrappers
        storeTypeName = UnwrapStoreType(storeTypeName);

        var openParen = storeTypeName.IndexOf('(');
        if (openParen < 0)
            return storeTypeName.Trim();

        var baseName = storeTypeName[..openParen].Trim();
        var closeParen = FindMatchingCloseParen(storeTypeName, openParen);
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
            return baseName;
        }

        if (string.Equals(baseName, "DateTime", StringComparison.OrdinalIgnoreCase))
        {
            // Args is just "'UTC'" — no numeric parts, timezone extracted later
            return baseName;
        }

        // Enum8('a'=1,'b'=2) or Enum16(...) — just return the base name
        if (string.Equals(baseName, "Enum8", StringComparison.OrdinalIgnoreCase)
            || string.Equals(baseName, "Enum16", StringComparison.OrdinalIgnoreCase)
            || string.Equals(baseName, "Enum", StringComparison.OrdinalIgnoreCase))
            return baseName;

        // Decimal32(S), Decimal64(S), Decimal128(S), Decimal256(S)
        // These take a single scale argument; precision is fixed per type.
        if (string.Equals(baseName, "Decimal32", StringComparison.OrdinalIgnoreCase))
        {
            precision = 9;
            if (int.TryParse(args, out var s)) scale = s;
            return baseName;
        }

        if (string.Equals(baseName, "Decimal64", StringComparison.OrdinalIgnoreCase))
        {
            precision = 18;
            if (int.TryParse(args, out var s)) scale = s;
            return baseName;
        }

        // Note: Decimal128 (38 digits) and Decimal256 (76 digits) exceed .NET decimal's
        // 28-29 digit precision. Values exceeding .NET's range will throw OverflowException
        // at materialization time. This is a known limitation documented in AGENTS.md.
        if (string.Equals(baseName, "Decimal128", StringComparison.OrdinalIgnoreCase))
        {
            precision = 38;
            if (int.TryParse(args, out var s)) scale = s;
            return baseName;
        }

        if (string.Equals(baseName, "Decimal256", StringComparison.OrdinalIgnoreCase))
        {
            precision = 76;
            if (int.TryParse(args, out var s)) scale = s;
            return baseName;
        }

        // Time64(N)
        if (string.Equals(baseName, "Time64", StringComparison.OrdinalIgnoreCase))
        {
            if (int.TryParse(args, out var p))
                precision = p;
            return baseName;
        }

        // Array(...), Map(...), Tuple(...), Variant(...), Json(...) — return base name, inner parsing in FindMapping
        if (string.Equals(baseName, "Array", StringComparison.OrdinalIgnoreCase)
            || string.Equals(baseName, "Map", StringComparison.OrdinalIgnoreCase)
            || string.Equals(baseName, "Tuple", StringComparison.OrdinalIgnoreCase)
            || string.Equals(baseName, "Variant", StringComparison.OrdinalIgnoreCase)
            || string.Equals(baseName, "Json", StringComparison.OrdinalIgnoreCase))
        {
            return baseName;
        }

        // For everything else (Decimal, etc.), let the base handle it
        return base.ParseStoreTypeName(storeTypeName, ref unicode, ref size, ref precision, ref scale);
    }

    protected override RelationalTypeMapping? FindMapping(in RelationalTypeMappingInfo mappingInfo)
        // Call base first so plugin/extension type mappings can intercept before our defaults.
        => base.FindMapping(in mappingInfo)
           ?? FindDateTime64Mapping(mappingInfo)
           ?? FindDateTimeMapping(mappingInfo)
           ?? FindFixedStringMapping(mappingInfo)
           ?? FindTimeMappings(mappingInfo)
           ?? FindArrayMapping(mappingInfo)
           ?? FindMapMapping(mappingInfo)
           ?? FindTupleMapping(mappingInfo)
           ?? FindVariantMapping(mappingInfo)
           ?? FindDynamicMapping(mappingInfo)
           ?? FindJsonMapping(mappingInfo)
           ?? FindEnumMapping(mappingInfo)
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

    private static RelationalTypeMapping? FindTimeMappings(in RelationalTypeMappingInfo mappingInfo)
    {
        if (string.Equals(mappingInfo.StoreTypeNameBase, "Time64", StringComparison.OrdinalIgnoreCase))
            return new ClickHouseTimeSpanTypeMapping(mappingInfo.Precision);

        return null;
    }

    private RelationalTypeMapping? FindArrayMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        RelationalTypeMapping? elementMapping = null;

        // Resolve element mapping from store type: Array(X)
        if (string.Equals(mappingInfo.StoreTypeNameBase, "Array", StringComparison.OrdinalIgnoreCase))
        {
            var storeTypeName = mappingInfo.StoreTypeName;
            if (storeTypeName is null)
                return null;

            var innerType = ExtractInnerType(storeTypeName, "Array");
            if (innerType is null)
                return null;

            elementMapping = FindMapping(innerType);
        }

        var clrType = mappingInfo.ClrType;

        // Resolve element mapping from CLR type if not already resolved from store type
        if (elementMapping is null)
        {
            if (clrType is not null && clrType.IsArray && clrType.GetArrayRank() == 1)
                elementMapping = FindMapping(clrType.GetElementType()!);
            else if (clrType is not null && clrType.IsGenericType && clrType.GetGenericTypeDefinition() == typeof(List<>))
                elementMapping = FindMapping(clrType.GetGenericArguments()[0]);
        }

        if (elementMapping is null)
            return null;

        // If CLR type is List<T>, use a ValueConverter to bridge List<T> ↔ T[]
        if (clrType is not null && clrType.IsGenericType && clrType.GetGenericTypeDefinition() == typeof(List<>))
        {
            var listElementType = clrType.GetGenericArguments()[0];
            var converterType = typeof(ListToArrayConverter<>).MakeGenericType(listElementType);
            var converter = (ValueConverter)Activator.CreateInstance(converterType)!;
            var comparer = ClickHouseArrayTypeMapping.CreateListComparer(listElementType);
            return new ClickHouseArrayTypeMapping(elementMapping, converter, comparer);
        }

        return new ClickHouseArrayTypeMapping(elementMapping);
    }

    private RelationalTypeMapping? FindMapMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        // Resolve from store type: Map(K, V)
        if (string.Equals(mappingInfo.StoreTypeNameBase, "Map", StringComparison.OrdinalIgnoreCase))
        {
            var storeTypeName = mappingInfo.StoreTypeName;
            if (storeTypeName is null)
                return null;

            var innerTypes = ExtractInnerTypes(storeTypeName, "Map", 2);
            if (innerTypes is null)
                return null;

            var keyMapping = FindMapping(innerTypes[0]);
            var valueMapping = FindMapping(innerTypes[1]);
            if (keyMapping is null || valueMapping is null)
                return null;

            return new ClickHouseMapTypeMapping(keyMapping, valueMapping);
        }

        // Resolve from CLR type: Dictionary<K,V>
        var clrType = mappingInfo.ClrType;
        if (clrType is not null && clrType.IsGenericType && clrType.GetGenericTypeDefinition() == typeof(Dictionary<,>))
        {
            var typeArgs = clrType.GetGenericArguments();
            var keyMapping = FindMapping(typeArgs[0]);
            var valueMapping = FindMapping(typeArgs[1]);
            if (keyMapping is not null && valueMapping is not null)
                return new ClickHouseMapTypeMapping(keyMapping, valueMapping);
        }

        return null;
    }

    private RelationalTypeMapping? FindTupleMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        // Resolve from store type: Tuple(T1, T2, ...)
        if (string.Equals(mappingInfo.StoreTypeNameBase, "Tuple", StringComparison.OrdinalIgnoreCase))
        {
            var storeTypeName = mappingInfo.StoreTypeName;
            if (storeTypeName is null)
                return null;

            var innerTypes = ExtractInnerTypes(storeTypeName, "Tuple");
            if (innerTypes is null || innerTypes.Count == 0)
                return null;

            var elementMappings = new List<RelationalTypeMapping>();
            foreach (var innerType in innerTypes)
            {
                var mapping = FindMapping(innerType);
                if (mapping is null)
                    return null;
                elementMappings.Add(mapping);
            }

            // If the CLR type is System.Tuple<>, use reference tuples (no conversion needed).
            // Otherwise default to ValueTuple<> (requires conversion from driver's Tuple<>).
            var useValueTuple = !IsReferenceTuple(mappingInfo.ClrType);
            return new ClickHouseTupleTypeMapping(elementMappings, useValueTuple);
        }

        // Resolve from CLR type: ValueTuple<...> or Tuple<...>
        var clrType = mappingInfo.ClrType;
        if (clrType is not null && clrType.IsGenericType)
        {
            var (isTuple, useVt) = ClassifyTupleType(clrType);
            if (isTuple)
            {
                var typeArgs = clrType.GetGenericArguments();
                var elementMappings = new List<RelationalTypeMapping>();
                foreach (var arg in typeArgs)
                {
                    var mapping = FindMapping(arg);
                    if (mapping is null)
                        return null;
                    elementMappings.Add(mapping);
                }

                return new ClickHouseTupleTypeMapping(elementMappings, useVt);
            }
        }

        return null;
    }

    private RelationalTypeMapping? FindVariantMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        if (!string.Equals(mappingInfo.StoreTypeNameBase, "Variant", StringComparison.OrdinalIgnoreCase))
            return null;

        var storeTypeName = mappingInfo.StoreTypeName;
        if (storeTypeName is null)
            return null;

        var innerTypes = ExtractInnerTypes(storeTypeName, "Variant");
        if (innerTypes is null || innerTypes.Count == 0)
            return null;

        var elementMappings = new List<RelationalTypeMapping>();
        foreach (var innerType in innerTypes)
        {
            var mapping = FindMapping(innerType);
            if (mapping is null)
                return null;
            elementMappings.Add(mapping);
        }

        return new ClickHouseVariantTypeMapping(elementMappings);
    }

    private RelationalTypeMapping? FindDynamicMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        if (!string.Equals(mappingInfo.StoreTypeNameBase, "Dynamic", StringComparison.OrdinalIgnoreCase))
            return null;

        return new ClickHouseDynamicTypeMapping(this);
    }

    private static RelationalTypeMapping? FindJsonMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        if (!string.Equals(mappingInfo.StoreTypeNameBase, "Json", StringComparison.OrdinalIgnoreCase))
            return null;

        // string CLR type + Json store type → specific string Json mapping
        if (mappingInfo.ClrType == typeof(string))
            return StringJsonMapping;

        return JsonMapping;
    }

    // Cached string-specific Json mapping to avoid repeated allocations
     private static readonly RelationalTypeMapping StringJsonMapping = new ClickHouseJsonTypeMapping(typeof(string));

    private static bool IsReferenceTuple(Type? type)
        => type is not null && type.IsGenericType && type.FullName?.StartsWith("System.Tuple`") == true;

    private static (bool IsTuple, bool UseValueTuple) ClassifyTupleType(Type type)
    {
        var fullName = type.GetGenericTypeDefinition().FullName;
        if (fullName?.StartsWith("System.ValueTuple`") == true)
            return (true, true);
        if (fullName?.StartsWith("System.Tuple`") == true)
            return (true, false);
        return (false, false);
    }

    // Cache enum mappings to avoid repeated reflection + converter creation
    private static readonly ConcurrentDictionary<Type, RelationalTypeMapping> EnumMappingCache = new();

    private static RelationalTypeMapping? FindEnumMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        var clrType = mappingInfo.ClrType;
        if (clrType is null || !clrType.IsEnum)
            return null;

        // ClickHouse Enum8/Enum16 values are read/written as strings by the driver.
        // Use EnumToStringConverter to convert between C# enums and strings.
        return EnumMappingCache.GetOrAdd(clrType, enumType => new ClickHouseEnumTypeMapping(enumType));
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
        var baseName = mappingInfo.StoreTypeNameBase;
        int? precision = mappingInfo.Precision;
        int? scale = mappingInfo.Scale;
        var useBigDecimal = mappingInfo.ClrType == typeof(ClickHouseDecimal);

        // Tier 1d: Decimal32/64/128/256 with fixed max precision
        if (string.Equals(baseName, "Decimal32", StringComparison.OrdinalIgnoreCase))
            precision ??= 9;
        else if (string.Equals(baseName, "Decimal64", StringComparison.OrdinalIgnoreCase))
            precision ??= 18;
        else if (string.Equals(baseName, "Decimal128", StringComparison.OrdinalIgnoreCase))
            precision ??= 38;
        else if (string.Equals(baseName, "Decimal256", StringComparison.OrdinalIgnoreCase))
            precision ??= 76;
        else if (!useBigDecimal
                 && mappingInfo.ClrType != typeof(decimal)
                 && !string.Equals(baseName, "Decimal", StringComparison.OrdinalIgnoreCase))
            return null;

        // ClickHouseDecimal supports the full range; .NET decimal is limited to 28-29 digits.
        if (useBigDecimal)
            return new ClickHouseBigDecimalTypeMapping(precision, scale);

        return new ClickHouseDecimalTypeMapping(precision, scale);
    }

    private static string? ExtractTimezone(string storeTypeName)
    {
        var match = TimezoneRegex.Match(storeTypeName);
        return match.Success ? match.Groups[1].Value : null;
    }

    /// <summary>
    /// Strips Nullable(...) and LowCardinality(...) wrappers from a store type name.
    /// Handles nesting: LowCardinality(Nullable(String)) → String
    /// </summary>
    private static string UnwrapStoreType(string storeTypeName)
    {
        var s = storeTypeName.Trim();
        while (true)
        {
            if (TryUnwrapPrefix(s, "Nullable", out var inner)
                || TryUnwrapPrefix(s, "LowCardinality", out inner))
            {
                s = inner;
                continue;
            }

            break;
        }

        return s;
    }

    private static bool TryUnwrapPrefix(string s, string prefix, out string inner)
    {
        inner = s;
        if (s.Length <= prefix.Length + 2 // need at least prefix + "(X)"
            || !s.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)
            || s[prefix.Length] != '(')
            return false;

        // Find matching close paren for the one at prefix.Length
        var depth = 0;
        for (var i = prefix.Length; i < s.Length; i++)
        {
            if (s[i] == '(')
                depth++;
            else if (s[i] == ')')
            {
                depth--;
                if (depth == 0)
                {
                    // Only unwrap if this closing paren is the last character
                    if (i == s.Length - 1)
                    {
                        inner = s[(prefix.Length + 1)..i].Trim();
                        return true;
                    }

                    return false;
                }
            }
        }

        return false;
    }

    /// <summary>
    /// Extracts the single inner type from a parameterized store type like Array(Int32).
    /// </summary>
    private static string? ExtractInnerType(string storeTypeName, string prefix)
    {
        var types = ExtractInnerTypes(storeTypeName, prefix, 1);
        return types?.Count == 1 ? types[0] : null;
    }

    /// <summary>
    /// Splits the inner types of a parameterized store type, respecting nested parens.
    /// Example: Map(String, Array(Int32)) → ["String", "Array(Int32)"]
    /// </summary>
    private static List<string>? ExtractInnerTypes(string storeTypeName, string prefix, int? expectedCount = null)
    {
        var openParen = prefix.Length;
        if (storeTypeName.Length <= openParen + 2
            || storeTypeName[openParen] != '(')
            return null;

        var closeParen = FindMatchingCloseParen(storeTypeName, openParen);
        if (closeParen < 0)
            return null;

        var inner = storeTypeName[(openParen + 1)..closeParen];
        var results = new List<string>();
        var depth = 0;
        var start = 0;

        for (var i = 0; i < inner.Length; i++)
        {
            if (inner[i] == '(') depth++;
            else if (inner[i] == ')') depth--;
            else if (inner[i] == ',' && depth == 0)
            {
                results.Add(inner[start..i].Trim());
                start = i + 1;
            }
        }

        results.Add(inner[start..].Trim());

        if (expectedCount.HasValue && results.Count != expectedCount.Value)
            return null;

        return results;
    }

    /// <summary>
    /// Finds the matching closing paren for the opening paren at the given index.
    /// </summary>
    private static int FindMatchingCloseParen(string s, int openParenIndex)
    {
        var depth = 0;
        for (var i = openParenIndex; i < s.Length; i++)
        {
            if (s[i] == '(') depth++;
            else if (s[i] == ')')
            {
                depth--;
                if (depth == 0)
                    return i;
            }
        }

        return -1;
    }
}
