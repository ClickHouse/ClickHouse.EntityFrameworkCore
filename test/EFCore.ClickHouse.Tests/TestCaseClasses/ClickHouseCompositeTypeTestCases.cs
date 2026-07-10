using System.Collections.Generic;

namespace EFCore.ClickHouse.Tests;

public static class ClickHouseCompositeTypeTestCases
{
    private static readonly string[] NullableSubtypes =
    [
        "String",
        "Int32",
        "Int64",
        "Float64",
        "DateTime",
        "UUID",
        "Decimal(18, 4)",
    ];

    public static IEnumerable<object[]> NullableSubtypeCases()
    {
        foreach (var subtype in NullableSubtypes)
        {
            yield return
            [
                $"LowCardinality(Nullable({subtype}))",
                $"LowCardinality({subtype})",
            ];
            yield return
            [
                $"Array(Nullable({subtype}))",
                $"Array(Nullable({subtype}))",
            ];
            yield return
            [
                $"Map(String, Nullable({subtype}))",
                $"Map(String, Nullable({subtype}))",
            ];
            yield return
            [
                $"Tuple(Nullable({subtype}), String)",
                $"Tuple(Nullable({subtype}), String)",
            ];
            yield return
            [
                $"Variant(String, Nullable({subtype}))",
                $"Variant(String, Nullable({subtype}))",
            ];
        }
    }
}
