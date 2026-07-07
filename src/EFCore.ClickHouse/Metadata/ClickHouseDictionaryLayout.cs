namespace ClickHouse.EntityFrameworkCore.Metadata;

/// <summary>
/// ClickHouse dictionary in-memory layout. Maps to the <c>LAYOUT(&lt;name&gt;(...))</c> clause of
/// <c>CREATE DICTIONARY</c>. See https://clickhouse.com/docs/en/sql-reference/dictionaries#storing-dictionaries-in-memory.
/// </summary>
public enum ClickHouseDictionaryLayout
{
    Flat,
    Hashed,
    HashedArray,
    ComplexKeyHashed,
    ComplexKeyHashedArray,
    RangeHashed,
    Cache,
    ComplexKeyCache,
    Direct,
    IpTrie,
}

public static class ClickHouseDictionaryLayoutExtensions
{
    /// <summary>The canonical ClickHouse <c>LAYOUT</c> keyword (e.g. <c>COMPLEX_KEY_HASHED</c>).</summary>
    public static string ToClickHouseName(this ClickHouseDictionaryLayout layout) => layout switch
    {
        ClickHouseDictionaryLayout.Flat => "FLAT",
        ClickHouseDictionaryLayout.Hashed => "HASHED",
        ClickHouseDictionaryLayout.HashedArray => "HASHED_ARRAY",
        ClickHouseDictionaryLayout.ComplexKeyHashed => "COMPLEX_KEY_HASHED",
        ClickHouseDictionaryLayout.ComplexKeyHashedArray => "COMPLEX_KEY_HASHED_ARRAY",
        ClickHouseDictionaryLayout.RangeHashed => "RANGE_HASHED",
        ClickHouseDictionaryLayout.Cache => "CACHE",
        ClickHouseDictionaryLayout.ComplexKeyCache => "COMPLEX_KEY_CACHE",
        ClickHouseDictionaryLayout.Direct => "DIRECT",
        ClickHouseDictionaryLayout.IpTrie => "IP_TRIE",
        _ => throw new ArgumentOutOfRangeException(nameof(layout), layout, "Unknown ClickHouse dictionary layout."),
    };
}
