namespace ClickHouse.EntityFrameworkCore.Storage.Internal;

/// <summary>
/// Reads the wrapper structure of a ClickHouse store type name.
/// </summary>
/// <remarks>
/// <para>
/// Several parts of the provider ask the same questions of a store type: does it wrap with
/// <c>Nullable(...)</c>, and what is inside a given wrapper. They used to answer with their own
/// string checks, which disagreed — one accepted <c>Nullable ( Date32 )</c> and another did not,
/// so a column ClickHouse accepts lost its nullable element type. One helper keeps them consistent.
/// </para>
/// <para>
/// ClickHouse tolerates whitespace between a type name and its argument list, and normalizes it
/// away: <c>Array( Nullable ( Date32 ) )</c> and <c>Array(Nullable(Date32))</c> name the same type.
/// So does a difference of case. Both are accepted here.
/// </para>
/// </remarks>
internal static class ClickHouseStoreTypeName
{
    /// <summary>
    /// Removes a <paramref name="wrapper"/> layer, if <paramref name="storeType"/> is exactly that
    /// wrapper applied to one inner type.
    /// </summary>
    public static bool TryUnwrap(string storeType, string wrapper, out string inner)
    {
        inner = storeType;

        var s = storeType.AsSpan().Trim();
        if (!s.StartsWith(wrapper, StringComparison.OrdinalIgnoreCase))
            return false;

        var rest = s[wrapper.Length..].TrimStart();
        if (rest.Length < 2 || rest[0] != '(')
            return false;

        // Walk to the parenthesis that closes the one just found. It must be the last character,
        // otherwise the wrapper is not applied to the whole of the store type.
        var depth = 0;
        for (var i = 0; i < rest.Length; i++)
        {
            if (rest[i] == '(')
            {
                depth++;
            }
            else if (rest[i] == ')')
            {
                depth--;
                if (depth != 0)
                    continue;

                if (i != rest.Length - 1)
                    return false;

                inner = rest[1..i].Trim().ToString();
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Reports whether <paramref name="storeType"/> denotes a value that can be NULL.
    /// </summary>
    /// <remarks>
    /// <c>LowCardinality</c> is a storage-only encoding, and is the one wrapper ClickHouse allows
    /// outside <c>Nullable</c> — it rejects <c>Nullable(LowCardinality(T))</c> and accepts
    /// <c>LowCardinality(Nullable(T))</c>. So the nullable marker is not always the outermost one.
    /// </remarks>
    public static bool IsNullable(string storeType)
    {
        var current = storeType;
        while (true)
        {
            if (TryUnwrap(current, "Nullable", out _))
                return true;
            if (TryUnwrap(current, "LowCardinality", out var inner))
            {
                current = inner;
                continue;
            }

            return false;
        }
    }
}
