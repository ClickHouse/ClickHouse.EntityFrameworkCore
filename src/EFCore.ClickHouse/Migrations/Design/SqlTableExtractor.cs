using System.Text.RegularExpressions;

namespace ClickHouse.EntityFrameworkCore.Migrations.Design;

/// <summary>
/// Pulls the table names referenced by the <c>FROM</c>/<c>JOIN</c> clauses of a materialized
/// view's SELECT query. Used only to feed the dependency graph in
/// <see cref="ClickHouseMigrationsSplitter"/>, so it is intentionally lightweight: names are
/// normalized (database qualifier and backticks stripped, lower-cased) and over-matching is
/// harmless — a spurious edge between two unrelated tables still yields a valid acyclic order.
/// </summary>
internal static partial class SqlTableExtractor
{
    // FROM / JOIN  followed by an optionally back-ticked, optionally db-qualified identifier.
    // A '(' after the keyword (a subquery) is deliberately not matched.
    [GeneratedRegex(
        @"\b(?:FROM|JOIN)\s+(`?[A-Za-z_][\w$]*`?(?:\s*\.\s*`?[A-Za-z_][\w$]*`?)?)",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex TableReferenceRegex();

    public static IReadOnlyCollection<string> ExtractTableReferences(string? sql)
    {
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (string.IsNullOrWhiteSpace(sql))
            return names;

        foreach (Match match in TableReferenceRegex().Matches(sql))
        {
            var name = Normalize(match.Groups[1].Value);
            if (name.Length > 0)
                names.Add(name);
        }

        return names;
    }

    private static string Normalize(string reference)
    {
        // Keep the table portion of an optional "database.table" reference, drop backticks/whitespace.
        var dotIndex = reference.LastIndexOf('.');
        var table = dotIndex >= 0 ? reference[(dotIndex + 1)..] : reference;
        return table.Trim().Trim('`').Trim().ToLowerInvariant();
    }
}
