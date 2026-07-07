using System.Text.RegularExpressions;

namespace ClickHouse.EntityFrameworkCore.Migrations.Design;

/// <summary>
/// Rewrites EF Core's translated SELECT — which always carries an explicit
/// <c>FROM &lt;table&gt; [AS &lt;alias&gt;]</c> — into the FROM-less form a ClickHouse projection
/// requires: the parent table is implicit, so the FROM clause is removed and the table-alias
/// qualifiers are stripped from the remaining columns. Only the <c>SELECT [GROUP BY] [ORDER BY]</c>
/// shape is supported; a WHERE clause, JOIN, or HAVING cannot live inside a projection and is
/// rejected with a message pointing to <c>FromRaw</c>.
/// </summary>
internal static partial class ProjectionSqlRewriter
{
    // " FROM `tbl` [AS `a`]" — optionally db-qualified table, optional alias captured as "alias".
    // A '(' after FROM (a subquery) is deliberately not matched: projections read the parent table only.
    [GeneratedRegex(
        @"\s+FROM\s+`?[A-Za-z_][\w$]*`?(?:\s*\.\s*`?[A-Za-z_][\w$]*`?)?(?:\s+AS\s+(?<alias>`?[A-Za-z_][\w$]*`?))?",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex FromClauseRegex();

    public static string Rewrite(string translatedSql)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(translatedSql);

        var match = FromClauseRegex().Match(translatedSql);
        if (!match.Success)
            throw new InvalidOperationException(
                $"Could not locate a FROM clause in the translated projection query '{translatedSql}'. "
                + "Define the projection with .FromRaw(...) instead.");

        // Keep everything before and after the FROM segment (the trailing GROUP BY / ORDER BY survive).
        var withoutFrom = translatedSql[..match.Index] + translatedSql[(match.Index + match.Length)..];

        if (ContainsKeyword(withoutFrom, "WHERE")
            || ContainsKeyword(withoutFrom, "JOIN")
            || ContainsKeyword(withoutFrom, "HAVING"))
        {
            throw new InvalidOperationException(
                "A ClickHouse projection's SELECT cannot contain WHERE, JOIN, or HAVING. "
                + "Use only Select / GroupBy / OrderBy in the LINQ body, or define the projection with .FromRaw(...).");
        }

        var alias = match.Groups["alias"].Value.Trim('`');
        var result = withoutFrom;
        if (!string.IsNullOrEmpty(alias))
        {
            var escaped = Regex.Escape(alias);
            // Backtick-qualified ("`a`.") then bare ("a.") references. Strip only outside string
            // literals so a literal like 'e.g. high' (with the table alias `e`) is not corrupted.
            result = ReplaceOutsideStringLiterals(result, $"`{escaped}`\\.", "");
            result = ReplaceOutsideStringLiterals(result, $"\\b{escaped}\\.", "");
        }

        // ClickHouse projection ORDER BY does not accept the SQL NULLS FIRST / NULLS LAST modifiers
        // that EF Core appends to each translated ORDER BY term (`ORDER BY x NULLS FIRST` → syntax
        // error in projection DDL). Strip them — null ordering is not meaningful for a projection's
        // physical sort order. Again only outside literals.
        result = ReplaceOutsideStringLiterals(result, @"\s+NULLS\s+(?:FIRST|LAST)", "", RegexOptions.IgnoreCase);

        // Collapse the whitespace left where the FROM clause was removed.
        return CollapseWhitespace(result).Trim();
    }

    // A deliberately rough word-boundary scan over the remaining SELECT body. It can over-reject a
    // body that merely contains the word (e.g. a column literally named "Where"), but the failure is
    // safe — a clear "use FromRaw(...)" error, never silent corruption — and the LINQ path only feeds
    // simple SELECT/GroupBy/OrderBy shapes here.
    private static bool ContainsKeyword(string sql, string keyword)
        => Regex.IsMatch(sql, $"\\b{keyword}\\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static string CollapseWhitespace(string sql)
        => Regex.Replace(sql, @"\s+", " ", RegexOptions.CultureInvariant);

    // Applies a regex replacement to the parts of the SQL that are NOT inside a single-quoted string
    // literal, leaving literal text untouched. ClickHouse escapes an embedded quote by doubling it
    // ('' — the SQL standard) or with a backslash (\'); both are handled so the split tracks the true
    // literal boundaries and never treats an escaped quote as a terminator.
    private static string ReplaceOutsideStringLiterals(
        string sql, string pattern, string replacement, RegexOptions options = RegexOptions.None)
    {
        var regex = new Regex(pattern, options | RegexOptions.CultureInvariant);
        var builder = new System.Text.StringBuilder(sql.Length);
        var i = 0;
        while (i < sql.Length)
        {
            var quote = sql.IndexOf('\'', i);
            if (quote < 0)
            {
                builder.Append(regex.Replace(sql[i..], replacement));
                break;
            }

            // Rewrite the code segment before the literal, then copy the literal verbatim.
            builder.Append(regex.Replace(sql[i..quote], replacement));

            var j = quote + 1;
            while (j < sql.Length)
            {
                if (sql[j] == '\\' && j + 1 < sql.Length)
                {
                    j += 2; // backslash-escaped character (e.g. \')
                    continue;
                }
                if (sql[j] == '\'')
                {
                    if (j + 1 < sql.Length && sql[j + 1] == '\'')
                    {
                        j += 2; // doubled '' escape stays inside the literal
                        continue;
                    }
                    j++; // closing quote
                    break;
                }
                j++;
            }

            builder.Append(sql, quote, j - quote);
            i = j;
        }

        return builder.ToString();
    }
}
