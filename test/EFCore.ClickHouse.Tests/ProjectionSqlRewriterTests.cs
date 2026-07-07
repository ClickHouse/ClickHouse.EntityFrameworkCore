using ClickHouse.EntityFrameworkCore.Migrations.Design;
using Xunit;

namespace EFCore.ClickHouse.Tests;

/// <summary>
/// Direct tests of <see cref="ProjectionSqlRewriter"/> over representative EF Core output — the
/// translated SELECT always carries a FROM &lt;table&gt; AS &lt;alias&gt; that a projection cannot have.
/// </summary>
public class ProjectionSqlRewriterTests
{
    [Fact]
    public void Strips_FROM_clause_and_backtick_alias_qualifiers()
    {
        const string sql =
            "SELECT `e`.`Category` AS `Category`, count(*) AS `Cnt` FROM `events` AS `e` GROUP BY `e`.`Category`";

        var result = ProjectionSqlRewriter.Rewrite(sql);

        Assert.DoesNotContain("FROM", result, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("`e`.", result);
        Assert.Contains("SELECT `Category` AS `Category`", result);
        Assert.Contains("GROUP BY `Category`", result);
    }

    [Fact]
    public void Strips_FROM_when_table_is_database_qualified()
    {
        const string sql = "SELECT `e`.`Id` AS `Id` FROM `analytics`.`events` AS `e`";

        var result = ProjectionSqlRewriter.Rewrite(sql);

        Assert.Equal("SELECT `Id` AS `Id`", result);
    }

    [Fact]
    public void Strips_FROM_without_an_alias()
    {
        var result = ProjectionSqlRewriter.Rewrite("SELECT Category FROM events GROUP BY Category");

        Assert.Equal("SELECT Category GROUP BY Category", result);
    }

    [Fact]
    public void Keeps_ORDER_BY_after_stripping()
    {
        const string sql = "SELECT `e`.`Id` AS `Id` FROM `events` AS `e` ORDER BY `e`.`Id`";

        var result = ProjectionSqlRewriter.Rewrite(sql);

        Assert.DoesNotContain("FROM", result, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ORDER BY `Id`", result);
    }

    [Theory]
    [InlineData("SELECT `e`.`Url` AS `Url` FROM `events` AS `e` ORDER BY `e`.`Url` NULLS FIRST", "ORDER BY `Url`")]
    [InlineData("SELECT `e`.`Url` AS `Url` FROM `events` AS `e` ORDER BY `e`.`Url` NULLS LAST", "ORDER BY `Url`")]
    public void Strips_NULLS_FIRST_or_LAST_which_ClickHouse_projections_reject(string sql, string expected)
    {
        // EF Core appends NULLS FIRST/LAST to translated ORDER BY terms, but a ClickHouse projection's
        // ORDER BY rejects those modifiers (syntax error in projection DDL), so the rewriter drops them.
        var result = ProjectionSqlRewriter.Rewrite(sql);

        Assert.DoesNotContain("NULLS", result, StringComparison.OrdinalIgnoreCase);
        Assert.EndsWith(expected, result);
    }

    [Theory]
    [InlineData("SELECT `e`.`Id` AS `Id` FROM `events` AS `e` WHERE `e`.`Id` > 0", "WHERE")]
    [InlineData("SELECT `e`.`Id` FROM `events` AS `e` INNER JOIN `other` AS `o` ON `e`.`Id` = `o`.`Id`", "JOIN")]
    public void Rejects_constructs_a_projection_cannot_express(string sql, string expectedInMessage)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => ProjectionSqlRewriter.Rewrite(sql));
        Assert.Contains(expectedInMessage, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Rejects_query_with_no_FROM_clause()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => ProjectionSqlRewriter.Rewrite("SELECT 1 AS x"));
        Assert.Contains("FROM", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
