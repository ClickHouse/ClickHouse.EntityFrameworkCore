namespace ClickHouse.EntityFrameworkCore.Migrations;

/// <summary>
/// One column of a ClickHouse dictionary as it appears in the <c>CREATE DICTIONARY ( … )</c> column
/// list — a name, a ClickHouse store type, and an optional <c>DEFAULT</c> expression. Key columns are
/// listed here as well as in the dictionary's <c>PRIMARY KEY</c>.
/// </summary>
public sealed record ClickHouseDictionaryColumn(string Name, string Type, string? Default = null);
