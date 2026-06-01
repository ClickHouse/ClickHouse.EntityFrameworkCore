using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace ClickHouse.EntityFrameworkCore.Migrations.Operations;

public class ClickHouseCreateDictionaryOperation : MigrationOperation
{
    public required string DictionaryName { get; set; }
    public required IReadOnlyList<ClickHouseDictionaryColumn> Columns { get; set; }
    public required IReadOnlyList<string> KeyColumns { get; set; }
    public required string SourceTable { get; set; }
    public string? SourceDatabase { get; set; }
    public required string Layout { get; set; }
    public string? LayoutParams { get; set; }
    public int? LifetimeMin { get; set; }
    public int? LifetimeMax { get; set; }
    public string? Database { get; set; }
    public string? Cluster { get; set; }
    public bool IfNotExists { get; set; }

    /// <summary>Emit <c>CREATE OR REPLACE DICTIONARY</c> (used when an existing dictionary changed).</summary>
    public bool OrReplace { get; set; }
}
