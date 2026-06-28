using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace ClickHouse.EntityFrameworkCore.Migrations.Operations;

public class ClickHouseCreateDictionaryOperation : MigrationOperation
{
    public required string DictionaryName { get; set; }
    public required IReadOnlyList<ClickHouseDictionaryColumn> Columns { get; set; }
    public required IReadOnlyList<string> KeyColumns { get; set; }
    public required string SourceTable { get; set; }
    public string? SourceDatabase { get; set; }

    /// <summary>Optional <c>SOURCE(CLICKHOUSE(...))</c> connection settings. When all are omitted, the
    /// dictionary loads as the <c>default</c> user with an empty password, which fails on servers
    /// where <c>default</c> is password-protected. Prefer <see cref="SourceNamedCollection"/> to keep
    /// credentials out of the migration; the inline settings below act as overrides.</summary>
    public string? SourceNamedCollection { get; set; }
    public string? SourceHost { get; set; }
    public int? SourcePort { get; set; }
    public string? SourceUser { get; set; }
    public string? SourcePassword { get; set; }
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
