namespace ClickHouse.EntityFrameworkCore.Migrations.Design;

/// <summary>
/// Ordered phases used by <see cref="ClickHouseMigrationsSplitter"/> to sequence migration
/// operations so that every object exists before it is referenced. Operations are emitted
/// phase-by-phase in ascending value: all drops happen before any creates, databases and
/// tables are created before the materialized views that read them, and indexes are created
/// last. Within the materialized-view phases the splitter additionally topologically sorts
/// by inter-view dependencies.
/// </summary>
internal enum MigrationPhase
{
    // ── Tear-down (most dependent first) ───────────────────────────────
    DropIndexes = 1,
    DropProjections = 2,
    DropMaterializedViews = 3,
    DropDictionaries = 4,
    DropTables = 5,
    DropDatabases = 6,

    // ── Build-up (least dependent first) ───────────────────────────────
    CreateDatabases = 7,
    // Renames act on pre-existing objects and must run before anything new references the old or
    // new name — in particular before a materialized view or dictionary created in the same
    // migration whose SELECT/SOURCE targets the renamed table.
    Renames = 8,
    CreateTables = 9,
    AddColumns = 10,
    CreateMaterializedViews = 11,
    CreateDictionaries = 12,
    AlterColumns = 13,
    // Projections and indexes are table-attached MergeTree sub-objects added via ALTER TABLE after
    // the table and its columns exist; they come last.
    CreateProjections = 14,
    CreateIndexes = 15,
}
