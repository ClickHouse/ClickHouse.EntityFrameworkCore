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
    DropMaterializedViews = 2,
    DropDictionaries = 3,
    DropTables = 4,
    DropDatabases = 5,

    // ── Build-up (least dependent first) ───────────────────────────────
    CreateDatabases = 6,
    CreateTables = 7,
    AddColumns = 8,
    CreateMaterializedViews = 9,
    CreateDictionaries = 10,
    AlterColumns = 11,
    CreateIndexes = 12,
}
