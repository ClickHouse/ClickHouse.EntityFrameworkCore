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
    DropTables = 3,
    DropDatabases = 4,

    // ── Build-up (least dependent first) ───────────────────────────────
    CreateDatabases = 5,
    CreateTables = 6,
    AddColumns = 7,
    CreateMaterializedViews = 8,
    AlterColumns = 9,
    CreateIndexes = 10,
}
