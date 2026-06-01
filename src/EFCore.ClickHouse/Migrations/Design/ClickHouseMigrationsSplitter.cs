using ClickHouse.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace ClickHouse.EntityFrameworkCore.Migrations.Design;

/// <summary>
/// Orders a flat list of migration operations into dependency-respecting steps so that every
/// object exists before it is referenced. Operations are grouped into <see cref="MigrationPhase"/>s
/// and emitted phase-by-phase (drops before creates; databases and tables before the materialized
/// views that read them; indexes last). Within the materialized-view phases the operations are
/// additionally topologically sorted by inter-view dependencies, with cycle detection.
/// </summary>
/// <remarks>
/// For the current feature set (no dictionaries, no refreshable-view <c>DEPENDS ON</c>), phase
/// ordering alone already guarantees tables-before-views; the topological sort is cheap insurance
/// for future object kinds and provides a clear diagnostic if a circular materialized-view
/// reference is ever introduced. It is not, today, load-bearing on its own.
/// </remarks>
public sealed class ClickHouseMigrationsSplitter
{
    /// <summary>
    /// Sorts <paramref name="operations"/> by dependency and returns them as numbered steps.
    /// The <paramref name="model"/> is accepted for parity/future use and is currently unused.
    /// </summary>
    public IReadOnlyList<StepMigration> Split(IReadOnlyList<MigrationOperation> operations, IModel? model = null)
    {
        if (operations.Count == 0)
            return [];

        var sorted = SortOperationsByDependencies(operations);

        var steps = new List<StepMigration>(sorted.Count);
        for (var i = 0; i < sorted.Count; i++)
        {
            var (op, originalIndex) = sorted[i];
            steps.Add(new StepMigration(i + 1, GetOperationDescription(op), op, originalIndex));
        }

        return steps;
    }

    private static List<(MigrationOperation Op, int Index)> SortOperationsByDependencies(
        IReadOnlyList<MigrationOperation> operations)
    {
        var phaseGroups = new Dictionary<MigrationPhase, List<(MigrationOperation Op, int Index)>>();
        foreach (var phase in Enum.GetValues<MigrationPhase>())
            phaseGroups[phase] = [];

        for (var i = 0; i < operations.Count; i++)
            phaseGroups[ClassifyOperation(operations[i])].Add((operations[i], i));

        var result = new List<(MigrationOperation, int)>(operations.Count);

        // Tear-down: most dependent first. Materialized-view drops are reversed relative to
        // create order so a dependent view is dropped before what it reads.
        result.AddRange(phaseGroups[MigrationPhase.DropIndexes]);
        result.AddRange(SortByDependencies(phaseGroups[MigrationPhase.DropMaterializedViews], reverseForDrops: true));
        result.AddRange(phaseGroups[MigrationPhase.DropTables]);
        result.AddRange(phaseGroups[MigrationPhase.DropDatabases]);

        // Build-up: least dependent first. Tables exist before the views that read them.
        result.AddRange(phaseGroups[MigrationPhase.CreateDatabases]);
        result.AddRange(phaseGroups[MigrationPhase.CreateTables]);
        result.AddRange(phaseGroups[MigrationPhase.AddColumns]);
        result.AddRange(SortByDependencies(phaseGroups[MigrationPhase.CreateMaterializedViews], reverseForDrops: false));
        result.AddRange(phaseGroups[MigrationPhase.AlterColumns]);
        result.AddRange(phaseGroups[MigrationPhase.CreateIndexes]);

        return result;
    }

    private static MigrationPhase ClassifyOperation(MigrationOperation op) => op switch
    {
        DropIndexOperation => MigrationPhase.DropIndexes,
        ClickHouseDropMaterializedViewOperation => MigrationPhase.DropMaterializedViews,
        DropTableOperation => MigrationPhase.DropTables,
        ClickHouseDropDatabaseOperation => MigrationPhase.DropDatabases,
        ClickHouseCreateDatabaseOperation => MigrationPhase.CreateDatabases,
        CreateTableOperation => MigrationPhase.CreateTables,
        AddColumnOperation => MigrationPhase.AddColumns,
        ClickHouseCreateMaterializedViewOperation => MigrationPhase.CreateMaterializedViews,
        AlterColumnOperation or DropColumnOperation or RenameColumnOperation
            or RenameTableOperation or RenameIndexOperation => MigrationPhase.AlterColumns,
        CreateIndexOperation => MigrationPhase.CreateIndexes,
        // Data/SQL operations and anything unrecognized land after creates so they can rely on
        // tables/views existing, without preceding column alterations.
        _ => MigrationPhase.AlterColumns,
    };

    /// <summary>
    /// Topologically sorts a single phase via Kahn's algorithm. An edge runs from the operation
    /// that produces a table to the operation that reads it, so producers come first; for drop
    /// phases the result is reversed. A self-reference or cycle has no valid ordering and throws.
    /// </summary>
    private static List<(MigrationOperation Op, int Index)> SortByDependencies(
        List<(MigrationOperation Op, int Index)> operations,
        bool reverseForDrops)
    {
        // No early-return for a single op: a lone materialized view whose SELECT reads its own
        // target table is a self-cycle and must reach the cycle check below.
        if (operations.Count == 0)
            return operations;

        // Map each produced table name to the operations (by local node index) that produce it.
        var producedBy = new Dictionary<string, List<int>>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < operations.Count; i++)
        {
            var name = GetProducedName(operations[i].Op);
            if (name is null)
                continue;
            if (!producedBy.TryGetValue(name, out var producers))
                producedBy[name] = producers = [];
            producers.Add(i);
        }

        var adjacency = new List<HashSet<int>>(operations.Count);
        for (var i = 0; i < operations.Count; i++)
            adjacency.Add([]);
        var inDegree = new int[operations.Count];

        for (var i = 0; i < operations.Count; i++)
        {
            foreach (var dep in GetDependencies(operations[i].Op))
            {
                if (!producedBy.TryGetValue(dep, out var producers))
                    continue;
                foreach (var producer in producers)
                {
                    if (adjacency[producer].Add(i))
                        inDegree[i]++;
                }
            }
        }

        var queue = new Queue<int>();
        for (var i = 0; i < operations.Count; i++)
            if (inDegree[i] == 0)
                queue.Enqueue(i);

        var sorted = new List<(MigrationOperation, int)>(operations.Count);
        while (queue.Count > 0)
        {
            var node = queue.Dequeue();
            sorted.Add(operations[node]);
            foreach (var dependent in adjacency[node].OrderBy(x => x))
                if (--inDegree[dependent] == 0)
                    queue.Enqueue(dependent);
        }

        if (sorted.Count != operations.Count)
        {
            var members = string.Join(", ", Enumerable.Range(0, operations.Count)
                .Where(i => inDegree[i] > 0)
                .Select(i => GetProducedName(operations[i].Op) ?? "<unknown>")
                .OrderBy(s => s, StringComparer.Ordinal));
            throw new InvalidOperationException(
                $"Circular dependency detected between materialized view operations: {members}. " +
                "Each member reads a table produced by another in the cycle, so no valid create / " +
                "drop ordering exists. Break the cycle by removing one of the cross-references.");
        }

        if (reverseForDrops)
            sorted.Reverse();

        return sorted;
    }

    // The table an operation produces (and that others may read). Materialized-view creates produce
    // their TO target; drops are keyed by view name (drops carry no SELECT, so they form no edges).
    private static string? GetProducedName(MigrationOperation op) => op switch
    {
        ClickHouseCreateMaterializedViewOperation mv => NormalizeName(mv.TargetTable),
        ClickHouseDropMaterializedViewOperation mv => NormalizeName(mv.ViewName),
        _ => null,
    };

    // The tables an operation reads. Only materialized-view creates expose a SELECT to parse.
    private static IReadOnlyCollection<string> GetDependencies(MigrationOperation op) => op switch
    {
        ClickHouseCreateMaterializedViewOperation mv => SqlTableExtractor.ExtractTableReferences(mv.SelectQuery),
        _ => [],
    };

    private static string NormalizeName(string name) => name.Trim().Trim('`').Trim().ToLowerInvariant();

    private static string GetOperationDescription(MigrationOperation op) => op switch
    {
        CreateTableOperation o => $"Create table {o.Name}",
        DropTableOperation o => $"Drop table {o.Name}",
        AddColumnOperation o => $"Add column {o.Table}.{o.Name}",
        DropColumnOperation o => $"Drop column {o.Table}.{o.Name}",
        AlterColumnOperation o => $"Alter column {o.Table}.{o.Name}",
        RenameColumnOperation o => $"Rename column {o.Table}.{o.Name} to {o.NewName}",
        RenameTableOperation o => $"Rename table {o.Name} to {o.NewName}",
        CreateIndexOperation o => $"Create index {o.Name} on {o.Table}",
        DropIndexOperation o => $"Drop index {o.Name}",
        ClickHouseCreateMaterializedViewOperation o => $"Create materialized view {o.ViewName}",
        ClickHouseDropMaterializedViewOperation o => $"Drop materialized view {o.ViewName}",
        ClickHouseCreateDatabaseOperation o => $"Create database {o.Name}",
        ClickHouseDropDatabaseOperation o => $"Drop database {o.Name}",
        _ => op.GetType().Name,
    };
}
