using System.Text.RegularExpressions;
using ClickHouse.EntityFrameworkCore.Extensions;
using ClickHouse.EntityFrameworkCore.Metadata;
using ClickHouse.EntityFrameworkCore.Migrations.Design;
using ClickHouse.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Design.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Design;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

// DesignTimeServicesBuilder and the scaffolder are internal EF Core APIs.
#pragma warning disable EF1001

namespace EFCore.ClickHouse.Tests;

/// <summary>
/// Regression tests for the model-evolution bugs found in the 2026-07-05 review of the migrations
/// feature stack (materialized views / step splitting / dictionaries / projections). Each test
/// pins one confirmed finding by asserting the correct behavior; all now pass against the fixes.
/// One further finding (`migrations remove` silently losing the last step's DDL) needs the real
/// CLI round-trip and lives in <see cref="DotnetEfCliTests"/>.
/// </summary>
public class MigrationEvolutionBugTests
{
    // ── Finding 1: LINQ-defined SELECT bodies are never persisted to the model snapshot ─────────
    // The builder stores only the transient PendingLambda annotation; the snapshot generator strips
    // it and no SelectSql annotation is ever written. Every subsequent `migrations add` then diffs
    // snapshot SelectSql=null against freshly translated SQL: MVs get a spurious Drop+Create in
    // every migration, and a LINQ projection (zero surviving annotations) is re-emitted as ADD
    // PROJECTION without IF NOT EXISTS, which failed on apply with "projection already exists".

    [Fact]
    public void Linq_view_select_sql_is_persisted_in_the_snapshot()
    {
        using var context = new LinqMvContext();
        var migration = ResolveScaffolder(context).ScaffoldMigration("InitialCreate", "TestRoot", "Migrations");

        Assert.Contains("ClickHouse:MaterializedView:mv1:SelectSql", migration.SnapshotCode);
    }

    [Fact]
    public void Linq_projection_select_sql_is_persisted_in_the_snapshot()
    {
        using var context = new LinqProjectionContext();
        var migration = ResolveScaffolder(context).ScaffoldMigration("InitialCreate", "TestRoot", "Migrations");

        Assert.Contains("ClickHouse:Projection:p1:SelectSql", migration.SnapshotCode);
    }

    // ── Finding 4: ViewsEqual never compares the resolved target TABLE name ─────────────────────
    // Renaming an MV's target table emits only RenameTable; the physical MV keeps `TO old_table`.

    [Fact]
    public void Materialized_view_is_recreated_when_its_target_table_is_renamed()
    {
        var ops = Diff(
            previous: b =>
            {
                DefineMvEntities(b, targetTable: "target");
                b.HasMaterializedView<Target>("mv1").FromRaw("SELECT * FROM source");
            },
            current: b =>
            {
                DefineMvEntities(b, targetTable: "target_v2");
                b.HasMaterializedView<Target>("mv1").FromRaw("SELECT * FROM source");
            });

        // The deployed MV's TO clause is baked at create time, so a target-table rename must
        // drop+recreate the view; the pre-fix differ emitted only the RenameTable op.
        Assert.NotEmpty(ops.OfType<ClickHouseCreateMaterializedViewOperation>());
    }

    // ── Finding 5: DictionariesEqual never compares the resolved source table name ──────────────
    // Renaming the dictionary's source table leaves SOURCE(... TABLE 'old') → UNKNOWN_TABLE on the
    // next LIFETIME reload.

    [Fact]
    public void Dictionary_is_recreated_when_its_source_table_is_renamed()
    {
        var ops = Diff(
            previous: b =>
            {
                DefineSource(b, table: "source");
                b.HasDictionary<Source>("dict1").FromTable<Source>()
                    .HasKey(s => s.Id).Layout(ClickHouseDictionaryLayout.Hashed).Lifetime(60);
            },
            current: b =>
            {
                DefineSource(b, table: "source_v2");
                b.HasDictionary<Source>("dict1").FromTable<Source>()
                    .HasKey(s => s.Id).Layout(ClickHouseDictionaryLayout.Hashed).Lifetime(60);
            });

        Assert.NotEmpty(ops.OfType<ClickHouseCreateDictionaryOperation>());
    }

    // ── Finding 3: the splitter reorders renames after MV/dictionary creates ────────────────────
    // RenameTable/RenameColumn land in the AlterColumns phase, which runs after
    // CreateMaterializedViews/CreateDictionaries — inverting the differ's rename-before-create
    // order, so a create referencing the renamed name fails with UNKNOWN_TABLE at apply time.

    [Fact]
    public void Rename_table_step_runs_before_creates_that_reference_the_new_name()
    {
        var ops = new List<MigrationOperation>
        {
            new RenameTableOperation { Name = "orders", NewName = "orders_v2" },
            new ClickHouseCreateMaterializedViewOperation
            {
                ViewName = "mv1",
                TargetTable = "mv1_target",
                SelectQuery = "SELECT * FROM orders_v2",
            },
        };

        var steps = new ClickHouseMigrationsSplitter().Split(ops);
        var renameIndex = IndexOf(steps, s => s.Operation is RenameTableOperation);
        var createViewIndex = IndexOf(steps, s => s.Operation is ClickHouseCreateMaterializedViewOperation);

        Assert.True(renameIndex < createViewIndex,
            $"The rename must be applied before the view that reads the new name; got: {Describe(steps)}");
    }

    // ── Finding 10: an MV read by another MV *by view name* gets no dependency edge ─────────────
    // GetProducedName registers only the TO target table, never the view's own name, so the
    // topological sort can order the reader before the view it reads.

    [Fact]
    public void View_reading_another_new_view_is_created_after_it()
    {
        var ops = new List<MigrationOperation>
        {
            new ClickHouseCreateMaterializedViewOperation
            {
                ViewName = "mv2",
                TargetTable = "mv2_target",
                SelectQuery = "SELECT * FROM mv1", // reads the other view by its own name — legal ClickHouse
            },
            new ClickHouseCreateMaterializedViewOperation
            {
                ViewName = "mv1",
                TargetTable = "mv1_target",
                SelectQuery = "SELECT * FROM src",
            },
        };

        var steps = new ClickHouseMigrationsSplitter().Split(ops);
        var mv1Index = IndexOf(steps, s => s.Operation is ClickHouseCreateMaterializedViewOperation { ViewName: "mv1" });
        var mv2Index = IndexOf(steps, s => s.Operation is ClickHouseCreateMaterializedViewOperation { ViewName: "mv2" });

        Assert.True(mv1Index < mv2Index,
            $"mv1 must exist before mv2 reads it; got: {Describe(steps)}");
    }

    // ── Finding 6: the projection rewriter's alias strip corrupts string literals ───────────────
    // The bare-alias regex (\b<alias>\.) runs over the whole SQL, including literal text.

    [Fact]
    public void Alias_stripping_does_not_corrupt_string_literals()
    {
        const string sql =
            "SELECT CASE WHEN `e`.`Amount` > 100 THEN 'e.g. high' ELSE 'low' END AS `Label` FROM `events` AS `e`";

        var result = ProjectionSqlRewriter.Rewrite(sql);

        Assert.Contains("'e.g. high'", result); // the pre-fix regex corrupted this to 'g. high'
    }

    // ── Finding 7: closure captures leave parameter placeholders in the DDL ─────────────────────
    // TranslateToSql bakes ToQueryString() output verbatim into the CREATE, so a captured local
    // becomes "-- threshold='42'\n... WHERE Id > {threshold:Int64}" — invalid in view DDL.

    [Fact]
    public void Captured_variable_in_linq_view_is_inlined_into_the_ddl()
    {
        var threshold = 42L;
        var ops = Diff(
            previous: b => DefineMvEntities(b, targetTable: "target"),
            current: b =>
            {
                DefineMvEntities(b, targetTable: "target");
                b.HasMaterializedView<Target>("mv1")
                    .From<Source>()
                    .Select(q => q
                        .Where(s => s.Id > threshold)
                        .GroupBy(s => s.Id)
                        .Select(g => new Target { Hour = g.Key, Hits = g.Count() }));
            });

        var create = Assert.Single(ops.OfType<ClickHouseCreateMaterializedViewOperation>());
        Assert.DoesNotContain("{threshold", create.SelectQuery); // no ClickHouse query-parameter placeholder
        Assert.DoesNotContain("--", create.SelectQuery);         // no parameter-comment preamble
        Assert.Contains("42", create.SelectQuery);               // the captured value is inlined
    }

    // ── Finding 8: identity strings carry the assembly version ──────────────────────────────────
    // TargetType/SourceType/ColumnClrTypes store Type.AssemblyQualifiedName (Version=...) and the
    // differ compares the raw strings, so a version bump re-diffs every MV/dictionary as changed.

    [Fact]
    public void Assembly_version_bump_does_not_rediff_unchanged_views()
    {
        // Simulate the snapshot written by an older build of the same model: identical except the
        // assembly version embedded in the TargetType identity string.
        var bumpedAqn = Regex.Replace(
            typeof(Target).AssemblyQualifiedName!, @"Version=[\d.]+", "Version=9.9.9.9");

        var ops = Diff(
            previous: b =>
            {
                DefineMvEntities(b, targetTable: "target");
                b.HasMaterializedView<Target>("mv1").FromRaw("SELECT * FROM source");
                b.HasAnnotation("ClickHouse:MaterializedView:mv1:TargetType", bumpedAqn);
            },
            current: b =>
            {
                DefineMvEntities(b, targetTable: "target");
                b.HasMaterializedView<Target>("mv1").FromRaw("SELECT * FROM source");
            });

        Assert.Empty(ops.OfType<ClickHouseDropMaterializedViewOperation>());
        Assert.Empty(ops.OfType<ClickHouseCreateMaterializedViewOperation>());
    }

    // ── Finding 9: the multi-operation scaffold path bypasses base-scaffolder behaviors ─────────
    // (a) rootNamespace is ignored, (b) duplicate migration names are not rejected, (c) an
    // existing differently-named snapshot class is not reused (a context rename then produces a
    // second ModelSnapshot class for the same context, resolved arbitrarily).

    [Fact]
    public void Multi_op_migration_honors_root_namespace()
    {
        using var context = new MultiOpContext();
        var migration = ResolveScaffolder(context).ScaffoldMigration("MultiOp", rootNamespace: "TestRoot", subNamespace: "Migrations");

        Assert.Contains("namespace TestRoot.Migrations", migration.MigrationCode);
    }

    [Fact]
    public void Multi_op_migration_rejects_duplicate_migration_name()
    {
        // ExistingMigration below is a compiled migration named "ExistingMigration" for this
        // context; the base scaffolder throws DuplicateMigrationName before scaffolding.
        using var context = new MultiOpContext();
        var scaffolder = ResolveScaffolder(context);

        Assert.ThrowsAny<Exception>(
            () => scaffolder.ScaffoldMigration("ExistingMigration", "TestRoot", "Migrations"));
    }

    [Fact]
    public void Multi_op_migration_reuses_existing_snapshot_class_name()
    {
        // LegacySnapshotName below plays the snapshot left behind after the context class was
        // renamed; base reuses its name instead of introducing a second snapshot class.
        using var context = new RenamedSnapshotContext();
        var migration = ResolveScaffolder(context).ScaffoldMigration("AfterRename", "TestRoot", "Migrations");

        Assert.Equal("LegacySnapshotName", migration.SnapshotName);
    }

    // ── harness ──────────────────────────────────────────────────────────────────────────────────

    private static void DefineMvEntities(ModelBuilder b, string targetTable)
    {
        DefineSource(b, table: "source");
        b.Entity<Target>(e => { e.HasKey(x => x.Hour); e.ToTable(targetTable); });
    }

    private static void DefineSource(ModelBuilder b, string table)
        => b.Entity<Source>(e => { e.HasKey(x => x.Id); e.ToTable(table); });

    private static int IndexOf(IReadOnlyList<StepMigration> steps, Func<StepMigration, bool> predicate)
    {
        for (var i = 0; i < steps.Count; i++)
            if (predicate(steps[i]))
                return i;
        return -1;
    }

    private static string Describe(IReadOnlyList<StepMigration> steps)
        => string.Join(" | ", steps.Select(s => s.OperationDescription));

    private static IList<MigrationOperation> Diff(
        Action<ModelBuilder> previous,
        Action<ModelBuilder> current)
    {
        using var prevCtx = new TestContext(previous);
        using var currCtx = new TestContext(current);
        var prevModel = prevCtx.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var currModel = currCtx.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var differ = currCtx.GetService<IMigrationsModelDiffer>();
        return differ.GetDifferences(prevModel, currModel).ToList();
    }

    private static IMigrationsScaffolder ResolveScaffolder(DbContext context)
    {
        var assembly = typeof(MigrationEvolutionBugTests).Assembly;
        var reporter = new OperationReporter(new OperationReportHandler());
        var services = new DesignTimeServicesBuilder(assembly, assembly, reporter, []).Build(context);
        return services.GetRequiredService<IMigrationsScaffolder>();
    }

    public class Source
    {
        public long Id { get; set; }
        public long Value { get; set; }
    }

    public class Target
    {
        public long Hour { get; set; }
        public int Hits { get; set; }
    }

    private sealed class TestContext : DbContext
    {
        private readonly Action<ModelBuilder> _configure;
        public TestContext(Action<ModelBuilder> configure) => _configure = configure;
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.UseClickHouse("Host=localhost;Database=evolution_test").EnableServiceProviderCaching(false);
        protected override void OnModelCreating(ModelBuilder modelBuilder) => _configure(modelBuilder);
    }

    private sealed class LinqMvContext : DbContext
    {
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.UseClickHouse("Host=localhost;Database=evolution_test");

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            DefineMvEntities(modelBuilder, targetTable: "target");
            modelBuilder.HasMaterializedView<Target>("mv1")
                .From<Source>()
                .Select(q => q.GroupBy(s => s.Id).Select(g => new Target { Hour = g.Key, Hits = g.Count() }));
        }
    }

    private sealed class LinqProjectionContext : DbContext
    {
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.UseClickHouse("Host=localhost;Database=evolution_test");

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<Source>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("source", t => t.HasMergeTreeEngine().WithOrderBy("Id"));
                e.HasProjection("p1")
                    .Select(q => q.GroupBy(s => s.Value).Select(g => new { Value = g.Key, Cnt = g.Count() }));
            });
    }

    private sealed class MultiOpContext : DbContext
    {
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.UseClickHouse("Host=localhost;Database=evolution_test");

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            // Two tables → two operations → the splitter (multi-op) path.
            modelBuilder.Entity<Source>(e => { e.HasKey(x => x.Id); e.ToTable("source"); });
            modelBuilder.Entity<Target>(e => { e.HasKey(x => x.Hour); e.ToTable("target"); });
        }
    }

    /// <summary>A compiled migration named ExistingMigration for <see cref="MultiOpContext"/>.</summary>
    [Microsoft.EntityFrameworkCore.Infrastructure.DbContext(typeof(MultiOpContext))]
    [Migration("20240101000000_ExistingMigration")]
    public class ExistingMigration : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder) { }
    }

    private sealed class RenamedSnapshotContext : DbContext
    {
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.UseClickHouse("Host=localhost;Database=evolution_test");

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Source>(e => { e.HasKey(x => x.Id); e.ToTable("source"); });
            modelBuilder.Entity<Target>(e => { e.HasKey(x => x.Hour); e.ToTable("target"); });
        }
    }

    /// <summary>The snapshot class left behind after <see cref="RenamedSnapshotContext"/> was renamed.</summary>
    [Microsoft.EntityFrameworkCore.Infrastructure.DbContext(typeof(RenamedSnapshotContext))]
    public class LegacySnapshotName : ModelSnapshot
    {
        protected override void BuildModel(ModelBuilder modelBuilder) { }
    }
}
