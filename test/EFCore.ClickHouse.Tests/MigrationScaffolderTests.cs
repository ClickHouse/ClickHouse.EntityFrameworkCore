using System.Reflection;
using ClickHouse.EntityFrameworkCore.Extensions;
using ClickHouse.EntityFrameworkCore.Metadata;
using ClickHouse.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Design.Internal;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Design;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

// DesignTimeServicesBuilder and the scaffolder are internal EF Core APIs.
#pragma warning disable EF1001

namespace EFCore.ClickHouse.Tests;

/// <summary>
/// Drives <c>ClickHouseMigrationsScaffolder</c> in-process via EF Core's <see cref="DesignTimeServicesBuilder"/>
/// — the same path <c>dotnet ef migrations add</c> uses, but inside the test process so the fragile
/// scaffolder logic (splitting, forward-only <c>Down</c> injection, brace-matching, using injection,
/// shared snapshot) is actually exercised and covered. No database required: the model uses a
/// <c>FromRaw</c> view and a dictionary, neither of which needs a live connection to scaffold.
/// </summary>
public class MigrationScaffolderTests
{
    [Fact]
    public void Scaffold_splits_multi_operation_migration_into_ordered_forward_only_steps()
    {
        using var context = new ScaffoldContext();
        var scaffolder = ResolveScaffolder(context);

        var migration = scaffolder.ScaffoldMigration("InitialCreate", rootNamespace: "TestRoot", subNamespace: "Migrations");

        var dir = Path.Combine(Path.GetTempPath(), "ch_scaffold_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var files = scaffolder.Save(dir, migration, outputDir: null);
            var outDir = Path.GetDirectoryName(files.MigrationFile)!;

            var stepFiles = Directory.GetFiles(outDir, "*.cs")
                .Where(f => !f.EndsWith(".Designer.cs", StringComparison.Ordinal)
                         && !f.EndsWith("ModelSnapshot.cs", StringComparison.Ordinal))
                .OrderBy(f => f, StringComparer.Ordinal)
                .ToArray();

            // 2 tables + 1 materialized view + 1 dictionary → multiple single-operation steps.
            Assert.True(stepFiles.Length > 1, $"Expected multiple step files; found {stepFiles.Length}.");

            // Step ids are suffixed _001, _002, … and therefore sort in dependency order.
            var suffixes = stepFiles.Select(f => Path.GetFileNameWithoutExtension(f)[^3..]).ToList();
            Assert.Equal(suffixes.OrderBy(s => s, StringComparer.Ordinal).ToList(), suffixes);

            foreach (var file in stepFiles)
            {
                var code = File.ReadAllText(file);
                // Forward-only Down, and the required usings were injected so the file compiles standalone.
                Assert.Contains("throw new ClickHouseDownMigrationNotSupportedException", code);
                Assert.Contains("using ClickHouse.EntityFrameworkCore.Migrations;", code);
                Assert.Contains("using ClickHouse.EntityFrameworkCore.Extensions;", code);
            }

            // One shared snapshot is written for the whole set (single history baseline).
            Assert.Single(Directory.GetFiles(outDir, "*ModelSnapshot.cs"));

            // The table create is ordered before the dictionary that reads from it.
            var tableStep = stepFiles.First(f => File.ReadAllText(f).Contains("CreateTable"));
            var dictStep = stepFiles.First(f => File.ReadAllText(f).Contains("CreateClickHouseDictionary"));
            Assert.True(
                string.CompareOrdinal(Path.GetFileName(tableStep), Path.GetFileName(dictStep)) < 0,
                "Expected the source table step to be ordered before the dictionary step.");
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void Single_operation_migration_diffs_against_existing_snapshot()
    {
        // A single-operation diff takes the base-scaffolder fallback path, which re-processes the
        // assembly's model snapshot for its own diff. Processing the snapshot's cached model twice
        // used to leave the second diff with a null source model, scaffolding the whole schema as
        // CreateTable instead of the actual one-operation delta (here: a column rename).
        using var context = new RenameContext();
        var scaffolder = ResolveScaffolder(context);

        var migration = scaffolder.ScaffoldMigration("RenameAToB", rootNamespace: "TestRoot", subNamespace: "Migrations");

        // Not split into steps: the delta really was a single operation.
        Assert.DoesNotMatch(@"_\d{3}$", migration.MigrationId);
        Assert.Contains("RenameColumn", migration.MigrationCode);
        Assert.DoesNotContain("CreateTable", migration.MigrationCode);
    }

    [Fact]
    public void Generated_step_Down_throws_forward_only_exception()
    {
        // The scaffolded Down is forward-only; constructing the exception it throws carries the step id.
        var ex = new ClickHouseDownMigrationNotSupportedException("20240101000000_InitialCreate_001");
        Assert.Equal("20240101000000_InitialCreate_001", ex.MigrationId);
        Assert.IsAssignableFrom<NotSupportedException>(ex);
    }

    private static IMigrationsScaffolder ResolveScaffolder(DbContext context)
    {
        var assembly = typeof(MigrationScaffolderTests).Assembly;
        var reporter = new OperationReporter(new OperationReportHandler());
        var builder = new DesignTimeServicesBuilder(assembly, assembly, reporter, []);
        var services = builder.Build(context);
        return services.GetRequiredService<IMigrationsScaffolder>();
    }

    private sealed class ScaffoldContext : DbContext
    {
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.UseClickHouse("Host=localhost;Database=scaffold_test");

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<HitsSource>(b =>
            {
                b.HasKey(e => e.Id);
                b.ToTable("hits_source", t => t.HasMergeTreeEngine().WithOrderBy("Id"));
            });

            modelBuilder.Entity<HitsByHour>(b =>
            {
                b.HasKey(e => e.Bucket);
                b.ToTable("hits_by_hour", t => t.HasSummingMergeTreeEngine("Hits").WithOrderBy("Bucket"));
            });

            modelBuilder.HasMaterializedView<HitsByHour>("hits_mv")
                .FromRaw("SELECT Id AS Bucket, Value AS Hits FROM hits_source");

            modelBuilder.HasDictionary<HitsSource>("hits_dict")
                .FromTable<HitsSource>()
                .HasKey(h => h.Id)
                .Layout(ClickHouseDictionaryLayout.Hashed)
                .Lifetime(60);
        }
    }

    private sealed class RenameContext : DbContext
    {
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.UseClickHouse("Host=localhost;Database=scaffold_test");

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<RenameItem>(b =>
            {
                b.HasKey(e => e.Id);
                b.Property(e => e.A).HasColumnName("B"); // column was "A" in the snapshot below
                b.ToTable("rename_items", t => t.HasMergeTreeEngine().WithOrderBy("Id"));
            });
    }

    // Stands in for the compiled snapshot a real project accumulates in its migrations assembly;
    // IMigrationsAssembly discovers it via the [DbContext] attribute. Mirrors RenameContext's
    // model except the renamed column, so the diff is exactly one RenameColumnOperation.
    [Microsoft.EntityFrameworkCore.Infrastructure.DbContext(typeof(RenameContext))]
    public class RenameContextModelSnapshot : Microsoft.EntityFrameworkCore.Infrastructure.ModelSnapshot
    {
        protected override void BuildModel(ModelBuilder modelBuilder)
        {
            modelBuilder.HasAnnotation("ProductVersion", "10.0.2");

            modelBuilder.Entity("EFCore.ClickHouse.Tests.MigrationScaffolderTests+RenameItem", b =>
            {
                b.Property<long>("Id").HasColumnType("Int64");
                b.Property<long>("A").HasColumnType("Int64");
                b.HasKey("Id");
                b.ToTable("rename_items", (string)null);

                b
                    .HasAnnotation("ClickHouse:Engine", "MergeTree")
                    .HasAnnotation("ClickHouse:OrderBy", new[] { "Id" });
            });
        }
    }

    private class RenameItem
    {
        public long Id { get; set; }
        public long A { get; set; }
    }

    private class HitsSource
    {
        public long Id { get; set; }
        public long Value { get; set; }
    }

    private class HitsByHour
    {
        public long Bucket { get; set; }
        public long Hits { get; set; }
    }
}
