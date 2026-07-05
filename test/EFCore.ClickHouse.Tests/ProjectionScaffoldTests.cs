using ClickHouse.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Design.Internal;
using Microsoft.EntityFrameworkCore.Migrations.Design;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

// DesignTimeServicesBuilder and the scaffolder are internal EF Core APIs.
#pragma warning disable EF1001

namespace EFCore.ClickHouse.Tests;

/// <summary>
/// Drives <c>ClickHouseMigrationsScaffolder</c> end-to-end for a table that carries projections —
/// the same path <c>dotnet ef migrations add</c> uses. Projections defined with LINQ
/// (<c>.Select(...)</c>) stash a non-serializable <c>PendingProjectionQuery</c> delegate on the
/// entity type that is only translated at differ time; the snapshot generator and the
/// CreateTable C# generator both used to choke on it. These tests pin that scaffolding a LINQ
/// projection succeeds and produces a clean snapshot and a clean CreateTable operation.
/// </summary>
public class ProjectionScaffoldTests
{
    [Fact]
    public void Scaffold_with_linq_projection_succeeds_and_omits_pending_lambda_from_snapshot()
    {
        var (scaffolder, _) = Resolve();

        var migration = scaffolder.ScaffoldMigration("InitialCreate", "TestRoot", "Migrations");

        // The transient pending-lambda delegate must never reach the generated snapshot.
        Assert.DoesNotContain("PendingProjectionQuery", migration.SnapshotCode);
        Assert.DoesNotContain("PendingLambda", migration.SnapshotCode);
    }

    [Fact]
    public void CreateTable_step_omits_projection_annotations_and_projection_is_a_separate_step()
    {
        var (scaffolder, _) = Resolve();
        var migration = scaffolder.ScaffoldMigration("InitialCreate", "TestRoot", "Migrations");

        var dir = Path.Combine(Path.GetTempPath(), "ch_projscaffold_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var files = scaffolder.Save(dir, migration, outputDir: null);
            var outDir = Path.GetDirectoryName(files.MigrationFile)!;
            var stepFiles = Directory.GetFiles(outDir, "*.cs")
                .Where(f => !f.EndsWith(".Designer.cs", StringComparison.Ordinal)
                         && !f.EndsWith("ModelSnapshot.cs", StringComparison.Ordinal))
                .ToArray();

            var createTableStep = stepFiles.Single(f => File.ReadAllText(f).Contains("CreateTable"));
            var createTableCode = File.ReadAllText(createTableStep);

            // The table operation must not carry projection annotations — projections are their own steps.
            Assert.DoesNotContain("ClickHouse:Projection:", createTableCode);
            // …but it must still carry the engine annotation it legitimately owns.
            Assert.Contains("ClickHouse:Engine", createTableCode);

            // The projection is emitted as its own AddClickHouseProjection step, with the LINQ body
            // translated to a FROM-less SELECT.
            var projStep = stepFiles.Single(f => File.ReadAllText(f).Contains("AddClickHouseProjection"));
            var projCode = File.ReadAllText(projStep);
            Assert.Contains("GROUP BY", projCode);
            Assert.DoesNotContain("FROM", projCode.Split("selectQuery:")[1].Split(',')[0]);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    private static (IMigrationsScaffolder Scaffolder, DbContext Context) Resolve()
    {
        var context = new ProjectionContext();
        var assembly = typeof(ProjectionScaffoldTests).Assembly;
        var reporter = new OperationReporter(new OperationReportHandler());
        var builder = new DesignTimeServicesBuilder(assembly, assembly, reporter, []);
        var services = builder.Build(context);
        return (services.GetRequiredService<IMigrationsScaffolder>(), context);
    }

    private sealed class ProjectionContext : DbContext
    {
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.UseClickHouse("Host=localhost;Database=proj_scaffold_test");

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Hit>(b =>
            {
                b.HasKey(e => e.Id);
                b.ToTable("hits", t => t.HasMergeTreeEngine().WithOrderBy("Id"));

                b.HasProjection("proj_by_user")
                    .Select(q => q.GroupBy(h => h.UserId).Select(g => new { g.Key, Hits = g.Count() }));
            });
        }
    }

    private class Hit
    {
        public long Id { get; set; }
        public int UserId { get; set; }
    }
}
