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
/// Scaffolds a LINQ-defined materialized view through the design-time pipeline that
/// <c>dotnet ef migrations add</c> uses. A LINQ view body stashes a non-serializable
/// <c>PendingMaterializedViewQuery</c> delegate on the model that is only translated at differ
/// time; the snapshot generator must drop it (it filters annotations via
/// <c>FilterIgnoredAnnotations</c> only, never <c>RemoveAnnotationsHandledByConventions</c>), or
/// scaffolding throws trying to emit the delegate as a C# literal.
/// </summary>
public class MaterializedViewScaffoldTests
{
    [Fact]
    public void Scaffold_with_linq_materialized_view_succeeds_and_omits_pending_lambda_from_snapshot()
    {
        using var context = new MvContext();
        var assembly = typeof(MaterializedViewScaffoldTests).Assembly;
        var reporter = new OperationReporter(new OperationReportHandler());
        var services = new DesignTimeServicesBuilder(assembly, assembly, reporter, []).Build(context);
        var scaffolder = services.GetRequiredService<IMigrationsScaffolder>();

        var migration = scaffolder.ScaffoldMigration("InitialCreate", "TestRoot", "Migrations");

        Assert.DoesNotContain("PendingMaterializedViewQuery", migration.SnapshotCode);
        Assert.DoesNotContain("PendingLambda", migration.SnapshotCode);
    }

    private sealed class MvContext : DbContext
    {
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.UseClickHouse("Host=localhost;Database=mv_scaffold_test");

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Event>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("events", t => t.HasMergeTreeEngine().WithOrderBy("Id"));
            });

            modelBuilder.Entity<HitsByHour>(e =>
            {
                e.HasKey(x => x.Bucket);
                e.ToTable("hits_by_hour", t => t.HasSummingMergeTreeEngine("Hits").WithOrderBy("Bucket"));
            });

            modelBuilder.HasMaterializedView<HitsByHour>("hits_mv")
                .From<Event>()
                .Select(src => src
                    .GroupBy(e => e.Id)
                    .Select(g => new HitsByHour { Bucket = g.Key, Hits = (long)g.Count() }));
        }
    }

    private class Event
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
