using ClickHouse.EntityFrameworkCore.Migrations;
using ClickHouse.EntityFrameworkCore.Migrations.Design;
using ClickHouse.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Xunit;

namespace EFCore.ClickHouse.Tests;

/// <summary>
/// Unit tests for <see cref="ClickHouseMigrationsSplitter"/> — phase ordering, step numbering,
/// materialized-view topological ordering, and cycle detection. Pure, no database.
/// </summary>
public class MigrationSplitterTests
{
    private readonly ClickHouseMigrationsSplitter _splitter = new();

    [Fact]
    public void Empty_input_yields_no_steps()
        => Assert.Empty(_splitter.Split([]));

    [Fact]
    public void Single_operation_yields_one_numbered_step()
    {
        var steps = _splitter.Split([new CreateTableOperation { Name = "t" }]);

        var step = Assert.Single(steps);
        Assert.Equal(1, step.StepNumber);
        Assert.Equal("001", step.StepSuffix);
        Assert.Equal(0, step.OriginalIndex);
    }

    [Fact]
    public void Steps_are_numbered_sequentially_with_three_digit_suffixes()
    {
        var steps = _splitter.Split(
        [
            new CreateTableOperation { Name = "a" },
            new CreateTableOperation { Name = "b" },
            new CreateTableOperation { Name = "c" },
        ]);

        Assert.Equal([1, 2, 3], steps.Select(s => s.StepNumber));
        Assert.Equal(["001", "002", "003"], steps.Select(s => s.StepSuffix));
    }

    [Fact]
    public void Create_table_is_ordered_before_materialized_view_that_reads_it()
    {
        // Supplied view-first; the splitter must move the table create ahead of it.
        var steps = _splitter.Split(
        [
            new ClickHouseCreateMaterializedViewOperation
            {
                ViewName = "mv", TargetTable = "tgt", SelectQuery = "SELECT * FROM src",
            },
            new CreateTableOperation { Name = "src" },
            new CreateTableOperation { Name = "tgt" },
        ]);

        Assert.True(IndexOfTable(steps, "src") < IndexOfView(steps, "mv"));
        Assert.True(IndexOfTable(steps, "tgt") < IndexOfView(steps, "mv"));
    }

    [Fact]
    public void Create_database_is_ordered_before_create_table()
    {
        var steps = _splitter.Split(
        [
            new CreateTableOperation { Name = "t" },
            new ClickHouseCreateDatabaseOperation { Name = "db" },
        ]);

        Assert.IsType<ClickHouseCreateDatabaseOperation>(steps[0].Operation);
        Assert.IsType<CreateTableOperation>(steps[1].Operation);
    }

    [Fact]
    public void Materialized_view_drops_precede_table_drops()
    {
        var steps = _splitter.Split(
        [
            new DropTableOperation { Name = "src" },
            new ClickHouseDropMaterializedViewOperation { ViewName = "mv" },
        ]);

        Assert.IsType<ClickHouseDropMaterializedViewOperation>(steps[0].Operation);
        Assert.IsType<DropTableOperation>(steps[1].Operation);
    }

    [Fact]
    public void All_drops_precede_all_creates()
    {
        var steps = _splitter.Split(
        [
            new CreateTableOperation { Name = "new" },
            new DropTableOperation { Name = "old" },
        ]);

        Assert.IsType<DropTableOperation>(steps[0].Operation);
        Assert.IsType<CreateTableOperation>(steps[1].Operation);
    }

    [Fact]
    public void Dependent_materialized_view_is_created_after_the_one_it_reads()
    {
        // mv_b reads mv_a's target table, so mv_a must be created first — even though supplied last.
        var steps = _splitter.Split(
        [
            new ClickHouseCreateMaterializedViewOperation
            {
                ViewName = "mv_b", TargetTable = "b_target", SelectQuery = "SELECT * FROM a_target",
            },
            new ClickHouseCreateMaterializedViewOperation
            {
                ViewName = "mv_a", TargetTable = "a_target", SelectQuery = "SELECT * FROM base_table",
            },
        ]);

        Assert.True(IndexOfView(steps, "mv_a") < IndexOfView(steps, "mv_b"));
    }

    [Fact]
    public void Self_referencing_materialized_view_throws_cycle_error()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => _splitter.Split(
        [
            new ClickHouseCreateMaterializedViewOperation
            {
                ViewName = "loop", TargetTable = "loop_tbl", SelectQuery = "SELECT * FROM loop_tbl",
            },
        ]));

        Assert.Contains("Circular dependency", ex.Message);
    }

    [Fact]
    public void Mutually_dependent_materialized_views_throw_cycle_error()
    {
        Assert.Throws<InvalidOperationException>(() => _splitter.Split(
        [
            new ClickHouseCreateMaterializedViewOperation
            {
                ViewName = "x", TargetTable = "x_tbl", SelectQuery = "SELECT * FROM y_tbl",
            },
            new ClickHouseCreateMaterializedViewOperation
            {
                ViewName = "y", TargetTable = "y_tbl", SelectQuery = "SELECT * FROM x_tbl",
            },
        ]));
    }

    [Fact]
    public void Dictionary_create_is_ordered_after_its_source_table()
    {
        var steps = _splitter.Split(
        [
            new ClickHouseCreateDictionaryOperation
            {
                DictionaryName = "d", Columns = [new ClickHouseDictionaryColumn("Id", "UInt64")],
                KeyColumns = ["Id"], SourceTable = "src", Layout = "HASHED",
            },
            new CreateTableOperation { Name = "src" },
        ]);

        Assert.True(IndexOfTable(steps, "src") < FindIndex(steps,
            s => s.Operation is ClickHouseCreateDictionaryOperation));
    }

    [Fact]
    public void Dictionary_drop_precedes_table_drop()
    {
        var steps = _splitter.Split(
        [
            new DropTableOperation { Name = "src" },
            new ClickHouseDropDictionaryOperation { DictionaryName = "d" },
        ]);

        Assert.IsType<ClickHouseDropDictionaryOperation>(steps[0].Operation);
        Assert.IsType<DropTableOperation>(steps[1].Operation);
    }

    private static int IndexOfTable(IReadOnlyList<StepMigration> steps, string name)
        => FindIndex(steps, s => s.Operation is CreateTableOperation t && t.Name == name);

    private static int IndexOfView(IReadOnlyList<StepMigration> steps, string viewName)
        => FindIndex(steps, s => s.Operation is ClickHouseCreateMaterializedViewOperation v && v.ViewName == viewName);

    private static int FindIndex(IReadOnlyList<StepMigration> steps, Func<StepMigration, bool> predicate)
    {
        for (var i = 0; i < steps.Count; i++)
            if (predicate(steps[i]))
                return i;
        return -1;
    }
}
