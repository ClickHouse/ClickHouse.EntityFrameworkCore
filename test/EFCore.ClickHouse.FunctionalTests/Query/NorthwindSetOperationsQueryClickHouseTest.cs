using Microsoft.EntityFrameworkCore.TestUtilities;

namespace Microsoft.EntityFrameworkCore.Query;

public class NorthwindSetOperationsQueryClickHouseTest
    : NorthwindSetOperationsQueryRelationalTestBase<NorthwindQueryClickHouseFixture<NoopModelCustomizer>>
{
    public NorthwindSetOperationsQueryClickHouseTest(NorthwindQueryClickHouseFixture<NoopModelCustomizer> fixture)
        : base(fixture)
    {
    }

    // ClickHouse cannot resolve identifiers from a UNION subquery alias in correlated subqueries
    // (lateral join / OUTER APPLY pattern). Server error: "Identifier 'u.CustomerID' cannot be resolved"
    public override Task Union_on_entity_with_correlated_collection(bool async)
        => Assert.ThrowsAsync<ClickHouse.Driver.ClickHouseServerException>(
            () => base.Union_on_entity_with_correlated_collection(async));

    public override Task Union_on_entity_plus_other_column_with_correlated_collection(bool async)
        => Assert.ThrowsAsync<ClickHouse.Driver.ClickHouseServerException>(
            () => base.Union_on_entity_plus_other_column_with_correlated_collection(async));

    // Client evaluation before set operations is not translatable (same as Npgsql)
    public override async Task Client_eval_Union_FirstOrDefault(bool async)
        => Assert.Equal(
            "Unable to translate set operation after client projection has been applied. Consider moving the set operation before the last 'Select' call.",
            (await Assert.ThrowsAsync<InvalidOperationException>(
                () => base.Client_eval_Union_FirstOrDefault(async))).Message);

    // ClickHouse's query optimizer pushes the outer City projection into the UNION ALL subquery,
    // causing the inner DISTINCT to operate on just City instead of all entity columns.
    // Result: 10 distinct cities instead of 11 rows (two México D.F. entries merge).
    // This is a ClickHouse column-pruning optimization, not a provider bug.
    //
    // Previously this override was Task.CompletedTask, which silently masked the wrong-result
    // behavior. Assert the specific observed mismatch so (a) the regression is documented,
    // (b) the test fails with a clear signal if the row counts change, and
    // (c) CI catches an unexpected fix (e.g. ClickHouse optimizer update) so we can remove
    // the workaround.
    public override async Task Concat_with_distinct_on_both_source_and_pruning(bool async)
    {
        var ex = await Assert.ThrowsAsync<Xunit.Sdk.EqualException>(
            () => base.Concat_with_distinct_on_both_source_and_pruning(async));

        Assert.Matches(@"Expected:\s*11\b", ex.Message);
        Assert.Matches(@"Actual:\s*10\b", ex.Message);
    }
}
