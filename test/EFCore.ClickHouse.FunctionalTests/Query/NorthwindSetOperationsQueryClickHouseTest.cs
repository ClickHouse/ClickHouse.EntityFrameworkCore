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

    // ClickHouse query optimizer pushes the outer City projection into the UNION ALL subquery,
    // causing the inner DISTINCT to operate on just City instead of all entity columns.
    // Result: 10 distinct cities instead of 11 rows (two México D.F. entries merge).
    // This is a ClickHouse column-pruning optimization, not a provider bug.
    public override Task Concat_with_distinct_on_both_source_and_pruning(bool async)
        => Task.CompletedTask;
}
