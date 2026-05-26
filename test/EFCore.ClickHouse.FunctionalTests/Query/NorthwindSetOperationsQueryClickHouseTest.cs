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

    // ClickHouse's query optimizer historically pushed the outer City projection into the
    // UNION ALL subquery, causing the inner DISTINCT to operate on just City instead of all
    // entity columns (10 rows instead of 11; two México D.F. entries merge). Newer ClickHouse
    // versions fix this and return the correct 11 rows. Accept either outcome until the older
    // image is no longer in active use; assert the specific mismatch pattern when we do see
    // the legacy behavior, so a different wrong answer still fails loudly.
    public override async Task Concat_with_distinct_on_both_source_and_pruning(bool async)
    {
        try
        {
            await base.Concat_with_distinct_on_both_source_and_pruning(async);
        }
        catch (Xunit.Sdk.EqualException ex)
        {
            Assert.Matches(@"Expected:\s*11\b", ex.Message);
            Assert.Matches(@"Actual:\s*10\b", ex.Message);
        }
    }
}
