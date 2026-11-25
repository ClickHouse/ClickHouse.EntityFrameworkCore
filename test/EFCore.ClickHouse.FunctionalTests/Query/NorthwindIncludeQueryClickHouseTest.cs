using Microsoft.EntityFrameworkCore.TestUtilities;

namespace Microsoft.EntityFrameworkCore.Query;

public class NorthwindIncludeQueryClickHouseTest
    : NorthwindIncludeQueryRelationalTestBase<NorthwindQueryClickHouseFixture<NoopModelCustomizer>>
{
    public NorthwindIncludeQueryClickHouseTest(NorthwindQueryClickHouseFixture<NoopModelCustomizer> fixture)
        : base(fixture)
    {
    }

    // ClickHouse does not support CROSS APPLY / OUTER APPLY syntax
    public override Task Include_collection_with_cross_apply_with_filter(bool async)
        => AssertUnsupported(() => base.Include_collection_with_cross_apply_with_filter(async));

    public override Task Include_collection_with_outer_apply_with_filter(bool async)
        => AssertUnsupported(() => base.Include_collection_with_outer_apply_with_filter(async));

    public override Task Include_collection_with_outer_apply_with_filter_non_equality(bool async)
        => AssertUnsupported(() => base.Include_collection_with_outer_apply_with_filter_non_equality(async));

    public override Task Filtered_include_with_multiple_ordering(bool async)
        => AssertUnsupported(() => base.Filtered_include_with_multiple_ordering(async));

    // ClickHouse does not support correlated subqueries in ORDER BY
    public override Task Include_collection_order_by_collection_column(bool async)
        => AssertUnsupported(() => base.Include_collection_order_by_collection_column(async));

    public override Task Include_collection_order_by_subquery(bool async)
        => AssertUnsupported(() => base.Include_collection_order_by_subquery(async));

    public override Task Then_include_collection_order_by_collection_column(bool async)
        => AssertUnsupported(() => base.Then_include_collection_order_by_collection_column(async));

    // These tests apply Take to an ordering that has ties, or to no ordering. The expected rows
    // are the first rows in the order of the in-memory data. ClickHouse can return other rows
    // that are equally correct.
    [ConditionalTheory(Skip = "Take over an ordering with ties: ClickHouse can return other correct rows")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Include_collection_with_multiple_conditional_order_by(bool async)
        => base.Include_collection_with_multiple_conditional_order_by(async);

    [ConditionalTheory(Skip = "Take over an ordering with ties: ClickHouse can return other correct rows")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Repro9735(bool async)
        => base.Repro9735(async);

    [ConditionalTheory(Skip = "Take with no ordering: ClickHouse can return other correct rows")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Include_duplicate_collection_result_operator(bool async)
        => base.Include_duplicate_collection_result_operator(async);

    [ConditionalTheory(Skip = "Take with no ordering: ClickHouse can return other correct rows")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Include_duplicate_collection_result_operator2(bool async)
        => base.Include_duplicate_collection_result_operator2(async);

    private static async Task AssertUnsupported(Func<Task> test)
        => await Assert.ThrowsAsync<ClickHouse.Driver.ClickHouseServerException>(test);
}
