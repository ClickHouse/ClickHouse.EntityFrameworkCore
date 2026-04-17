using Microsoft.EntityFrameworkCore.TestUtilities;

namespace Microsoft.EntityFrameworkCore.Query;

public class NorthwindJoinQueryClickHouseTest
    : NorthwindJoinQueryRelationalTestBase<NorthwindQueryClickHouseFixture<NoopModelCustomizer>>
{
    public NorthwindJoinQueryClickHouseTest(NorthwindQueryClickHouseFixture<NoopModelCustomizer> fixture)
        : base(fixture)
    {
    }

    // ClickHouse does not support CROSS APPLY / OUTER APPLY syntax
    public override Task SelectMany_with_client_eval(bool async)
        => AssertUnsupported(() => base.SelectMany_with_client_eval(async));

    public override Task SelectMany_with_client_eval_with_collection_shaper(bool async)
        => AssertUnsupported(() => base.SelectMany_with_client_eval_with_collection_shaper(async));

    public override Task SelectMany_with_client_eval_with_collection_shaper_ignored(bool async)
        => AssertUnsupported(() => base.SelectMany_with_client_eval_with_collection_shaper_ignored(async));

    public override Task SelectMany_with_selecting_outer_element(bool async)
        => AssertUnsupported(() => base.SelectMany_with_selecting_outer_element(async));

    public override Task SelectMany_with_selecting_outer_entity(bool async)
        => AssertUnsupported(() => base.SelectMany_with_selecting_outer_entity(async));

    public override Task SelectMany_with_selecting_outer_entity_column_and_inner_column(bool async)
        => AssertUnsupported(() => base.SelectMany_with_selecting_outer_entity_column_and_inner_column(async));

    public override Task Take_in_collection_projection_with_FirstOrDefault_on_top_level(bool async)
        => AssertUnsupported(() => base.Take_in_collection_projection_with_FirstOrDefault_on_top_level(async));

    // ClickHouse cannot resolve identifiers from outer scope in APPLY-style subqueries
    public override Task SelectMany_with_client_eval_with_constructor(bool async)
        => AssertUnsupported(() => base.SelectMany_with_client_eval_with_constructor(async));

    // Complex LINQ pattern not translatable
    public override Task GroupJoin_aggregate_anonymous_key_selectors2(bool async)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => base.GroupJoin_aggregate_anonymous_key_selectors2(async));

    private static async Task AssertUnsupported(Func<Task> test)
        => await Assert.ThrowsAsync<ClickHouse.Driver.ClickHouseServerException>(test);
}
