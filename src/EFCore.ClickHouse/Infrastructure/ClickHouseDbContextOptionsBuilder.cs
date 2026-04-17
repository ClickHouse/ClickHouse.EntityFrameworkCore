using ClickHouse.EntityFrameworkCore.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace ClickHouse.EntityFrameworkCore.Infrastructure;

public class ClickHouseDbContextOptionsBuilder
    : RelationalDbContextOptionsBuilder<ClickHouseDbContextOptionsBuilder, ClickHouseOptionsExtension>
{
    public ClickHouseDbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder)
        : base(optionsBuilder)
    {
    }

    /// <summary>
    /// Disables automatic injection of <c>set_join_use_nulls=1</c> into connection strings.
    /// Use this when the ClickHouse server or user profile forbids changing that setting
    /// (e.g. <c>readonly=1</c> profiles). With auto-injection disabled, LEFT JOINs return
    /// column defaults (0, "") for non-matching rows rather than NULL, and EF Core's
    /// null-based navigation detection will not work as expected.
    /// </summary>
    public virtual ClickHouseDbContextOptionsBuilder DisableJoinNullSemantics()
        => WithOption(e => e.WithJoinNullSemanticsDisabled(true));
}
