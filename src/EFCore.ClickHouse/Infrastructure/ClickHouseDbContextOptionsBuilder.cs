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
}
