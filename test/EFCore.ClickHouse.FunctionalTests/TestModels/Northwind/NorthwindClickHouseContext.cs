using Microsoft.EntityFrameworkCore;

namespace Microsoft.EntityFrameworkCore.TestModels.Northwind;

public class NorthwindClickHouseContext : NorthwindRelationalContext
{
    public NorthwindClickHouseContext(DbContextOptions options)
        : base(options)
    {
    }
}
