using Microsoft.EntityFrameworkCore.Infrastructure;

namespace ClickHouse.EntityFrameworkCore.Infrastructure.Internal;

public class ClickHouseSingletonOptions : IClickHouseSingletonOptions
{
    public void Initialize(IDbContextOptions options)
    {
    }

    public void Validate(IDbContextOptions options)
    {
    }
}
