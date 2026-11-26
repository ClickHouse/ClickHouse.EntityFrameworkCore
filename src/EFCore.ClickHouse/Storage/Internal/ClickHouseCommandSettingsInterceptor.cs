using System.Data.Common;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal;

/// <summary>
/// Adds the provider query settings to a driver command when EF Core creates it. EF Core runs the
/// interceptors that the provider registers before the interceptors of the application. The
/// settings are therefore on the driver command before an application interceptor can replace the
/// command with a wrapper, for example for profiling.
/// </summary>
public class ClickHouseCommandSettingsInterceptor : DbCommandInterceptor
{
    public override DbCommand CommandCreated(CommandEndEventData eventData, DbCommand result)
    {
        // The connection is resolved for each command: a constructor dependency on it would make a
        // cycle, because the connection logger depends on the interceptors.
        eventData.Context?.GetService<IClickHouseRelationalConnection>().ApplyQuerySettings(result);
        return result;
    }
}
