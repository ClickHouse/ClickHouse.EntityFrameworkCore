using System.Data.Common;
using ClickHouse.Driver;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal;

public interface IClickHouseRelationalConnection : IRelationalConnection
{
    IClickHouseRelationalConnection CreateMasterConnection();
    void ApplyQuerySettings(DbCommand command);
    InsertOptions? CreateInsertOptions(IClickHouseClient client);
    IClickHouseClient GetClickHouseClient();
}
