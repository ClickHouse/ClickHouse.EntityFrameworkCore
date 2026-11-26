using System.Data.Common;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal;

/// <summary>
/// A relational command that adds the provider query settings to each driver command it creates.
/// <see cref="ClickHouseCommandSettingsInterceptor"/> adds them before the interceptors of the
/// application run. This class also covers a command that EF Core creates without interceptors.
/// </summary>
public class ClickHouseRelationalCommand : RelationalCommand
{
    public ClickHouseRelationalCommand(
        RelationalCommandBuilderDependencies dependencies,
        string commandText,
        string logCommandText,
        IReadOnlyList<IRelationalParameter> parameters)
        : base(dependencies, commandText, logCommandText, parameters)
    {
    }

    public override DbCommand CreateDbCommand(
        RelationalCommandParameterObject parameterObject,
        Guid commandId,
        DbCommandMethod commandMethod)
    {
        var command = base.CreateDbCommand(parameterObject, commandId, commandMethod);

        if (parameterObject.Connection is IClickHouseRelationalConnection connection)
            connection.ApplyQuerySettings(command);

        return command;
    }
}
