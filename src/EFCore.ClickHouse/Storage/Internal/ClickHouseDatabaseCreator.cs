using System.Data.Common;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Parameters;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal;

public class ClickHouseDatabaseCreator : RelationalDatabaseCreator
{
    private readonly IClickHouseRelationalConnection _connection;
    private readonly IRawSqlCommandBuilder _rawSqlCommandBuilder;
    private readonly ISqlGenerationHelper _sqlGenerationHelper;

    public ClickHouseDatabaseCreator(
        RelationalDatabaseCreatorDependencies dependencies,
        IClickHouseRelationalConnection connection,
        IRawSqlCommandBuilder rawSqlCommandBuilder,
        ISqlGenerationHelper sqlGenerationHelper)
        : base(dependencies)
    {
        _connection = connection;
        _rawSqlCommandBuilder = rawSqlCommandBuilder;
        _sqlGenerationHelper = sqlGenerationHelper;
    }

    private string GetDatabaseName()
        => new ClickHouseConnectionStringBuilder(_connection.ConnectionString).Database;

    public override bool Exists()
    {
        try
        {
            _connection.Open();
            return DatabaseExists(_connection.DbConnection, GetDatabaseName());
        }
        catch
        {
            return false;
        }
        finally
        {
            _connection.Close();
        }
    }

    public override async Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await _connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            return await DatabaseExistsAsync(_connection.DbConnection, GetDatabaseName(), cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            return false;
        }
        finally
        {
            await _connection.CloseAsync().ConfigureAwait(false);
        }
    }

    public override bool HasTables()
    {
        _connection.Open();
        try
        {
            return HasTablesCore(_connection.DbConnection, GetDatabaseName());
        }
        finally
        {
            _connection.Close();
        }
    }

    public override async Task<bool> HasTablesAsync(CancellationToken cancellationToken = default)
    {
        await _connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await HasTablesCoreAsync(_connection.DbConnection, GetDatabaseName(), cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            await _connection.CloseAsync().ConfigureAwait(false);
        }
    }

    public override void Create()
    {
        using var masterConnection = _connection.CreateMasterConnection();
        masterConnection.Open();
        try
        {
            using var command = masterConnection.DbConnection.CreateCommand();
            command.CommandText = $"CREATE DATABASE IF NOT EXISTS {_sqlGenerationHelper.DelimitIdentifier(GetDatabaseName())}";
            command.ExecuteNonQuery();
        }
        finally
        {
            masterConnection.Close();
        }
    }

    public override async Task CreateAsync(CancellationToken cancellationToken = default)
    {
        var masterConnection = _connection.CreateMasterConnection();
        await using (masterConnection.ConfigureAwait(false))
        {
            await masterConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var command = masterConnection.DbConnection.CreateCommand();
                await using (command.ConfigureAwait(false))
                {
                    command.CommandText = $"CREATE DATABASE IF NOT EXISTS {_sqlGenerationHelper.DelimitIdentifier(GetDatabaseName())}";
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
            }
            finally
            {
                await masterConnection.CloseAsync().ConfigureAwait(false);
            }
        }
    }

    public override void Delete()
    {
        using var masterConnection = _connection.CreateMasterConnection();
        masterConnection.Open();
        try
        {
            using var command = masterConnection.DbConnection.CreateCommand();
            command.CommandText = $"DROP DATABASE IF EXISTS {_sqlGenerationHelper.DelimitIdentifier(GetDatabaseName())}";
            command.ExecuteNonQuery();
        }
        finally
        {
            masterConnection.Close();
        }
    }

    public override async Task DeleteAsync(CancellationToken cancellationToken = default)
    {
        var masterConnection = _connection.CreateMasterConnection();
        await using (masterConnection.ConfigureAwait(false))
        {
            await masterConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var command = masterConnection.DbConnection.CreateCommand();
                await using (command.ConfigureAwait(false))
                {
                    command.CommandText = $"DROP DATABASE IF EXISTS {_sqlGenerationHelper.DelimitIdentifier(GetDatabaseName())}";
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
            }
            finally
            {
                await masterConnection.CloseAsync().ConfigureAwait(false);
            }
        }
    }

    private bool DatabaseExists(DbConnection connection, string databaseName)
    {
        using var command = connection.CreateCommand();
        command.CommandText = $"EXISTS DATABASE {_sqlGenerationHelper.DelimitIdentifier(databaseName)}";
        return Convert.ToBoolean(command.ExecuteScalar());
    }

    private async Task<bool> DatabaseExistsAsync(
        DbConnection connection, string databaseName, CancellationToken cancellationToken)
    {
        var command = connection.CreateCommand();
        await using (command.ConfigureAwait(false))
        {
            command.CommandText = $"EXISTS DATABASE {_sqlGenerationHelper.DelimitIdentifier(databaseName)}";
            return Convert.ToBoolean(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));
        }
    }

    private static bool HasTablesCore(DbConnection connection, string databaseName)
    {
        using var command = connection.CreateCommand();
        var param = command.CreateParameter();
        param.ParameterName = "db";
        param.Value = databaseName;
        command.Parameters.Add(param);
        command.CommandText = $"SELECT count() > 0 FROM system.tables WHERE database = {((ClickHouseDbParameter)param).QueryForm}";
        return Convert.ToBoolean(command.ExecuteScalar());
    }

    private static async Task<bool> HasTablesCoreAsync(
        DbConnection connection, string databaseName, CancellationToken cancellationToken)
    {
        var command = connection.CreateCommand();
        await using (command.ConfigureAwait(false))
        {
            var param = command.CreateParameter();
            param.ParameterName = "db";
            param.Value = databaseName;
            command.Parameters.Add(param);
            command.CommandText = $"SELECT count() > 0 FROM system.tables WHERE database = {((ClickHouseDbParameter)param).QueryForm}";
            return Convert.ToBoolean(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));
        }
    }
}
