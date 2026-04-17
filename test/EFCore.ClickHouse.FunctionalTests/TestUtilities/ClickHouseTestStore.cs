using System.Collections.Concurrent;
using ClickHouse.Driver.ADO;

namespace Microsoft.EntityFrameworkCore.TestUtilities;

public class ClickHouseTestStore : RelationalTestStore
{
    private static readonly ConcurrentDictionary<string, object> InitLocks = new();
    private static readonly ConcurrentDictionary<string, byte> SharedInitialized = new();

    private readonly string? _scriptPath;
    private readonly string? _additionalSql;
    private readonly bool _shared;

    public ClickHouseTestStore(
        string name,
        string? scriptPath = null,
        string? additionalSql = null,
        bool shared = true)
        : base(name, shared, CreateConnection(name))
    {
        _shared = shared;
        _additionalSql = additionalSql;

        UseConnectionString = true;
        _scriptPath = scriptPath is null
            ? null
            : Path.Combine(Path.GetDirectoryName(typeof(ClickHouseTestStore).Assembly.Location)!, scriptPath);

        EnsureInitialized();
    }

    public static ClickHouseTestStore GetOrCreate(
        string name,
        string? scriptPath = null,
        string? additionalSql = null)
        => new(name, scriptPath, additionalSql, shared: true);

    public static ClickHouseTestStore Create(
        string name,
        string? scriptPath = null,
        string? additionalSql = null)
        => new(name, scriptPath, additionalSql, shared: false);

    public static string CreateConnectionString(string databaseName)
        => new ClickHouseConnectionStringBuilder(TestEnvironment.DefaultConnection)
        {
            Database = databaseName
        }.ToString();

    public override DbContextOptionsBuilder AddProviderOptions(DbContextOptionsBuilder builder)
        => UseConnectionString
            ? builder.UseClickHouse(ConnectionString)
            : builder.UseClickHouse(Connection);

    public int ExecuteNonQuery(string sql)
    {
        using var command = Connection.CreateCommand();
        command.CommandText = sql;
        return command.ExecuteNonQuery();
    }

    private static ClickHouseConnection CreateConnection(string databaseName)
        => new(CreateConnectionString(databaseName));

    protected override Task InitializeAsync(
        Func<DbContext> createContext,
        Func<DbContext, Task>? seed,
        Func<DbContext, Task>? clean)
    {
        // Script-based stores are initialized eagerly in the constructor and must not go through
        // EF seeding, which would invoke SaveChanges (not supported yet by the provider).
        if (_scriptPath is not null)
        {
            return Task.CompletedTask;
        }

        return InitializeModelBasedStoreAsync(createContext, seed, clean);
    }

    private void EnsureInitialized()
    {
        var initKey = $"{Name}:{_scriptPath}:{_additionalSql}";
        if (_shared && SharedInitialized.ContainsKey(initKey))
            return;

        var gate = InitLocks.GetOrAdd(initKey, _ => new object());

        lock (gate)
        {
            if (_shared && SharedInitialized.ContainsKey(initKey))
                return;

            RecreateDatabase();

            if (_scriptPath is not null)
                ExecuteScript(_scriptPath);

            if (!string.IsNullOrWhiteSpace(_additionalSql))
                ExecuteScriptText(_additionalSql!);

            if (_shared)
                SharedInitialized.TryAdd(initKey, 0);
        }
    }

    private async Task InitializeModelBasedStoreAsync(
        Func<DbContext> createContext,
        Func<DbContext, Task>? seed,
        Func<DbContext, Task>? clean)
    {
        await using var context = createContext();

        if (clean is not null)
        {
            await clean(context);
        }

        await context.Database.EnsureCreatedResilientlyAsync();

        if (seed is not null)
        {
            await seed(context);
        }
    }

    private void RecreateDatabase()
    {
        var databaseName = Name;

        using var admin = new ClickHouseConnection(
            new ClickHouseConnectionStringBuilder(TestEnvironment.DefaultConnection) { Database = "default" }.ToString());
        admin.Open();

        using var drop = admin.CreateCommand();
        drop.CommandText = $"DROP DATABASE IF EXISTS `{databaseName}`";
        drop.ExecuteNonQuery();

        using var create = admin.CreateCommand();
        create.CommandText = $"CREATE DATABASE `{databaseName}`";
        create.ExecuteNonQuery();
    }

    private void ExecuteScript(string scriptPath)
        => ExecuteScriptText(File.ReadAllText(scriptPath));

    private void ExecuteScriptText(string script)
    {
        Connection.Open();
        try
        {
            foreach (var statement in SplitStatements(script))
            {
                using var command = Connection.CreateCommand();
                command.CommandText = statement;
                command.ExecuteNonQuery();
            }
        }
        finally
        {
            Connection.Close();
        }
    }

    private static IEnumerable<string> SplitStatements(string script)
    {
        var current = new System.Text.StringBuilder();

        foreach (var line in script.Split('\n'))
        {
            var trimmed = line.Trim();
            if (trimmed.Equals("GO", StringComparison.OrdinalIgnoreCase))
            {
                var goStatement = current.ToString().Trim();
                if (goStatement.Length > 0)
                    yield return goStatement;
                current.Clear();
                continue;
            }

            current.AppendLine(line);

            if (!trimmed.EndsWith(';'))
                continue;

            var statement = current.ToString().Trim();
            if (statement.Length > 0)
                yield return statement[..^1].TrimEnd();
            current.Clear();
        }

        var rest = current.ToString().Trim();
        if (rest.Length > 0)
            yield return rest;
    }
}
