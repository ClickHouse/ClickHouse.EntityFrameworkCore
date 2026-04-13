using System.Diagnostics;
using Xunit;

namespace EFCore.ClickHouse.Tests;

/// <summary>
/// End-to-end tests that shell out to the real dotnet-ef CLI tool
/// and verify the resulting database state against a real ClickHouse instance.
/// These tests skip automatically if dotnet-ef is not installed.
/// </summary>
public class DotnetEfCliTests : IAsyncLifetime
{
    private string _connectionString = default!;
    private string _smokeProjectDir = default!;
    private string? _migrationsDir;
    private bool _dotnetEfAvailable;

    public async Task InitializeAsync()
    {
        _dotnetEfAvailable = await IsDotnetEfInstalled();
        if (!_dotnetEfAvailable)
            return;

        _connectionString = await SharedContainer.GetConnectionStringAsync();

        var testDir = Path.GetDirectoryName(typeof(DotnetEfCliTests).Assembly.Location)!;
        var repoRoot = Path.GetFullPath(Path.Combine(testDir, "..", "..", "..", "..", ".."));
        _smokeProjectDir = Path.Combine(repoRoot, "test", "EFCore.ClickHouse.DesignSmoke");

        if (!Directory.Exists(_smokeProjectDir))
        {
            _dotnetEfAvailable = false;
            return;
        }

        _migrationsDir = Path.Combine(_smokeProjectDir, "Migrations");
        if (Directory.Exists(_migrationsDir))
            Directory.Delete(_migrationsDir, recursive: true);

        // Restore the DesignSmoke project (it's not in the solution file)
        await RunProcess("dotnet", ["restore", _smokeProjectDir]);
    }

    public Task DisposeAsync()
    {
        if (_migrationsDir is not null && Directory.Exists(_migrationsDir))
            Directory.Delete(_migrationsDir, recursive: true);
        return Task.CompletedTask;
    }

    [Fact]
    public async Task Database_update_creates_correct_schema()
    {
        if (!_dotnetEfAvailable)
            return; // dotnet-ef not installed — skip gracefully

        // Add migration and apply to real ClickHouse
        await RunDotnetEfSuccessfully("migrations", "add", "InitialCreate");
        await RunDotnetEfSuccessfully("database", "update");

        // Verify everything via ClickHouse system tables
        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(_connectionString);
        await connection.OpenAsync();

        // History table tracks the migration
        var historyCount = await QueryScalar<ulong>(connection,
            "SELECT count() FROM `__EFMigrationsHistory`");
        Assert.Equal(1UL, historyCount);

        // sensor_readings created with ReplacingMergeTree
        var sensorEngine = await QueryScalar<string>(connection,
            "SELECT engine FROM system.tables WHERE name = 'sensor_readings'");
        Assert.Equal("ReplacingMergeTree", sensorEngine);

        // audit_logs created with Memory
        var auditEngine = await QueryScalar<string>(connection,
            "SELECT engine FROM system.tables WHERE name = 'audit_logs'");
        Assert.Equal("Memory", auditEngine);

        // ORDER BY / sorting key
        var sortingKey = await QueryScalar<string>(connection,
            "SELECT sorting_key FROM system.tables WHERE name = 'sensor_readings'");
        Assert.Contains("Id", sortingKey);
        Assert.Contains("Timestamp", sortingKey);

        // PARTITION BY
        var partitionKey = await QueryScalar<string>(connection,
            "SELECT partition_key FROM system.tables WHERE name = 'sensor_readings'");
        Assert.Contains("toYYYYMM(Timestamp)", partitionKey);

        // PRIMARY KEY
        var primaryKey = await QueryScalar<string>(connection,
            "SELECT primary_key FROM system.tables WHERE name = 'sensor_readings'");
        Assert.Contains("Id", primaryKey);

        // Data skipping index exists
        var indexCount = await QueryScalar<ulong>(connection,
            "SELECT count() FROM system.data_skipping_indices WHERE table = 'sensor_readings'");
        Assert.True(indexCount > 0, "Expected at least one data skipping index");
    }

    [Fact]
    public async Task Idempotent_script_is_rejected()
    {
        if (!_dotnetEfAvailable)
            return; // dotnet-ef not installed — skip gracefully

        await RunDotnetEfSuccessfully("migrations", "add", "InitialCreate");

        var result = await RunDotnetEf("migrations", "script", "--idempotent");
        Assert.True(result.ExitCode != 0, "Expected --idempotent to fail");
        var output = result.StdOut + result.StdErr;
        Assert.Contains("does not support conditional SQL blocks", output);
    }

    private async Task RunDotnetEfSuccessfully(params string[] args)
    {
        var result = await RunDotnetEf(args);
        Assert.True(result.ExitCode == 0,
            $"dotnet-ef {string.Join(' ', args)} failed (exit {result.ExitCode}):\n{result.StdOut}\n{result.StdErr}");
    }

    private async Task<DotnetEfResult> RunDotnetEf(params string[] args)
    {
        var allArgs = new List<string>(args)
        {
            "--project", _smokeProjectDir,
            "--startup-project", _smokeProjectDir
        };

        var psi = new ProcessStartInfo
        {
            FileName = "dotnet-ef",
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            WorkingDirectory = _smokeProjectDir
        };
        psi.Environment["CLICKHOUSE_CONNECTION_STRING"] = _connectionString;

        foreach (var arg in allArgs)
            psi.ArgumentList.Add(arg);

        using var process = Process.Start(psi)!;
        var stdout = await process.StandardOutput.ReadToEndAsync();
        var stderr = await process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();

        return new DotnetEfResult(process.ExitCode, stdout, stderr);
    }

    private static async Task<bool> IsDotnetEfInstalled()
    {
        try
        {
            var psi = new ProcessStartInfo("dotnet-ef", "--version")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false
            };
            using var process = Process.Start(psi)!;
            await process.WaitForExitAsync();
            return process.ExitCode == 0;
        }
        catch
        {
            return false;
        }
    }

    private static async Task RunProcess(string fileName, string[] args)
    {
        var psi = new ProcessStartInfo(fileName)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false
        };
        foreach (var arg in args)
            psi.ArgumentList.Add(arg);

        using var process = Process.Start(psi)!;
        await process.WaitForExitAsync();
    }

    private static async Task<T> QueryScalar<T>(
        global::ClickHouse.Driver.ADO.ClickHouseConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        var result = await cmd.ExecuteScalarAsync();
        return (T)Convert.ChangeType(result!, typeof(T));
    }

    private record DotnetEfResult(int ExitCode, string StdOut, string StdErr);
}
