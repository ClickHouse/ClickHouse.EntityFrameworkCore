using System.Diagnostics;
using Xunit;

namespace EFCore.ClickHouse.Tests;

/// <summary>
/// End-to-end tests that shell out to the real dotnet-ef CLI tool and verify the resulting database
/// state against a real ClickHouse instance — the only path that exercises the full design-time
/// pipeline (scaffolder → splitter → C# code-gen → compile → migrator). These tests skip (loudly)
/// if dotnet-ef is not installed.
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
    public async Task Migrations_add_splits_into_ordered_forward_only_steps()
    {
        if (!_dotnetEfAvailable)
            return; // dotnet-ef not installed — skip gracefully (CI installs it, so coverage is real there)

        await RunDotnetEfSuccessfully("migrations", "add", "InitialCreate");

        var stepFiles = StepMigrationFiles();
        Assert.True(stepFiles.Length > 1,
            $"Expected the migration to split into multiple step files; found {stepFiles.Length}.");

        // Steps are suffixed _001, _002, … and sort in dependency order.
        var suffixes = stepFiles.Select(f => Path.GetFileNameWithoutExtension(f)[^3..]).ToList();
        Assert.Equal(suffixes.OrderBy(s => s, StringComparer.Ordinal).ToList(), suffixes);

        // Every step is forward-only: its Down throws rather than attempting a rollback.
        foreach (var file in stepFiles)
            Assert.Contains("throw new ClickHouseDownMigrationNotSupportedException", await File.ReadAllTextAsync(file));

        // Exactly one shared model snapshot is emitted for the whole set.
        Assert.Single(Directory.GetFiles(_migrationsDir!, "*ModelSnapshot.cs"));
    }

    [Fact]
    public async Task Database_update_creates_correct_schema()
    {
        if (!_dotnetEfAvailable)
            return; // dotnet-ef not installed — skip gracefully (CI installs it, so coverage is real there)

        // Add migration and apply to real ClickHouse
        await RunDotnetEfSuccessfully("migrations", "add", "InitialCreate");
        await RunDotnetEfSuccessfully("database", "update");

        // Verify everything via ClickHouse system tables
        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(_connectionString);
        await connection.OpenAsync();

        // The split produces one history row per step, so a partial failure stays resumable.
        var historyCount = await QueryScalar<ulong>(connection, "SELECT count() FROM `__EFMigrationsHistory`");
        Assert.Equal((ulong)StepMigrationFiles().Length, historyCount);
        Assert.True(historyCount > 1, "Expected the split migration to record multiple history rows.");

        // sensor_readings created with ReplacingMergeTree
        var sensorEngine = await QueryScalar<string>(connection,
            "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'sensor_readings'");
        Assert.Equal("ReplacingMergeTree", sensorEngine);

        // audit_logs created with Memory
        var auditEngine = await QueryScalar<string>(connection,
            "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'audit_logs'");
        Assert.Equal("Memory", auditEngine);

        // ORDER BY / sorting key
        var sortingKey = await QueryScalar<string>(connection,
            "SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 'sensor_readings'");
        Assert.Contains("Id", sortingKey);
        Assert.Contains("Timestamp", sortingKey);

        // PARTITION BY
        var partitionKey = await QueryScalar<string>(connection,
            "SELECT partition_key FROM system.tables WHERE database = currentDatabase() AND name = 'sensor_readings'");
        Assert.Contains("toYYYYMM(Timestamp)", partitionKey);

        // PRIMARY KEY
        var primaryKey = await QueryScalar<string>(connection,
            "SELECT primary_key FROM system.tables WHERE database = currentDatabase() AND name = 'sensor_readings'");
        Assert.Contains("Id", primaryKey);

        // Data skipping index exists
        var indexCount = await QueryScalar<ulong>(connection,
            "SELECT count() FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'sensor_readings'");
        Assert.True(indexCount > 0, "Expected at least one data skipping index");

        // The materialized view was created (and only because its source/target tables were ordered first).
        var mvEngine = await QueryScalar<string>(connection,
            "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'hits_mv'");
        Assert.Equal("MaterializedView", mvEngine);

        // The dictionary was created (ordered after its source table).
        var dictCount = await QueryScalar<ulong>(connection,
            "SELECT count() FROM system.dictionaries WHERE database = currentDatabase() AND name = 'hits_dict'");
        Assert.Equal(1UL, dictCount);

        // The projection was added to hits_source (ordered after the table create).
        var projectionCount = await QueryScalar<ulong>(connection,
            "SELECT count() FROM system.projections WHERE database = currentDatabase() AND table = 'hits_source' AND name = 'proj_by_value'");
        Assert.Equal(1UL, projectionCount);
    }

    [Fact]
    public async Task Projection_definition_is_baked_into_the_model_snapshot()
    {
        if (!_dotnetEfAvailable)
            return; // dotnet-ef not installed — skip gracefully (CI installs it, so coverage is real there)

        await RunDotnetEfSuccessfully("migrations", "add", "InitialCreate");

        var snapshot = Directory.GetFiles(_migrationsDir!, "*ModelSnapshot.cs").Single();
        var snapshotCode = await File.ReadAllTextAsync(snapshot);

        // The FromRaw projection's SELECT is serialized into the snapshot as a plain annotation, so the
        // projection round-trips on subsequent diffs. The transient LINQ lambda annotation is dropped by
        // ClickHouseAnnotationCodeGenerator, so scaffolding never crashes trying to emit a delegate.
        Assert.Contains("ClickHouse:Projection:proj_by_value:SelectSql", snapshotCode);
        Assert.Contains("SELECT Value, count() GROUP BY Value", snapshotCode);
        Assert.DoesNotContain("PendingLambda", snapshotCode);
    }

    [Fact]
    public async Task Generated_script_runs_as_one_multi_statement_batch()
    {
        if (!_dotnetEfAvailable)
            return; // dotnet-ef not installed — skip gracefully (CI installs it, so coverage is real there)

        await RunDotnetEfSuccessfully("migrations", "add", "InitialCreate");

        var scriptPath = Path.Combine(_smokeProjectDir, "migrate.sql");
        try
        {
            await RunDotnetEfSuccessfully("migrations", "script", "--output", scriptPath);
            var script = await File.ReadAllTextAsync(scriptPath);

            // The semicolon-termination fix: every DDL statement is terminated, so the statements don't
            // run together when concatenated.
            Assert.Contains(";", script);
            Assert.Contains("CREATE TABLE", script, StringComparison.OrdinalIgnoreCase);

            var csb = new global::ClickHouse.Driver.ADO.ClickHouseConnectionStringBuilder(_connectionString);

            // Run the WHOLE script as a single batch via clickhouse-client --multiquery. If any statement
            // were missing its terminator, adjacent statements would fuse and the batch would fail.
            var (exitCode, stdout, stderr) = await SharedContainer.ExecAsync(
                "clickhouse-client",
                "--password", csb.Password,
                "--database", csb.Database,
                "--multiquery",
                "--query", script);

            Assert.True(exitCode == 0, $"Multi-statement script failed (exit {exitCode}):\n{stdout}\n{stderr}");

            // Confirm the batch actually built the schema.
            using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(_connectionString);
            await connection.OpenAsync();
            var sensorCount = await QueryScalar<ulong>(connection,
                "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'sensor_readings'");
            Assert.Equal(1UL, sensorCount);
        }
        finally
        {
            if (File.Exists(scriptPath))
                File.Delete(scriptPath);
        }
    }

    [Fact]
    public async Task Idempotent_script_is_rejected()
    {
        if (!_dotnetEfAvailable)
            return; // dotnet-ef not installed — skip gracefully (CI installs it, so coverage is real there)

        await RunDotnetEfSuccessfully("migrations", "add", "InitialCreate");

        var result = await RunDotnetEf("migrations", "script", "--idempotent");
        Assert.True(result.ExitCode != 0, "Expected --idempotent to fail");
        var output = result.StdOut + result.StdErr;
        Assert.Contains("does not support conditional SQL blocks", output);
    }

    // Step migrations are the "*_NNN.cs" files (excluding their ".Designer.cs" companions and the snapshot).
    private string[] StepMigrationFiles()
        => Directory.GetFiles(_migrationsDir!, "*.cs")
            .Where(f => !f.EndsWith(".Designer.cs", StringComparison.Ordinal)
                     && !f.EndsWith("ModelSnapshot.cs", StringComparison.Ordinal))
            .OrderBy(f => f, StringComparer.Ordinal)
            .ToArray();

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
