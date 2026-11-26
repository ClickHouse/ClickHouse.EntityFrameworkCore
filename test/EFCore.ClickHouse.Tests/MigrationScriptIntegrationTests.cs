using System.Text.RegularExpressions;
using ClickHouse.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Xunit;

namespace EFCore.ClickHouse.Tests;

/// <summary>
/// Generates the script of a set of migrations, as <c>dotnet ef migrations script</c> does, and runs it
/// against ClickHouse. A client such as <c>clickhouse-client</c> splits a script at the statement
/// terminators, so a statement without its terminator merges with the next one and fails.
/// </summary>
public class MigrationScriptIntegrationTests : IAsyncLifetime
{
    private string _connectionString = default!;
    private string _databaseName = default!;

    public async Task InitializeAsync()
    {
        _connectionString = await SharedContainer.GetConnectionStringAsync();
        _databaseName = Regex.Match(_connectionString, @"Database=([^;]+)").Groups[1].Value;
    }

    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task Generated_script_runs_statement_by_statement()
    {
        string script;
        await using (var ctx = new ScriptContext(Options()))
            script = ctx.GetService<IMigrator>().GenerateScript();

        var statements = Regex.Split(script, @";[ \t]*\r?\n")
            .Select(s => s.Trim())
            .Where(s => s.Length > 0)
            .ToList();

        // One statement for each command: the history table, 13 DDL statements and 3 history rows.
        Assert.Equal(17, statements.Count);
        Assert.All(statements, s => Assert.DoesNotContain(";", s));

        foreach (var statement in statements)
            await ExecuteAsync(statement);

        Assert.Equal("Id,Title,Score", await QueryAsync(
            $"SELECT arrayStringConcat(groupArray(name), ',') FROM (SELECT name FROM system.columns " +
            $"WHERE database = '{_databaseName}' AND table = 'script_events_renamed' ORDER BY position)"));
        Assert.Equal("Int64", await QueryAsync(
            $"SELECT type FROM system.columns WHERE database = '{_databaseName}' " +
            "AND table = 'script_events_renamed' AND name = 'Score'"));
        Assert.Equal("", await QueryAsync(
            $"SELECT compression_codec FROM system.columns WHERE database = '{_databaseName}' " +
            "AND table = 'script_events_renamed' AND name = 'Score'"));
        Assert.Equal("0", await QueryAsync(
            $"SELECT count() FROM system.data_skipping_indices WHERE database = '{_databaseName}' " +
            "AND table = 'script_events_renamed'"));
        Assert.Equal("3", await QueryAsync("SELECT count() FROM `__EFMigrationsHistory`"));
        Assert.Equal("0", await QueryAsync("EXISTS DATABASE `efcore_script_scratch`"));
    }

    private DbContextOptions Options()
        => new DbContextOptionsBuilder()
            .UseClickHouse(_connectionString)
            // The test migrations have no model snapshot.
            .ConfigureWarnings(w => w.Ignore(RelationalEventId.PendingModelChangesWarning))
            .Options;

    private async Task ExecuteAsync(string sql)
    {
        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(_connectionString);
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    private async Task<string?> QueryAsync(string sql)
    {
        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(_connectionString);
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        return (await command.ExecuteScalarAsync())?.ToString();
    }

    public sealed class ScriptContext(DbContextOptions options) : DbContext(options);

    // The migrations below use each DDL statement that the SQL generator overrides.

    [DbContext(typeof(ScriptContext))]
    [Migration("20261125100001_CreateTable")]
    public class CreateTableMigration : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "script_events",
                columns: table => new
                {
                    Id = table.Column<long>(type: "Int64", nullable: false),
                    Name = table.Column<string>(type: "String", nullable: false),
                    Score = table.Column<int>(type: "Int32", nullable: false)
                        .Annotation("ClickHouse:ColumnCodec", "ZSTD"),
                },
                constraints: table => table.PrimaryKey("PK_script_events", x => x.Id))
                .Annotation("ClickHouse:Engine", "MergeTree")
                .Annotation("ClickHouse:OrderBy", new[] { "Id" });
        }
    }

    [DbContext(typeof(ScriptContext))]
    [Migration("20261125100002_ChangeColumns")]
    public class ChangeColumnsMigration : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(name: "Tag", table: "script_events", type: "String", nullable: false, defaultValue: "");
            migrationBuilder.AlterColumn<long>(name: "Score", table: "script_events", type: "Int64", nullable: false,
                    oldClrType: typeof(int), oldType: "Int32")
                .OldAnnotation("ClickHouse:ColumnCodec", "ZSTD");
            migrationBuilder.RenameColumn(name: "Name", table: "script_events", newName: "Title");
            migrationBuilder.CreateIndex(name: "ix_score", table: "script_events", column: "Score")
                .Annotation("ClickHouse:SkippingIndex:Type", "minmax");
            migrationBuilder.AddCheckConstraint(name: "ck_id", table: "script_events", sql: "`Id` > 0");
        }
    }

    [DbContext(typeof(ScriptContext))]
    [Migration("20261125100003_DropAndRename")]
    public class DropAndRenameMigration : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropCheckConstraint(name: "ck_id", table: "script_events");
            migrationBuilder.DropIndex(name: "ix_score", table: "script_events")
                .Annotation("ClickHouse:SkippingIndex:Type", "minmax");
            migrationBuilder.DropColumn(name: "Tag", table: "script_events");
            migrationBuilder.RenameTable(name: "script_events", newName: "script_events_renamed");
            migrationBuilder.Operations.Add(new ClickHouseCreateDatabaseOperation { Name = "efcore_script_scratch" });
            migrationBuilder.Operations.Add(new ClickHouseDropDatabaseOperation { Name = "efcore_script_scratch" });
        }
    }
}
