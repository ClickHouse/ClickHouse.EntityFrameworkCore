using System.Text;
using ClickHouse.EntityFrameworkCore.Extensions;
using ClickHouse.EntityFrameworkCore.Metadata;
using ClickHouse.EntityFrameworkCore.Metadata.Builders;
using ClickHouse.EntityFrameworkCore.Migrations.Design;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Testcontainers.ClickHouse;
using Xunit;

namespace EFCore.ClickHouse.Tests;

/// <summary>
/// Functional coverage for the three ways a dictionary's <c>SOURCE(CLICKHOUSE(...))</c> can authenticate
/// when loading from its source table, each against a real server where the relevant auth applies:
/// <list type="number">
/// <item>a named collection (credentials live in server config, not the DDL) — <see cref="NamedCollectionClickHouseFixture"/>;</item>
/// <item>passwordless (no credentials emitted; loads as the <c>default</c> user) — <see cref="PasswordlessClickHouseFixture"/>;</item>
/// <item>username/password written inline into the DDL — <see cref="NamedCollectionClickHouseFixture"/> (a password-protected server).</item>
/// </list>
/// Each test seeds the source, creates the dictionary through the provider pipeline, then proves the
/// dictionary actually <em>loads</em> by resolving values with <c>dictGet</c>.
/// </summary>
public class DictionaryCredentialIntegrationTests
{
    // ---- Mode 2: passwordless (no credentials in the source) -------------------------------------

    public sealed class Passwordless(PasswordlessClickHouseFixture fixture)
        : IClassFixture<PasswordlessClickHouseFixture>
    {
        [Fact]
        public async Task No_credentials_loads_against_passwordless_server()
        {
            var conn = fixture.ConnectionString;
            await using var ctx = CreateContext(conn, dict => { /* no WithSourceConnection — passwordless */ });

            var ddl = Generate(Diff(null, ctx), ctx);
            Assert.DoesNotContain("PASSWORD", ddl);
            Assert.DoesNotContain("NAME ", ddl);

            await ApplyAsync(conn, Diff(null, ctx), ctx);
            await SeedAsync(conn);

            Assert.Equal("USD", await DictGetNameAsync(conn, 1));
            Assert.Equal("EUR", await DictGetNameAsync(conn, 2));
        }
    }

    // ---- Modes 1 & 3: named collection and inline password (password-protected server) -----------

    public sealed class WithPasswordProtectedServer(NamedCollectionClickHouseFixture fixture)
        : IClassFixture<NamedCollectionClickHouseFixture>
    {
        [Fact]
        public async Task Inline_username_password_in_ddl_loads()
        {
            var conn = fixture.ConnectionString;
            await using var ctx = CreateContext(conn,
                dict => dict.WithSourceConnection(user: "default", password: NamedCollectionClickHouseFixture.Password),
                tableSuffix: "inline");

            var ddl = Generate(Diff(null, ctx), ctx);
            Assert.Contains("USER 'default'", ddl);
            Assert.Contains($"PASSWORD '{NamedCollectionClickHouseFixture.Password}'", ddl);

            await ApplyAsync(conn, Diff(null, ctx), ctx);
            await SeedAsync(conn, "currencies_inline");

            Assert.Equal("USD", await DictGetNameAsync(conn, 1, "currency_dict_inline"));
            Assert.Equal("EUR", await DictGetNameAsync(conn, 2, "currency_dict_inline"));
        }

        [Fact]
        public async Task Named_collection_loads_without_credentials_in_ddl()
        {
            var conn = fixture.ConnectionString;
            await using var ctx = CreateContext(conn,
                dict => dict.FromNamedCollection(NamedCollectionClickHouseFixture.CollectionName),
                tableSuffix: "nc");

            var ddl = Generate(Diff(null, ctx), ctx);
            Assert.Contains($"NAME '{NamedCollectionClickHouseFixture.CollectionName}'", ddl);
            Assert.DoesNotContain("PASSWORD", ddl); // the secret stays in server config, not the migration

            await ApplyAsync(conn, Diff(null, ctx), ctx);
            await SeedAsync(conn, "currencies_nc");

            Assert.Equal("USD", await DictGetNameAsync(conn, 1, "currency_dict_nc"));
            Assert.Equal("EUR", await DictGetNameAsync(conn, 2, "currency_dict_nc"));
        }

        [Fact]
        public async Task Composite_key_dictionary_loads()
        {
            var conn = fixture.ConnectionString;
            var options = new DbContextOptionsBuilder()
                .UseClickHouse(conn)
                .EnableServiceProviderCaching(false)
                .Options;
            await using var ctx = new CompositeKeyContext(options);

            var ddl = Generate(Diff(null, ctx), ctx);
            Assert.Contains("COMPLEX_KEY_HASHED", ddl);

            await ApplyAsync(conn, Diff(null, ctx), ctx);
            await using (var seed = new global::ClickHouse.Driver.ADO.ClickHouseConnection(conn))
            {
                await seed.OpenAsync();
                using var cmd = seed.CreateCommand();
                cmd.CommandText = "INSERT INTO fx_rates (Code, Year, Rate) VALUES ('USD', 2024, 1.1), ('EUR', 2024, 0.9)";
                await cmd.ExecuteNonQueryAsync();
            }

            await using var query = new global::ClickHouse.Driver.ADO.ClickHouseConnection(conn);
            await query.OpenAsync();
            using var get = query.CreateCommand();
            get.CommandText = "SELECT dictGet('fx_rate_dict', 'Rate', ('USD', toUInt16(2024)))";
            Assert.Equal(1.1, Convert.ToDouble(await get.ExecuteScalarAsync()), 3);
        }
    }

    private sealed class CompositeKeyContext(DbContextOptions options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<FxRate>(e =>
            {
                e.HasKey(x => new { x.Code, x.Year });
                e.ToTable("fx_rates", t => t.HasMergeTreeEngine().WithOrderBy("Code", "Year"));
            });

            modelBuilder.HasDictionary<FxRate>("fx_rate_dict")
                .FromTable<FxRate>()
                .HasKey(d => new { d.Code, d.Year })
                .Layout(ClickHouseDictionaryLayout.ComplexKeyHashed)
                .Lifetime(minSeconds: 300, maxSeconds: 360)
                .WithSourceConnection(user: "default", password: NamedCollectionClickHouseFixture.Password);
        }
    }

    private class FxRate
    {
        public string Code { get; set; } = string.Empty;
        public ushort Year { get; set; }
        public double Rate { get; set; }
    }

    // ---- Shared helpers --------------------------------------------------------------------------

    private static DbContext CreateContext(
        string connectionString,
        Action<ClickHouseDictionaryBuilder<Currency>> configureSource,
        string tableSuffix = "")
    {
        var options = new DbContextOptionsBuilder()
            .UseClickHouse(connectionString)
            .EnableServiceProviderCaching(false)
            .Options;
        return new DictContext(options, configureSource, tableSuffix);
    }

    private static IReadOnlyList<MigrationOperation> Diff(DbContext? from, DbContext to)
    {
        var fromModel = from?.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var toModel = to.GetService<IDesignTimeModel>().Model.GetRelationalModel();
        var ops = to.GetService<IMigrationsModelDiffer>().GetDifferences(fromModel, toModel);
        return new ClickHouseMigrationsSplitter().Split(ops).Select(s => s.Operation).ToList();
    }

    private static string Generate(IReadOnlyList<MigrationOperation> ops, DbContext ctx)
        => string.Join("\n", ctx.GetService<IMigrationsSqlGenerator>().Generate(ops).Select(c => c.CommandText));

    private static async Task ApplyAsync(string conn, IReadOnlyList<MigrationOperation> ops, DbContext ctx)
    {
        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(conn);
        await connection.OpenAsync();
        foreach (var command in ctx.GetService<IMigrationsSqlGenerator>().Generate(ops))
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = command.CommandText;
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private static async Task SeedAsync(string conn, string table = "currencies")
    {
        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(conn);
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"INSERT INTO {table} (Id, Rate, Name) VALUES (1, 1.5, 'USD'), (2, 2.5, 'EUR')";
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<string?> DictGetNameAsync(string conn, ulong key, string dict = "currency_dict")
    {
        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(conn);
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT dictGet('{dict}', 'Name', toUInt64({key}))";
        return (await cmd.ExecuteScalarAsync())?.ToString();
    }

    private sealed class DictContext(
        DbContextOptions options,
        Action<ClickHouseDictionaryBuilder<Currency>> configureSource,
        string tableSuffix) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            var table = string.IsNullOrEmpty(tableSuffix) ? "currencies" : $"currencies_{tableSuffix}";
            var dict = string.IsNullOrEmpty(tableSuffix) ? "currency_dict" : $"currency_dict_{tableSuffix}";

            modelBuilder.Entity<Currency>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable(table, t => t.HasMergeTreeEngine().WithOrderBy("Id"));
            });

            var builder = modelBuilder.HasDictionary<Currency>(dict)
                .FromTable<Currency>()
                .HasKey(d => d.Id)
                .Layout(ClickHouseDictionaryLayout.Hashed)
                .Lifetime(minSeconds: 300, maxSeconds: 360);
            configureSource(builder);
        }
    }

    private class Currency
    {
        public ulong Id { get; set; }
        public double Rate { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}

/// <summary>
/// A ClickHouse container whose <c>default</c> user has no password. The Testcontainers ClickHouse
/// module mandates a non-empty password, so this builds a raw container off the base image (whose
/// <c>default</c> user is passwordless out of the box) and constructs the connection string by hand.
/// </summary>
public sealed class PasswordlessClickHouseFixture : IAsyncLifetime
{
    private DotNet.Testcontainers.Containers.IContainer _container = default!;
    public string ConnectionString { get; private set; } = default!;

    public async Task InitializeAsync()
    {
        // The modern base image requires a password for `default`; this users.d override restores the
        // passwordless behavior the "no credentials" dictionary source mode relies on.
        const string usersXml =
            """
            <clickhouse>
                <users>
                    <default>
                        <password></password>
                        <networks><ip>::/0</ip></networks>
                        <profile>default</profile>
                        <quota>default</quota>
                    </default>
                </users>
            </clickhouse>
            """;

        _container = new DotNet.Testcontainers.Builders.ContainerBuilder()
            .WithImage("clickhouse/clickhouse-server:latest")
            .WithPortBinding(8123, true)
            .WithPortBinding(9000, true)
            .WithResourceMapping(
                Encoding.UTF8.GetBytes(usersXml),
                "/etc/clickhouse-server/users.d/allow_no_password.xml")
            .WithWaitStrategy(DotNet.Testcontainers.Builders.Wait.ForUnixContainer()
                .UntilHttpRequestIsSucceeded(r => r.ForPort(8123).ForPath("/ping")))
            .Build();
        await _container.StartAsync();

        ConnectionString = new global::ClickHouse.Driver.ADO.ClickHouseConnectionStringBuilder
        {
            Host = _container.Hostname,
            Port = _container.GetMappedPublicPort(8123),
            Username = "default",
            Database = "default",
        }.ConnectionString;
    }

    public async Task DisposeAsync() => await _container.DisposeAsync();
}

/// <summary>
/// A password-protected ClickHouse container that also defines a named collection in server config,
/// so it can exercise both the inline-password and named-collection dictionary source modes.
/// </summary>
public sealed class NamedCollectionClickHouseFixture : IAsyncLifetime
{
    public const string Password = "secret";
    public const string CollectionName = "ch_test";

    private ClickHouseContainer _container = default!;
    public string ConnectionString { get; private set; } = default!;

    public async Task InitializeAsync()
    {
        var configXml =
            $"""
             <clickhouse>
                 <named_collections>
                     <{CollectionName}>
                         <user>default</user>
                         <password>{Password}</password>
                     </{CollectionName}>
                 </named_collections>
             </clickhouse>
             """;

        _container = new ClickHouseBuilder("clickhouse/clickhouse-server:latest")
            .WithUsername("default")
            .WithPassword(Password)
            .WithDatabase("default")
            .WithResourceMapping(
                Encoding.UTF8.GetBytes(configXml),
                "/etc/clickhouse-server/config.d/named_collections.xml")
            .Build();
        await _container.StartAsync();
        ConnectionString = _container.GetConnectionString();
    }

    public async Task DisposeAsync() => await _container.DisposeAsync();
}
