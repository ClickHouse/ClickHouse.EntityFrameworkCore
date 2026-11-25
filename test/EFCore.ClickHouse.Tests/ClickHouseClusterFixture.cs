using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Networks;
using Testcontainers.ClickHouse;
using Xunit;

namespace EFCore.ClickHouse.Tests;

/// <summary>
/// Starts a ClickHouse cluster with one shard and two replicas (hosts <c>ch1</c> and <c>ch2</c>).
/// <c>ch1</c> also runs the embedded ClickHouse Keeper that replication and <c>ON CLUSTER</c> DDL need.
/// Both hosts define the macros <c>{shard}</c>, <c>{replica}</c> and <c>{cluster}</c>.
/// </summary>
public sealed class ClickHouseClusterFixture : IAsyncLifetime
{
    public const string ClusterName = "test_cluster";

    private const string Image = "clickhouse/clickhouse-server:latest";

    private INetwork? _network;
    private ClickHouseContainer? _node1;
    private ClickHouseContainer? _node2;
    private int _databaseCounter;

    public string Node1ConnectionString { get; private set; } = default!;
    public string Node2ConnectionString { get; private set; } = default!;

    public async Task InitializeAsync()
    {
        _network = new NetworkBuilder().Build();
        await _network.CreateAsync();

        // Keeper runs on ch1, so ch1 must be up before ch2 connects to it.
        _node1 = CreateNode("ch1", runsKeeper: true);
        await _node1.StartAsync();
        _node2 = CreateNode("ch2", runsKeeper: false);
        await _node2.StartAsync();

        Node1ConnectionString = _node1.GetConnectionString();
        Node2ConnectionString = _node2.GetConnectionString();
    }

    public async Task DisposeAsync()
    {
        if (_node2 is not null)
            await _node2.DisposeAsync();
        if (_node1 is not null)
            await _node1.DisposeAsync();
        if (_network is not null)
            await _network.DisposeAsync();
    }

    /// <summary>Returns a database name that no other test in this fixture uses.</summary>
    public string NewDatabaseName()
        => $"repl_{Interlocked.Increment(ref _databaseCounter)}";

    public string Node1(string database) => WithDatabase(Node1ConnectionString, database);

    public string Node2(string database) => WithDatabase(Node2ConnectionString, database);

    private static string WithDatabase(string connectionString, string database)
        => connectionString.Contains("Database=")
            ? System.Text.RegularExpressions.Regex.Replace(connectionString, @"Database=[^;]*", $"Database={database}")
            : connectionString + $";Database={database}";

    private ClickHouseContainer CreateNode(string host, bool runsKeeper)
    {
        var builder = new ClickHouseBuilder(Image)
            .WithNetwork(_network!)
            .WithNetworkAliases(host)
            .WithHostname(host)
            .WithResourceMapping(Encoding.UTF8.GetBytes(ClusterConfig(host)), FilePath.Of("/etc/clickhouse-server/config.d/cluster.xml"));

        if (runsKeeper)
            builder = builder.WithResourceMapping(Encoding.UTF8.GetBytes(KeeperConfig), FilePath.Of("/etc/clickhouse-server/config.d/keeper.xml"));

        return builder.Build();
    }

    private static string ClusterConfig(string host) => $"""
        <clickhouse>
          <interserver_http_host>{host}</interserver_http_host>
          <macros>
            <shard>01</shard>
            <replica>{host}</replica>
            <cluster>{ClusterName}</cluster>
          </macros>
          <zookeeper>
            <node><host>ch1</host><port>9181</port></node>
          </zookeeper>
          <remote_servers>
            <{ClusterName}>
              <shard>
                <internal_replication>true</internal_replication>
                <replica><host>ch1</host><port>9000</port></replica>
                <replica><host>ch2</host><port>9000</port></replica>
              </shard>
            </{ClusterName}>
          </remote_servers>
        </clickhouse>
        """;

    private const string KeeperConfig = """
        <clickhouse>
          <keeper_server>
            <tcp_port>9181</tcp_port>
            <server_id>1</server_id>
            <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
            <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>
            <raft_configuration>
              <server><id>1</id><hostname>ch1</hostname><port>9234</port></server>
            </raft_configuration>
          </keeper_server>
        </clickhouse>
        """;
}
