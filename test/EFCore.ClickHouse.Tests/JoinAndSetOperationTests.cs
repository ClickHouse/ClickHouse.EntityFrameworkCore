using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.ClickHouse.Tests;

#region Entities

public class Customer
{
    public long Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string City { get; set; } = string.Empty;
}

public class Order
{
    public long Id { get; set; }
    public long CustomerId { get; set; }
    public string Product { get; set; } = string.Empty;
    public decimal Amount { get; set; }
}

public class JoinDbContext : DbContext
{
    public DbSet<Customer> Customers => Set<Customer>();
    public DbSet<Order> Orders => Set<Order>();

    private readonly string _connectionString;

    public JoinDbContext(string connectionString)
    {
        _connectionString = connectionString;
    }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseClickHouse(_connectionString);
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Customer>(entity =>
        {
            entity.ToTable("join_customers");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.Name).HasColumnName("name");
            entity.Property(e => e.City).HasColumnName("city");
        });

        modelBuilder.Entity<Order>(entity =>
        {
            entity.ToTable("join_orders");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.CustomerId).HasColumnName("customer_id");
            entity.Property(e => e.Product).HasColumnName("product");
            entity.Property(e => e.Amount).HasColumnName("amount").HasColumnType("Decimal(18, 2)");
        });
    }
}

#endregion

public class JoinFixture : IAsyncLifetime
{
    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        ConnectionString = await SharedContainer.GetConnectionStringAsync();

        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(ConnectionString);
        await connection.OpenAsync();

        using var cmd1 = connection.CreateCommand();
        cmd1.CommandText = """
            CREATE TABLE join_customers (
                id Int64,
                name String,
                city String
            ) ENGINE = MergeTree()
            ORDER BY id
            """;
        await cmd1.ExecuteNonQueryAsync();

        using var cmd2 = connection.CreateCommand();
        cmd2.CommandText = """
            CREATE TABLE join_orders (
                id Int64,
                customer_id Int64,
                product String,
                amount Decimal(18, 2)
            ) ENGINE = MergeTree()
            ORDER BY id
            """;
        await cmd2.ExecuteNonQueryAsync();

        // Seed customers
        using var cmd3 = connection.CreateCommand();
        cmd3.CommandText = """
            INSERT INTO join_customers (id, name, city) VALUES
            (1, 'Alice', 'New York'),
            (2, 'Bob', 'London'),
            (3, 'Charlie', 'Paris'),
            (4, 'Diana', 'New York'),
            (5, 'Eve', 'Tokyo')
            """;
        await cmd3.ExecuteNonQueryAsync();

        // Seed orders (customer 5 has no orders; order customer_id=99 has no matching customer)
        using var cmd4 = connection.CreateCommand();
        cmd4.CommandText = """
            INSERT INTO join_orders (id, customer_id, product, amount) VALUES
            (1, 1, 'Widget', 10.00),
            (2, 1, 'Gadget', 25.50),
            (3, 2, 'Widget', 10.00),
            (4, 3, 'Gizmo', 42.00),
            (5, 3, 'Widget', 10.00),
            (6, 3, 'Gadget', 25.50),
            (7, 4, 'Gizmo', 42.00),
            (8, 99, 'Orphan', 5.00)
            """;
        await cmd4.ExecuteNonQueryAsync();
    }

    public Task DisposeAsync() => Task.CompletedTask;
}

public class JoinTests : IClassFixture<JoinFixture>
{
    private readonly JoinFixture _fixture;

    public JoinTests(JoinFixture fixture)
    {
        _fixture = fixture;
    }

    // ─── INNER JOIN ────────────────────────────────────────

    [Fact]
    public async Task InnerJoin_ReturnsMatchingRows()
    {
        await using var ctx = new JoinDbContext(_fixture.ConnectionString);

        var results = await ctx.Customers
            .Join(
                ctx.Orders,
                c => c.Id,
                o => o.CustomerId,
                (c, o) => new { c.Name, o.Product, o.Amount })
            .OrderBy(x => x.Name).ThenBy(x => x.Product)
            .AsNoTracking()
            .ToListAsync();

        // Alice(2) + Bob(1) + Charlie(3) + Diana(1) = 7 rows (orphan excluded, Eve excluded)
        Assert.Equal(7, results.Count);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal("Gadget", results[0].Product);
    }

    [Fact]
    public async Task InnerJoin_WithFilter_ReturnsFilteredRows()
    {
        await using var ctx = new JoinDbContext(_fixture.ConnectionString);

        var results = await ctx.Customers
            .Join(
                ctx.Orders,
                c => c.Id,
                o => o.CustomerId,
                (c, o) => new { c.Name, c.City, o.Product, o.Amount })
            .Where(x => x.City == "New York")
            .OrderBy(x => x.Name).ThenBy(x => x.Product)
            .AsNoTracking()
            .ToListAsync();

        // Alice(New York, 2 orders) + Diana(New York, 1 order) = 3
        Assert.Equal(3, results.Count);
        Assert.All(results, r => Assert.True(r.Name == "Alice" || r.Name == "Diana"));
    }

    [Fact]
    public async Task InnerJoin_WithAggregate_GroupsCorrectly()
    {
        await using var ctx = new JoinDbContext(_fixture.ConnectionString);

        var results = await ctx.Customers
            .Join(
                ctx.Orders,
                c => c.Id,
                o => o.CustomerId,
                (c, o) => new { c.Name, o.Amount })
            .GroupBy(x => x.Name)
            .Select(g => new { Name = g.Key, Total = g.Sum(x => x.Amount) })
            .OrderBy(x => x.Name)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(4, results.Count);
        // Alice: 10.00 + 25.50 = 35.50
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal(35.50m, results[0].Total);
        // Charlie: 42.00 + 10.00 + 25.50 = 77.50
        Assert.Equal("Charlie", results[2].Name);
        Assert.Equal(77.50m, results[2].Total);
    }

    // ─── LEFT JOIN ─────────────────────────────────────────

    [Fact]
    public async Task LeftJoin_IncludesCustomersWithoutOrders()
    {
        await using var ctx = new JoinDbContext(_fixture.ConnectionString);

        var results = await ctx.Customers
            .GroupJoin(
                ctx.Orders,
                c => c.Id,
                o => o.CustomerId,
                (c, orders) => new { c.Name, Orders = orders })
            .SelectMany(
                x => x.Orders.DefaultIfEmpty(),
                (x, o) => new { x.Name, Product = o == null ? "(none)" : o.Product })
            .OrderBy(x => x.Name).ThenBy(x => x.Product)
            .AsNoTracking()
            .ToListAsync();

        // All 5 customers; Eve has no orders → (none)
        Assert.Equal(8, results.Count);
        Assert.Contains(results, r => r.Name == "Eve" && r.Product == "(none)");
    }

    // ─── CROSS JOIN ────────────────────────────────────────

    [Fact]
    public async Task CrossJoin_ViaSelectMany_ReturnsCartesianProduct()
    {
        await using var ctx = new JoinDbContext(_fixture.ConnectionString);

        // Cross join just the cities with a small subset
        var cities = ctx.Customers
            .Select(c => c.City)
            .Distinct();

        var products = ctx.Orders
            .Select(o => o.Product)
            .Distinct();

        var results = await cities
            .SelectMany(_ => products, (city, product) => new { City = city, Product = product })
            .OrderBy(x => x.City).ThenBy(x => x.Product)
            .AsNoTracking()
            .ToListAsync();

        // 4 distinct cities × 4 distinct products = 16
        Assert.Equal(16, results.Count);
    }

    // ─── SUBQUERIES ────────────────────────────────────────

    [Fact]
    public async Task Subquery_InFilter_WhereIdInSubquery()
    {
        await using var ctx = new JoinDbContext(_fixture.ConnectionString);

        // Customers who have at least one order for 'Gizmo'
        var gizmoCustomerIds = ctx.Orders
            .Where(o => o.Product == "Gizmo")
            .Select(o => o.CustomerId);

        var results = await ctx.Customers
            .Where(c => gizmoCustomerIds.Contains(c.Id))
            .OrderBy(c => c.Name)
            .AsNoTracking()
            .ToListAsync();

        // Charlie and Diana ordered Gizmo
        Assert.Equal(2, results.Count);
        Assert.Equal("Charlie", results[0].Name);
        Assert.Equal("Diana", results[1].Name);
    }

    [Fact]
    public async Task Subquery_Any_WhereExistsCorrelated()
    {
        await using var ctx = new JoinDbContext(_fixture.ConnectionString);

        // Customers who have ANY order with Amount > 40
        var results = await ctx.Customers
            .Where(c => ctx.Orders.Any(o => o.CustomerId == c.Id && o.Amount > 40))
            .OrderBy(c => c.Name)
            .AsNoTracking()
            .ToListAsync();

        // Charlie (Gizmo 42.00) and Diana (Gizmo 42.00)
        Assert.Equal(2, results.Count);
        Assert.Equal("Charlie", results[0].Name);
        Assert.Equal("Diana", results[1].Name);
    }

    [Fact]
    public async Task Subquery_All_WhereAllOrdersMatch()
    {
        await using var ctx = new JoinDbContext(_fixture.ConnectionString);

        // Customers where ALL their orders have Amount <= 25.50
        // Must also have at least one order (exclude Eve)
        var customersWithOrders = ctx.Orders.Select(o => o.CustomerId).Distinct();

        var results = await ctx.Customers
            .Where(c => customersWithOrders.Contains(c.Id))
            .Where(c => ctx.Orders.Where(o => o.CustomerId == c.Id).All(o => o.Amount <= 25.50m))
            .OrderBy(c => c.Name)
            .AsNoTracking()
            .ToListAsync();

        // Alice: 10+25.50 ✓, Bob: 10 ✓
        Assert.Equal(2, results.Count);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal("Bob", results[1].Name);
    }

    [Fact]
    public async Task Subquery_ScalarInProjection_RawSqlNullBehavior()
    {
        // Verify ClickHouse scalar subquery returns NULL for no-match rows
        // (this is why the provider wraps non-nullable subqueries with ifNull)
        using var connection = new global::ClickHouse.Driver.ADO.ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT j.`name`,
                   ifNull((SELECT COUNT(*) FROM `join_orders` AS j0 WHERE j0.`customer_id` = j.`id`), 0)
            FROM `join_customers` AS j
            ORDER BY j.`name`
            """;

        using var reader = await cmd.ExecuteReaderAsync();
        var rows = new List<(string Name, ulong Count)>();
        while (await reader.ReadAsync())
        {
            rows.Add((reader.GetString(0), (ulong)reader.GetValue(1)));
        }

        Assert.Equal(5, rows.Count);
        Assert.Equal(("Alice", 2UL), rows[0]);
        Assert.Equal(("Eve", 0UL), rows[4]); // ifNull converts NULL → 0
    }

    [Fact]
    public async Task Subquery_ScalarInProjection()
    {
        await using var ctx = new JoinDbContext(_fixture.ConnectionString);

        var results = await ctx.Customers
            .Select(c => new
            {
                c.Name,
                OrderCount = ctx.Orders.Count(o => o.CustomerId == c.Id)
            })
            .OrderBy(x => x.Name)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(5, results.Count);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal(2, results[0].OrderCount);
        Assert.Equal("Eve", results[4].Name);
        Assert.Equal(0, results[4].OrderCount);
    }

    [Fact]
    public void Count_AlwaysCastToInt64()
    {
        // Regression for PR review finding 6: COUNT always returns UInt64 from ClickHouse.
        // We CAST the server-side result to Int64 (never Int32) — Int64 is wide enough to
        // hold any realistic COUNT, is a common supertype for any signed-integer set-op
        // branch, and when EF expects Int32 the client-side ClickHouseInt32TypeMapping
        // narrows via Convert.ToInt32(Int64) which still raises OverflowException above
        // Int32.MaxValue. A server-side CAST AS Int32 would silently truncate instead.
        using var ctx = new JoinDbContext(_fixture.ConnectionString);

        var sql = ctx.Customers.Select(c => ctx.Orders.Count(o => o.CustomerId == c.Id))
            .ToQueryString();

        Assert.Matches(@"CAST\([^)]*COUNT[^)]*\)\s+AS\s+Int64\)", sql);
        Assert.DoesNotMatch(@"AS\s+Int32\)", sql);
    }

    [Fact]
    public async Task Count_TopLevel_ReturnsCorrectValue()
    {
        // Sanity: CAST to Int64 at the server still materializes correctly when EF expects Int32.
        await using var ctx = new JoinDbContext(_fixture.ConnectionString);

        var counts = await ctx.Customers.Select(c => ctx.Orders.Count(o => o.CustomerId == c.Id))
            .ToListAsync();
        Assert.Equal(5, counts.Count);
    }

    [Fact]
    public void ScalarSubquery_Count_IsWrappedWithIfNull()
    {
        // ClickHouse returns NULL for no-match scalar subqueries even for COUNT, unlike
        // standard SQL. Wrapping with ifNull(..., 0) is the right fix for COUNT specifically.
        using var ctx = new JoinDbContext(_fixture.ConnectionString);

        var sql = ctx.Customers
            .Select(c => new
            {
                c.Name,
                Cnt = ctx.Orders.Count(o => o.CustomerId == c.Id)
            })
            .ToQueryString();

        Assert.Contains("ifNull(", sql);
    }

    [Fact]
    public void ScalarSubquery_Sum_IsWrappedWithIfNull()
    {
        // LINQ Sum() on an empty set returns 0. EF emits COALESCE(SUM(...), 0), but ClickHouse's
        // scalar subquery wraps the whole thing with NULL on empty input — so the outer
        // ifNull is still needed to match LINQ semantics.
        using var ctx = new JoinDbContext(_fixture.ConnectionString);

        var sql = ctx.Customers
            .Select(c => new
            {
                c.Name,
                Total = ctx.Orders.Where(o => o.CustomerId == c.Id).Sum(o => o.Amount)
            })
            .ToQueryString();

        Assert.Contains("ifNull(", sql);
    }

    [Fact]
    public void ScalarSubquery_FirstOrDefault_IsNotWrappedWithIfNullZero()
    {
        // A non-COUNT scalar subquery (like FirstOrDefault on a non-nullable value type)
        // must NOT be wrapped with ifNull(..., 0). The fallback value 0 is semantically
        // wrong for aggregates like Min/Max where "no row" is not "zero", and it is
        // type-incorrect for projections like DateTime / Guid.
        using var ctx = new JoinDbContext(_fixture.ConnectionString);

        var sql = ctx.Customers
            .Select(c => new
            {
                c.Name,
                FirstAmount = ctx.Orders
                    .Where(o => o.CustomerId == c.Id)
                    .Select(o => o.Amount)
                    .FirstOrDefault()
            })
            .ToQueryString();

        // The only ifNull the provider emits is for COUNT. A non-COUNT subquery has
        // no reason to be wrapped.
        Assert.DoesNotContain("ifNull(", sql);
    }

    // Regression for PR #10 review finding 3: interface collection CLR types claimed
    // support but only List<T> and T[] actually worked. These tests pin the behavior
    // for each interface shape.

    [Fact]
    public async Task Join_Local_ArrayCollection_Works()
    {
        await using var ctx = new JoinDbContext(_fixture.ConnectionString);
        var ids = new long[] { 1, 2, 3 };

        var results = await ctx.Customers
            .Where(c => ids.Contains(c.Id))
            .OrderBy(c => c.Id)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(3, results.Count);
    }

    [Fact]
    public async Task Join_Local_ListCollection_Works()
    {
        await using var ctx = new JoinDbContext(_fixture.ConnectionString);
        var ids = new List<long> { 1, 2, 3 };

        var results = await ctx.Customers
            .Where(c => ids.Contains(c.Id))
            .OrderBy(c => c.Id)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(3, results.Count);
    }

    [Fact]
    public async Task Join_Local_IEnumerableCollection_Works()
    {
        await using var ctx = new JoinDbContext(_fixture.ConnectionString);
        IEnumerable<long> ids = new List<long> { 1, 2, 3 };

        var results = await ctx.Customers
            .Where(c => ids.Contains(c.Id))
            .OrderBy(c => c.Id)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(3, results.Count);
    }

    [Fact]
    public async Task Join_Local_IReadOnlyListCollection_Works()
    {
        await using var ctx = new JoinDbContext(_fixture.ConnectionString);
        IReadOnlyList<long> ids = new List<long> { 1, 2, 3 };

        var results = await ctx.Customers
            .Where(c => ids.Contains(c.Id))
            .OrderBy(c => c.Id)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(3, results.Count);
    }

    [Fact]
    public async Task Join_Local_IListCollection_Works()
    {
        await using var ctx = new JoinDbContext(_fixture.ConnectionString);
        IList<long> ids = new List<long> { 1, 2, 3 };

        var results = await ctx.Customers
            .Where(c => ids.Contains(c.Id))
            .OrderBy(c => c.Id)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(3, results.Count);
    }

    [Fact]
    public void ScalarSubquery_Max_IsNotWrappedWithIfNullZero()
    {
        // Max on an empty set should return NULL in ClickHouse, matching LINQ/EF semantics
        // for nullable aggregates. Wrapping with ifNull(..., 0) would produce 0 instead.
        using var ctx = new JoinDbContext(_fixture.ConnectionString);

        var sql = ctx.Customers
            .Select(c => new
            {
                c.Name,
                MaxAmount = ctx.Orders
                    .Where(o => o.CustomerId == c.Id)
                    .Max(o => (decimal?)o.Amount)
            })
            .ToQueryString();

        Assert.DoesNotContain("ifNull(", sql);
    }
}

public class SetOperationTests : IClassFixture<JoinFixture>
{
    private readonly JoinFixture _fixture;

    public SetOperationTests(JoinFixture fixture)
    {
        _fixture = fixture;
    }

    // ─── UNION ALL (Concat) ────────────────────────────────

    [Fact]
    public async Task Concat_UnionAll_ReturnsDuplicates()
    {
        await using var ctx = new JoinDbContext(_fixture.ConnectionString);

        var nyCustomers = ctx.Customers.Where(c => c.City == "New York").Select(c => c.Name);
        var activeCustomers = ctx.Customers.Where(c => c.Id <= 2).Select(c => c.Name);

        // Alice appears in both sets → should appear twice
        var results = await nyCustomers
            .Concat(activeCustomers)
            .OrderBy(n => n)
            .AsNoTracking()
            .ToListAsync();

        // NY: Alice, Diana; Id<=2: Alice, Bob → combined: Alice, Alice, Bob, Diana
        Assert.Equal(4, results.Count);
        Assert.Equal(2, results.Count(n => n == "Alice"));
    }

    // ─── UNION DISTINCT ───────────────────────────────────

    [Fact]
    public async Task Union_Distinct_RemovesDuplicates()
    {
        await using var ctx = new JoinDbContext(_fixture.ConnectionString);

        var nyCustomers = ctx.Customers.Where(c => c.City == "New York").Select(c => c.Name);
        var activeCustomers = ctx.Customers.Where(c => c.Id <= 2).Select(c => c.Name);

        var results = await nyCustomers
            .Union(activeCustomers)
            .OrderBy(n => n)
            .AsNoTracking()
            .ToListAsync();

        // Alice (deduped), Bob, Diana
        Assert.Equal(3, results.Count);
        Assert.Equal(1, results.Count(n => n == "Alice"));
    }

    // ─── INTERSECT ────────────────────────────────────────

    [Fact]
    public async Task Intersect_ReturnsCommonElements()
    {
        await using var ctx = new JoinDbContext(_fixture.ConnectionString);

        var nyCustomers = ctx.Customers.Where(c => c.City == "New York").Select(c => c.Name);
        var firstThree = ctx.Customers.Where(c => c.Id <= 3).Select(c => c.Name);

        var results = await nyCustomers
            .Intersect(firstThree)
            .OrderBy(n => n)
            .AsNoTracking()
            .ToListAsync();

        // NY: Alice, Diana; first 3: Alice, Bob, Charlie → intersection: Alice
        Assert.Single(results);
        Assert.Equal("Alice", results[0]);
    }

    // ─── EXCEPT ───────────────────────────────────────────

    [Fact]
    public async Task Except_ReturnsOnlyInFirst()
    {
        await using var ctx = new JoinDbContext(_fixture.ConnectionString);

        var allNames = ctx.Customers.Select(c => c.Name);
        var nyNames = ctx.Customers.Where(c => c.City == "New York").Select(c => c.Name);

        var results = await allNames
            .Except(nyNames)
            .OrderBy(n => n)
            .AsNoTracking()
            .ToListAsync();

        // All: Alice,Bob,Charlie,Diana,Eve minus NY: Alice,Diana → Bob,Charlie,Eve
        Assert.Equal(3, results.Count);
        Assert.Equal("Bob", results[0]);
        Assert.Equal("Charlie", results[1]);
        Assert.Equal("Eve", results[2]);
    }

    // ─── Set operations with full entity projections ──────

    [Fact]
    public async Task Union_FullEntity_RemovesDuplicates()
    {
        await using var ctx = new JoinDbContext(_fixture.ConnectionString);

        var set1 = ctx.Customers.Where(c => c.City == "New York");
        var set2 = ctx.Customers.Where(c => c.Id <= 2);

        var results = await set1
            .Union(set2)
            .OrderBy(c => c.Name)
            .AsNoTracking()
            .ToListAsync();

        // Alice (deduped), Bob, Diana
        Assert.Equal(3, results.Count);
    }

    [Fact]
    public async Task Concat_FullEntity_KeepsDuplicates()
    {
        await using var ctx = new JoinDbContext(_fixture.ConnectionString);

        var set1 = ctx.Customers.Where(c => c.City == "New York");
        var set2 = ctx.Customers.Where(c => c.Id <= 2);

        var results = await set1
            .Concat(set2)
            .OrderBy(c => c.Name)
            .AsNoTracking()
            .ToListAsync();

        // Alice×2, Bob, Diana
        Assert.Equal(4, results.Count);
    }

    // ─── Set operations with Take/Skip ────────────────────

    [Fact]
    public async Task Union_WithTake_LimitsResults()
    {
        await using var ctx = new JoinDbContext(_fixture.ConnectionString);

        var results = await ctx.Customers
            .Where(c => c.City == "New York")
            .Select(c => c.Name)
            .Union(ctx.Customers.Where(c => c.City == "London").Select(c => c.Name))
            .OrderBy(n => n)
            .Take(2)
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Equal("Alice", results[0]);
        Assert.Equal("Bob", results[1]);
    }

    // ─── Chained set operations ───────────────────────────

    [Fact]
    public async Task ChainedUnion_CombinesMultipleSets()
    {
        await using var ctx = new JoinDbContext(_fixture.ConnectionString);

        var ny = ctx.Customers.Where(c => c.City == "New York").Select(c => c.Name);
        var london = ctx.Customers.Where(c => c.City == "London").Select(c => c.Name);
        var paris = ctx.Customers.Where(c => c.City == "Paris").Select(c => c.Name);

        var results = await ny
            .Union(london)
            .Union(paris)
            .OrderBy(n => n)
            .AsNoTracking()
            .ToListAsync();

        // Alice, Bob, Charlie, Diana
        Assert.Equal(4, results.Count);
    }

    // ─── Mixed join + set operation ───────────────────────

    [Fact]
    public async Task Union_AfterJoin_Works()
    {
        await using var ctx = new JoinDbContext(_fixture.ConnectionString);

        // Products ordered by NY customers UNION products ordered by London customers
        var nyProducts = ctx.Customers
            .Where(c => c.City == "New York")
            .Join(ctx.Orders, c => c.Id, o => o.CustomerId, (c, o) => o.Product);

        var londonProducts = ctx.Customers
            .Where(c => c.City == "London")
            .Join(ctx.Orders, c => c.Id, o => o.CustomerId, (c, o) => o.Product);

        var results = await nyProducts
            .Union(londonProducts)
            .OrderBy(p => p)
            .AsNoTracking()
            .ToListAsync();

        // NY orders: Widget, Gadget, Gizmo; London: Widget → union: Gadget, Gizmo, Widget
        Assert.Equal(3, results.Count);
        Assert.Equal("Gadget", results[0]);
        Assert.Equal("Gizmo", results[1]);
        Assert.Equal("Widget", results[2]);
    }
}
