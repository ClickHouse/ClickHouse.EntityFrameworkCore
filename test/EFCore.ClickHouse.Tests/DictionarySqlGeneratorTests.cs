using ClickHouse.EntityFrameworkCore.Extensions;
using ClickHouse.EntityFrameworkCore.Migrations;
using ClickHouse.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Xunit;

namespace EFCore.ClickHouse.Tests;

/// <summary>
/// Unit tests for the SQL text generated for dictionary operations. No database.
/// </summary>
public class DictionarySqlGeneratorTests
{
    private static ClickHouseCreateDictionaryOperation SampleCreate(bool orReplace = false) => new()
    {
        DictionaryName = "currency_dict",
        Columns =
        [
            new ClickHouseDictionaryColumn("Id", "UInt64"),
            new ClickHouseDictionaryColumn("Rate", "Float64"),
            new ClickHouseDictionaryColumn("Name", "String", "''"),
        ],
        KeyColumns = ["Id"],
        SourceTable = "currencies",
        SourceDatabase = "shop",
        Layout = "HASHED",
        LifetimeMin = 300,
        LifetimeMax = 360,
        OrReplace = orReplace,
    };

    [Fact]
    public void CreateDictionary_emits_full_ddl()
    {
        var sql = Generate(SampleCreate());

        Assert.Contains("CREATE DICTIONARY `currency_dict`", sql);
        Assert.Contains("`Id` UInt64", sql);
        Assert.Contains("`Rate` Float64", sql);
        Assert.Contains("`Name` String DEFAULT ''", sql);
        Assert.Contains("PRIMARY KEY `Id`", sql);
        Assert.Contains("SOURCE(CLICKHOUSE(TABLE 'currencies' DB 'shop'))", sql);
        Assert.Contains("LAYOUT(HASHED())", sql);
        Assert.Contains("LIFETIME(MIN 300 MAX 360)", sql);
    }

    [Fact]
    public void CreateDictionary_orReplace_emits_create_or_replace()
    {
        var sql = Generate(SampleCreate(orReplace: true));
        Assert.Contains("CREATE OR REPLACE DICTIONARY `currency_dict`", sql);
        Assert.DoesNotContain("IF NOT EXISTS", sql);
    }

    [Fact]
    public void CreateDictionary_layout_params_are_emitted()
    {
        var op = SampleCreate();
        op.Layout = "CACHE";
        op.LayoutParams = "SIZE_IN_CELLS 10000";
        var sql = Generate(op);
        Assert.Contains("LAYOUT(CACHE(SIZE_IN_CELLS 10000))", sql);
    }

    [Fact]
    public void CreateDictionary_emits_inline_source_credentials()
    {
        var op = SampleCreate();
        op.SourceUser = "reader";
        op.SourcePassword = "p@ss'word";
        op.SourceHost = "ch-1.internal";
        op.SourcePort = 9440;
        var sql = Generate(op);

        // Credentials precede TABLE/DB; the password is escaped as a ClickHouse string literal.
        Assert.Contains("HOST 'ch-1.internal' PORT 9440 USER 'reader' PASSWORD 'p@ss\\'word' TABLE 'currencies' DB 'shop'", sql);
    }

    [Fact]
    public void CreateDictionary_named_collection_emits_NAME_and_no_password()
    {
        var op = SampleCreate();
        op.SourceNamedCollection = "ch_creds";
        var sql = Generate(op);

        Assert.Contains("SOURCE(CLICKHOUSE(NAME 'ch_creds' TABLE 'currencies' DB 'shop'))", sql);
        Assert.DoesNotContain("PASSWORD", sql);
    }

    [Fact]
    public void CreateDictionary_without_credentials_emits_bare_source()
    {
        var sql = Generate(SampleCreate());
        Assert.Contains("SOURCE(CLICKHOUSE(TABLE 'currencies' DB 'shop'))", sql);
        Assert.DoesNotContain("USER", sql);
        Assert.DoesNotContain("NAME ", sql);
    }

    [Fact]
    public void DropDictionary_emits_drop_with_if_exists()
    {
        var sql = Generate(new ClickHouseDropDictionaryOperation { DictionaryName = "currency_dict", IfExists = true });
        Assert.Contains("DROP DICTIONARY IF EXISTS `currency_dict`", sql);
    }

    private static string Generate(params MigrationOperation[] operations)
    {
        var options = new DbContextOptionsBuilder().UseClickHouse("Host=localhost;Database=test").Options;
        using var context = new DbContext(options);
        var generator = context.GetService<IMigrationsSqlGenerator>();
        return string.Join("\n", generator.Generate(operations).Select(c => c.CommandText));
    }
}
