using ClickHouse.EntityFrameworkCore.Extensions;
using ClickHouse.EntityFrameworkCore.Metadata;
using ClickHouse.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Xunit;

namespace EFCore.ClickHouse.Tests;

/// <summary>
/// A declared dictionary is queryable like a keyless DbSet: its CLR type is mapped as a view over
/// the dictionary, so <c>Set&lt;TDict&gt;()</c> compiles to <c>SELECT … FROM `dict`</c> and is never
/// migrated as a table. No database — these assert model shape and generated SQL.
/// </summary>
public class DictionaryQueryTests
{
    [Fact]
    public void Dictionary_type_is_mapped_as_keyless_view_not_a_table()
    {
        using var ctx = new QueryContext();
        var et = ctx.GetService<IDesignTimeModel>().Model.FindEntityType(typeof(CurrencyDict));

        Assert.NotNull(et);
        Assert.Null(et!.FindPrimaryKey());                 // keyless
        Assert.Equal("currency_dict", et.GetViewName());   // mapped to the dictionary as a view
        Assert.Null(et.GetTableName());                    // never a table
    }

    [Fact]
    public void Querying_the_dictionary_selects_from_the_dictionary()
    {
        using var ctx = new QueryContext();
        var sql = ctx.Set<CurrencyDict>().Where(d => d.Id == 1UL).ToQueryString();

        Assert.Contains("FROM `currency_dict`", sql);
        Assert.Contains("`Rate`", sql);
    }

    [Fact]
    public void Dictionary_view_entity_is_not_migrated_as_a_table()
    {
        using var ctx = new QueryContext();
        using var empty = new EmptyContext();

        var ops = ctx.GetService<IMigrationsModelDiffer>().GetDifferences(
            empty.GetService<IDesignTimeModel>().Model.GetRelationalModel(),
            ctx.GetService<IDesignTimeModel>().Model.GetRelationalModel());

        // The source table is created; the dictionary is a CREATE DICTIONARY; no CREATE TABLE for the view.
        Assert.Contains(ops, o => o is CreateTableOperation { Name: "currencies" });
        Assert.Contains(ops, o => o is ClickHouseCreateDictionaryOperation);
        Assert.DoesNotContain(ops, o => o is CreateTableOperation { Name: "currency_dict" });
    }

    private sealed class QueryContext : DbContext
    {
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.UseClickHouse("Host=localhost;Database=test").EnableServiceProviderCaching(false);

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Currency>(e =>
            {
                e.HasKey(x => x.Id);
                e.ToTable("currencies", t => t.HasMergeTreeEngine().WithOrderBy("Id"));
            });

            modelBuilder.HasDictionary<CurrencyDict>("currency_dict")
                .FromTable<Currency>()
                .HasKey(d => d.Id)
                .Layout(ClickHouseDictionaryLayout.Hashed)
                .Lifetime(60);
        }
    }

    private sealed class EmptyContext : DbContext
    {
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.UseClickHouse("Host=localhost;Database=test").EnableServiceProviderCaching(false);
    }

    private class Currency
    {
        public ulong Id { get; set; }
        public double Rate { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    // Distinct type from the source table → auto-mapped as the queryable view over the dictionary.
    private class CurrencyDict
    {
        public ulong Id { get; set; }
        public double Rate { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
