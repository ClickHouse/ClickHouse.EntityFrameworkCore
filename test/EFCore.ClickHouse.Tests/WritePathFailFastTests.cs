using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.ClickHouse.Tests;

public class WritePathFailFastTests
{
    [Fact]
    public async Task SaveChanges_Throws_NotSupportedException()
    {
        await using var context = new WritePathDbContext();
        context.Entities.Add(new WritePathEntity { Id = 1, Name = "test" });

        var ex = await Assert.ThrowsAsync<NotSupportedException>(() => context.SaveChangesAsync());
        Assert.Contains("read-only query scenarios", ex.Message);
    }

    private sealed class WritePathDbContext : DbContext
    {
        public DbSet<WritePathEntity> Entities => Set<WritePathEntity>();

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.UseClickHouse("Host=localhost;Protocol=http;Port=8123;Database=test");

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<WritePathEntity>(entity =>
            {
                entity.ToTable("write_path_entities");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).HasColumnName("id");
                entity.Property(e => e.Name).HasColumnName("name");
            });
        }
    }

    private sealed class WritePathEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
