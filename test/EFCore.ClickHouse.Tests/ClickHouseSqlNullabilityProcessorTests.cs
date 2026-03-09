using System.Linq.Expressions;
using ClickHouse.EntityFrameworkCore.Query.Expressions.Internal;
using ClickHouse.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

namespace EFCore.ClickHouse.Tests;

public class ClickHouseSqlNullabilityProcessorTests
{
    [Fact]
    public void RowValue_Equal_ExpandsWhenOptimizationDisabled()
    {
        using var ctx = new NullabilityTestDbContext();
        var processor = CreateProcessor(ctx);
        var binary = CreateRowComparisonBinary(ctx, ExpressionType.Equal);

        var result = processor.VisitSqlBinaryPublic(binary, allowOptimizedExpansion: false, out var nullable);

        Assert.False(nullable);
        var sqlBinary = Assert.IsType<SqlBinaryExpression>(result);
        Assert.Equal(ExpressionType.AndAlso, sqlBinary.OperatorType);
    }

    [Fact]
    public void RowValue_Equal_StaysCompactWhenOptimizationEnabled()
    {
        using var ctx = new NullabilityTestDbContext();
        var processor = CreateProcessor(ctx);
        var binary = CreateRowComparisonBinary(ctx, ExpressionType.Equal);

        var result = processor.VisitSqlBinaryPublic(binary, allowOptimizedExpansion: true, out var nullable);

        Assert.False(nullable);
        var sqlBinary = Assert.IsType<SqlBinaryExpression>(result);
        Assert.Equal(ExpressionType.Equal, sqlBinary.OperatorType);
        Assert.IsType<ClickHouseRowValueExpression>(sqlBinary.Left);
        Assert.IsType<ClickHouseRowValueExpression>(sqlBinary.Right);
    }

    [Fact]
    public void RowValue_NotEqual_ExpandsToOrElse()
    {
        using var ctx = new NullabilityTestDbContext();
        var processor = CreateProcessor(ctx);
        var binary = CreateRowComparisonBinary(ctx, ExpressionType.NotEqual);

        var result = processor.VisitSqlBinaryPublic(binary, allowOptimizedExpansion: true, out var nullable);

        Assert.False(nullable);
        var sqlBinary = Assert.IsType<SqlBinaryExpression>(result);
        Assert.Equal(ExpressionType.OrElse, sqlBinary.OperatorType);
    }

    private static TestableClickHouseSqlNullabilityProcessor CreateProcessor(DbContext context)
    {
        var dependencies = context.GetService<RelationalParameterBasedSqlProcessorDependencies>();
        var parameters = new RelationalParameterBasedSqlProcessorParameters(
            useRelationalNulls: false,
            collectionParameterTranslationMode: default);
        return new TestableClickHouseSqlNullabilityProcessor(dependencies, parameters);
    }

    private static SqlBinaryExpression CreateRowComparisonBinary(DbContext context, ExpressionType operatorType)
    {
        var typeMappingSource = context.GetService<IRelationalTypeMappingSource>();
        var intMapping = typeMappingSource.FindMapping(typeof(int))!;

        var leftNonNullable = new SqlParameterExpression(
            "left_nonnull",
            "left_nonnull",
            typeof(int),
            false,
            null,
            intMapping);

        var leftNullable = new SqlParameterExpression(
            "left_nullable",
            "left_nullable",
            typeof(int),
            true,
            null,
            intMapping);

        var rightNonNullableA = new SqlParameterExpression(
            "right_nonnull_a",
            "right_nonnull_a",
            typeof(int),
            false,
            null,
            intMapping);

        var rightNonNullableB = new SqlParameterExpression(
            "right_nonnull_b",
            "right_nonnull_b",
            typeof(int),
            false,
            null,
            intMapping);

        var leftRow = new ClickHouseRowValueExpression(
            [leftNonNullable, leftNullable],
            typeof(ValueTuple<int, int>));

        var rightRow = new ClickHouseRowValueExpression(
            [rightNonNullableA, rightNonNullableB],
            typeof(ValueTuple<int, int>));

        var sqlExpressionFactory = context.GetService<ISqlExpressionFactory>();
        return (SqlBinaryExpression)sqlExpressionFactory.MakeBinary(operatorType, leftRow, rightRow, typeMapping: null)!;
    }

    private sealed class TestableClickHouseSqlNullabilityProcessor : ClickHouseSqlNullabilityProcessor
    {
        public TestableClickHouseSqlNullabilityProcessor(
            RelationalParameterBasedSqlProcessorDependencies dependencies,
            RelationalParameterBasedSqlProcessorParameters parameters)
            : base(dependencies, parameters)
        {
        }

        public SqlExpression VisitSqlBinaryPublic(
            SqlBinaryExpression expression,
            bool allowOptimizedExpansion,
            out bool nullable)
            => base.VisitSqlBinary(expression, allowOptimizedExpansion, out nullable);
    }

    private sealed class NullabilityTestDbContext : DbContext
    {
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.UseClickHouse("Host=localhost;Protocol=http;Port=8123;Database=test");
    }
}
