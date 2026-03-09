using ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;
using Xunit;

namespace EFCore.ClickHouse.Tests;

/// <summary>
/// Unit tests for SQL literal generation in type mappings,
/// focusing on float/double special values (NaN, Infinity).
/// </summary>
public class TypeMappingLiteralTests
{
    // --- Float32 (ClickHouseFloatTypeMapping) ---

    [Fact]
    public void Float_NaN_GeneratesCastLiteral()
    {
        var mapping = new ClickHouseFloatTypeMapping();
        var literal = mapping.GenerateSqlLiteral(float.NaN);
        Assert.Equal("CAST('NaN' AS Float32)", literal);
    }

    [Fact]
    public void Float_PositiveInfinity_GeneratesCastLiteral()
    {
        var mapping = new ClickHouseFloatTypeMapping();
        var literal = mapping.GenerateSqlLiteral(float.PositiveInfinity);
        Assert.Equal("CAST('Inf' AS Float32)", literal);
    }

    [Fact]
    public void Float_NegativeInfinity_GeneratesCastLiteral()
    {
        var mapping = new ClickHouseFloatTypeMapping();
        var literal = mapping.GenerateSqlLiteral(float.NegativeInfinity);
        Assert.Equal("CAST('-Inf' AS Float32)", literal);
    }

    [Fact]
    public void Float_NormalValue_GeneratesNumericLiteral()
    {
        var mapping = new ClickHouseFloatTypeMapping();
        var literal = mapping.GenerateSqlLiteral(3.14f);
        Assert.Contains("3.14", literal);
        Assert.DoesNotContain("CAST", literal);
    }

    [Fact]
    public void Float_Zero_GeneratesNumericLiteral()
    {
        var mapping = new ClickHouseFloatTypeMapping();
        var literal = mapping.GenerateSqlLiteral(0.0f);
        Assert.DoesNotContain("CAST", literal);
    }

    [Fact]
    public void Float_Null_GeneratesNullLiteral()
    {
        var mapping = new ClickHouseFloatTypeMapping();
        var literal = mapping.GenerateSqlLiteral(null);
        Assert.Equal("NULL", literal);
    }

    // --- Float64 (ClickHouseDoubleTypeMapping) ---

    [Fact]
    public void Double_NaN_GeneratesCastLiteral()
    {
        var mapping = new ClickHouseDoubleTypeMapping();
        var literal = mapping.GenerateSqlLiteral(double.NaN);
        Assert.Equal("CAST('NaN' AS Float64)", literal);
    }

    [Fact]
    public void Double_PositiveInfinity_GeneratesCastLiteral()
    {
        var mapping = new ClickHouseDoubleTypeMapping();
        var literal = mapping.GenerateSqlLiteral(double.PositiveInfinity);
        Assert.Equal("CAST('Inf' AS Float64)", literal);
    }

    [Fact]
    public void Double_NegativeInfinity_GeneratesCastLiteral()
    {
        var mapping = new ClickHouseDoubleTypeMapping();
        var literal = mapping.GenerateSqlLiteral(double.NegativeInfinity);
        Assert.Equal("CAST('-Inf' AS Float64)", literal);
    }

    [Fact]
    public void Double_NormalValue_GeneratesNumericLiteral()
    {
        var mapping = new ClickHouseDoubleTypeMapping();
        var literal = mapping.GenerateSqlLiteral(2.718281828459045);
        Assert.Contains("2.718281828459045", literal);
        Assert.DoesNotContain("CAST", literal);
    }

    [Fact]
    public void Double_Zero_GeneratesNumericLiteral()
    {
        var mapping = new ClickHouseDoubleTypeMapping();
        var literal = mapping.GenerateSqlLiteral(0.0);
        Assert.DoesNotContain("CAST", literal);
    }

    [Fact]
    public void Double_Null_GeneratesNullLiteral()
    {
        var mapping = new ClickHouseDoubleTypeMapping();
        var literal = mapping.GenerateSqlLiteral(null);
        Assert.Equal("NULL", literal);
    }
}
