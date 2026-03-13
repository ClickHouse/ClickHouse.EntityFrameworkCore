using System.Net;
using System.Numerics;
using ClickHouse.Driver.Numerics;
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

    // --- BigInteger (ClickHouseBigIntegerTypeMapping) ---

    [Fact]
    public void BigInteger_GeneratesNumericLiteral()
    {
        var mapping = new ClickHouseBigIntegerTypeMapping("Int128");
        var literal = mapping.GenerateSqlLiteral(new BigInteger(123456789));
        Assert.Equal("123456789", literal);
    }

    [Fact]
    public void BigInteger_Negative_GeneratesNumericLiteral()
    {
        var mapping = new ClickHouseBigIntegerTypeMapping("Int256");
        var literal = mapping.GenerateSqlLiteral(new BigInteger(-42));
        Assert.Equal("-42", literal);
    }

    [Fact]
    public void BigInteger_Zero_GeneratesNumericLiteral()
    {
        var mapping = new ClickHouseBigIntegerTypeMapping("UInt128");
        var literal = mapping.GenerateSqlLiteral(BigInteger.Zero);
        Assert.Equal("0", literal);
    }

    [Fact]
    public void BigInteger_Null_GeneratesNullLiteral()
    {
        var mapping = new ClickHouseBigIntegerTypeMapping();
        var literal = mapping.GenerateSqlLiteral(null);
        Assert.Equal("NULL", literal);
    }

    // --- IPAddress (ClickHouseIPAddressTypeMapping) ---

    [Fact]
    public void IPAddress_IPv4_GeneratesQuotedLiteral()
    {
        var mapping = new ClickHouseIPAddressTypeMapping("IPv4");
        var literal = mapping.GenerateSqlLiteral(IPAddress.Parse("127.0.0.1"));
        Assert.Equal("'127.0.0.1'", literal);
    }

    [Fact]
    public void IPAddress_IPv6_GeneratesQuotedLiteral()
    {
        var mapping = new ClickHouseIPAddressTypeMapping("IPv6");
        var literal = mapping.GenerateSqlLiteral(IPAddress.IPv6Loopback);
        Assert.Equal("'::1'", literal);
    }

    [Fact]
    public void IPAddress_Null_GeneratesNullLiteral()
    {
        var mapping = new ClickHouseIPAddressTypeMapping();
        var literal = mapping.GenerateSqlLiteral(null);
        Assert.Equal("NULL", literal);
    }

    // --- BigDecimal (ClickHouseBigDecimalTypeMapping) ---

    [Fact]
    public void BigDecimal_GeneratesDecimalLiteral()
    {
        var mapping = new ClickHouseBigDecimalTypeMapping(38, 18);
        var literal = mapping.GenerateSqlLiteral(new ClickHouseDecimal(123.456m));
        Assert.Equal("123.456", literal);
    }

    [Fact]
    public void BigDecimal_Zero_GeneratesZeroLiteral()
    {
        var mapping = new ClickHouseBigDecimalTypeMapping();
        var literal = mapping.GenerateSqlLiteral(new ClickHouseDecimal(0m));
        Assert.Equal("0", literal);
    }

    [Fact]
    public void BigDecimal_Null_GeneratesNullLiteral()
    {
        var mapping = new ClickHouseBigDecimalTypeMapping();
        var literal = mapping.GenerateSqlLiteral(null);
        Assert.Equal("NULL", literal);
    }

    // --- Array (ClickHouseArrayTypeMapping) ---

    [Fact]
    public void Array_IntArray_GeneratesBracketLiteral()
    {
        var intMapping = new ClickHouseIntegerTypeMapping("Int32", typeof(int), System.Data.DbType.Int32);
        var mapping = new ClickHouseArrayTypeMapping(intMapping);
        var literal = mapping.GenerateSqlLiteral(new[] { 1, 2, 3 });
        Assert.Equal("[1, 2, 3]", literal);
    }

    [Fact]
    public void Array_StringArray_GeneratesBracketLiteral()
    {
        var strMapping = new ClickHouseStringTypeMapping();
        var mapping = new ClickHouseArrayTypeMapping(strMapping);
        var literal = mapping.GenerateSqlLiteral(new[] { "a", "b" });
        Assert.Equal("['a', 'b']", literal);
    }

    [Fact]
    public void Array_Empty_GeneratesEmptyBrackets()
    {
        var intMapping = new ClickHouseIntegerTypeMapping("Int32", typeof(int), System.Data.DbType.Int32);
        var mapping = new ClickHouseArrayTypeMapping(intMapping);
        var literal = mapping.GenerateSqlLiteral(Array.Empty<int>());
        Assert.Equal("[]", literal);
    }

    [Fact]
    public void Array_Null_GeneratesNullLiteral()
    {
        var intMapping = new ClickHouseIntegerTypeMapping("Int32", typeof(int), System.Data.DbType.Int32);
        var mapping = new ClickHouseArrayTypeMapping(intMapping);
        var literal = mapping.GenerateSqlLiteral(null);
        Assert.Equal("NULL", literal);
    }

    // --- Map (ClickHouseMapTypeMapping) ---

    [Fact]
    public void Map_StringInt_GeneratesMapLiteral()
    {
        var strMapping = new ClickHouseStringTypeMapping();
        var intMapping = new ClickHouseIntegerTypeMapping("Int32", typeof(int), System.Data.DbType.Int32);
        var mapping = new ClickHouseMapTypeMapping(strMapping, intMapping);
        // Use a single-entry map to avoid order-dependent assertions
        var dict = new Dictionary<string, int> { ["a"] = 1 };
        var literal = mapping.GenerateSqlLiteral(dict);
        Assert.Equal("map('a', 1)", literal);
    }

    [Fact]
    public void Map_Empty_GeneratesEmptyMapLiteral()
    {
        var strMapping = new ClickHouseStringTypeMapping();
        var intMapping = new ClickHouseIntegerTypeMapping("Int32", typeof(int), System.Data.DbType.Int32);
        var mapping = new ClickHouseMapTypeMapping(strMapping, intMapping);
        var dict = new Dictionary<string, int>();
        var literal = mapping.GenerateSqlLiteral(dict);
        Assert.Equal("map()", literal);
    }

    [Fact]
    public void Map_Null_GeneratesNullLiteral()
    {
        var strMapping = new ClickHouseStringTypeMapping();
        var intMapping = new ClickHouseIntegerTypeMapping("Int32", typeof(int), System.Data.DbType.Int32);
        var mapping = new ClickHouseMapTypeMapping(strMapping, intMapping);
        var literal = mapping.GenerateSqlLiteral(null);
        Assert.Equal("NULL", literal);
    }

    // --- Tuple (ClickHouseTupleTypeMapping) ---

    [Fact]
    public void Tuple_ValueTuple_GeneratesParenLiteral()
    {
        var intMapping = new ClickHouseIntegerTypeMapping("Int32", typeof(int), System.Data.DbType.Int32);
        var strMapping = new ClickHouseStringTypeMapping();
        var mapping = new ClickHouseTupleTypeMapping([intMapping, strMapping], useValueTuple: true);
        var literal = mapping.GenerateSqlLiteral((1, "hello"));
        Assert.Equal("(1, 'hello')", literal);
    }

    [Fact]
    public void Tuple_RefTuple_GeneratesParenLiteral()
    {
        var intMapping = new ClickHouseIntegerTypeMapping("Int32", typeof(int), System.Data.DbType.Int32);
        var strMapping = new ClickHouseStringTypeMapping();
        var mapping = new ClickHouseTupleTypeMapping([intMapping, strMapping], useValueTuple: false);
        var literal = mapping.GenerateSqlLiteral(Tuple.Create(42, "world"));
        Assert.Equal("(42, 'world')", literal);
    }

    [Fact]
    public void Tuple_Null_GeneratesNullLiteral()
    {
        var intMapping = new ClickHouseIntegerTypeMapping("Int32", typeof(int), System.Data.DbType.Int32);
        var strMapping = new ClickHouseStringTypeMapping();
        var mapping = new ClickHouseTupleTypeMapping([intMapping, strMapping]);
        var literal = mapping.GenerateSqlLiteral(null);
        Assert.Equal("NULL", literal);
    }
}
