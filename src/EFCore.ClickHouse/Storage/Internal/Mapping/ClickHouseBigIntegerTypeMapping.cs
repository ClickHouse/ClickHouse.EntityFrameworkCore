using System.Data.Common;
using System.Globalization;
using System.Linq.Expressions;
using System.Numerics;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

public class ClickHouseBigIntegerTypeMapping : RelationalTypeMapping
{
    private static readonly MethodInfo GetValueMethod =
        typeof(DbDataReader).GetRuntimeMethod(nameof(DbDataReader.GetValue), [typeof(int)])!;

    private static readonly MethodInfo ConvertMethod =
        typeof(ClickHouseBigIntegerTypeMapping).GetMethod(nameof(ConvertToBigInteger), BindingFlags.Static | BindingFlags.NonPublic)!;

    public ClickHouseBigIntegerTypeMapping(string storeType = "Int128")
        : base(storeType, typeof(BigInteger), System.Data.DbType.Object)
    {
    }

    protected ClickHouseBigIntegerTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseBigIntegerTypeMapping(parameters);

    public override MethodInfo GetDataReaderMethod()
        => GetValueMethod;

    public override Expression CustomizeDataReaderExpression(Expression expression)
        => Expression.Call(ConvertMethod, expression);

    protected override string GenerateNonNullSqlLiteral(object value)
        => value is BigInteger bi
            ? bi.ToString(CultureInfo.InvariantCulture)
            : Convert.ToString(value, CultureInfo.InvariantCulture)!;

    private static BigInteger ConvertToBigInteger(object value)
        => value switch
        {
            BigInteger bi => bi,
            Int128 i128 => (BigInteger)i128,
            UInt128 u128 => (BigInteger)u128,
            long l => new BigInteger(l),
            ulong ul => new BigInteger(ul),
            int i => new BigInteger(i),
            uint ui => new BigInteger(ui),
            _ => BigInteger.Parse(value.ToString()!)
        };
}
