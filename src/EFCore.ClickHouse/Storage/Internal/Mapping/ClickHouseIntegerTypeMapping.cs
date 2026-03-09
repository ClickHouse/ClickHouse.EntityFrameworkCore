using System.Data;
using System.Data.Common;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

public class ClickHouseIntegerTypeMapping : RelationalTypeMapping
{
    private static readonly MethodInfo GetValueMethod =
        typeof(DbDataReader).GetRuntimeMethod(nameof(DbDataReader.GetValue), [typeof(int)])!;

    private readonly MethodInfo _convertMethod;

    public ClickHouseIntegerTypeMapping(string storeType, Type clrType, DbType? dbType = null)
        : base(storeType, clrType, dbType)
    {
        _convertMethod = GetConvertMethod(clrType);
    }

    protected ClickHouseIntegerTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
        _convertMethod = GetConvertMethod(parameters.CoreParameters.ClrType);
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseIntegerTypeMapping(parameters);

    // ClickHouse returns integer types that may not match the expected CLR type
    // (e.g. COUNT returns UInt64 but EF expects Int32). Use GetValue + Convert
    // to safely handle the conversion.
    public override MethodInfo GetDataReaderMethod()
        => GetValueMethod;

    public override Expression CustomizeDataReaderExpression(Expression expression)
        => Expression.Call(_convertMethod, expression);

    private static MethodInfo GetConvertMethod(Type clrType)
    {
        var methodName = clrType == typeof(int) ? nameof(Convert.ToInt32)
            : clrType == typeof(long) ? nameof(Convert.ToInt64)
            : clrType == typeof(short) ? nameof(Convert.ToInt16)
            : clrType == typeof(byte) ? nameof(Convert.ToByte)
            : clrType == typeof(sbyte) ? nameof(Convert.ToSByte)
            : clrType == typeof(uint) ? nameof(Convert.ToUInt32)
            : clrType == typeof(ulong) ? nameof(Convert.ToUInt64)
            : clrType == typeof(ushort) ? nameof(Convert.ToUInt16)
            : nameof(Convert.ToInt32);

        return typeof(Convert).GetMethod(methodName, [typeof(object)])!;
    }
}
