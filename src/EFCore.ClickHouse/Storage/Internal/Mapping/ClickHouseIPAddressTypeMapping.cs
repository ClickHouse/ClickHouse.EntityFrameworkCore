using System.Data.Common;
using System.Linq.Expressions;
using System.Net;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

public class ClickHouseIPAddressTypeMapping : RelationalTypeMapping
{
    private static readonly MethodInfo GetValueMethod =
        typeof(DbDataReader).GetRuntimeMethod(nameof(DbDataReader.GetValue), [typeof(int)])!;

    public ClickHouseIPAddressTypeMapping(string storeType = "IPv4")
        : base(storeType, typeof(IPAddress), System.Data.DbType.Object)
    {
    }

    protected ClickHouseIPAddressTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseIPAddressTypeMapping(parameters);

    public override MethodInfo GetDataReaderMethod()
        => GetValueMethod;

    public override Expression CustomizeDataReaderExpression(Expression expression)
        => Expression.Convert(expression, typeof(IPAddress));

    protected override string GenerateNonNullSqlLiteral(object value)
        => $"'{(IPAddress)value}'";
}
