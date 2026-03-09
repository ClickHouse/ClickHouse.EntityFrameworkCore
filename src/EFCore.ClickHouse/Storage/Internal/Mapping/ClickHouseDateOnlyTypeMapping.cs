using System.Data.Common;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

public class ClickHouseDateOnlyTypeMapping : RelationalTypeMapping
{
    private static readonly MethodInfo GetValueMethod =
        typeof(DbDataReader).GetRuntimeMethod(nameof(DbDataReader.GetValue), [typeof(int)])!;

    private static readonly MethodInfo FromDateTimeMethod =
        typeof(DateOnly).GetMethod(nameof(DateOnly.FromDateTime), [typeof(DateTime)])!;

    public ClickHouseDateOnlyTypeMapping()
        : base("Date32", typeof(DateOnly), System.Data.DbType.Date)
    {
    }

    protected ClickHouseDateOnlyTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseDateOnlyTypeMapping(parameters);

    // The ClickHouse driver returns DateTime for Date columns, not DateOnly.
    // Use GetValue() + DateOnly.FromDateTime() to convert.
    public override MethodInfo GetDataReaderMethod()
        => GetValueMethod;

    public override Expression CustomizeDataReaderExpression(Expression expression)
        => Expression.Call(FromDateTimeMethod, Expression.Convert(expression, typeof(DateTime)));

    protected override string GenerateNonNullSqlLiteral(object value)
        => $"'{((DateOnly)value).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)}'";
}
