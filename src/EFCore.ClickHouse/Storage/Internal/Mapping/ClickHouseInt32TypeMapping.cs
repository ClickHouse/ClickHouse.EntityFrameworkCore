using System.Data.Common;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal.Mapping;

/// <summary>
/// Int32 mapping for ClickHouse. Exists for one specific reason: EF Core's
/// <c>SqlNullabilityProcessor</c> performs a <b>hard cast</b> when expanding a collection
/// parameter into a <c>ValuesExpression</c>:
/// <code>
/// IntTypeMapping intTypeMapping = (IntTypeMapping)TypeMappingSource.FindMapping(typeof(int));
/// </code>
/// The result is used to build a synthetic <c>_ord</c> column that preserves row order
/// within the VALUES clause. If the returned mapping doesn't inherit from
/// <see cref="IntTypeMapping"/>, an <see cref="InvalidCastException"/> is thrown.
///
/// Our shared <c>ClickHouseIntegerTypeMapping</c> handles all integer CLR types
/// (Int8/16/32/64 and UInt8/16/32/64) through a single class and inherits from
/// <see cref="RelationalTypeMapping"/> directly — it can't also inherit from
/// <see cref="IntTypeMapping"/>. This subclass carves out Int32 specifically so the
/// hard-cast path works, while preserving the custom materialization behavior
/// (<c>GetValue()</c> + <c>Convert.ToInt32()</c>) needed to safely handle ClickHouse
/// returning non-Int32 integer types — most notably aggregates like <c>COUNT</c>,
/// which return <c>UInt64</c> even when EF expects <c>int</c>.
/// </summary>
public class ClickHouseInt32TypeMapping : IntTypeMapping
{
    private static readonly MethodInfo GetValueMethod =
        typeof(DbDataReader).GetRuntimeMethod(nameof(DbDataReader.GetValue), [typeof(int)])!;

    private static readonly MethodInfo ConvertToInt32Method =
        typeof(Convert).GetMethod(nameof(Convert.ToInt32), [typeof(object)])!;

    public ClickHouseInt32TypeMapping()
        : base("Int32", System.Data.DbType.Int32)
    {
    }

    protected ClickHouseInt32TypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseInt32TypeMapping(parameters);

    public override MethodInfo GetDataReaderMethod()
        => GetValueMethod;

    public override Expression CustomizeDataReaderExpression(Expression expression)
        => Expression.Call(ConvertToInt32Method, expression);
}
