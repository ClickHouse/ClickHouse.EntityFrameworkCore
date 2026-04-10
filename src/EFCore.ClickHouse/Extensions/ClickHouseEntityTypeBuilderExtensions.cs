using ClickHouse.EntityFrameworkCore.Metadata.Builders;
using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace ClickHouse.EntityFrameworkCore.Extensions;

public static class ClickHouseEntityTypeBuilderExtensions
{
    public static ClickHouseMergeTreeEngineBuilder HasMergeTreeEngine(this TableBuilder tableBuilder)
    {
        ArgumentNullException.ThrowIfNull(tableBuilder);
        return new(GetEntityType(tableBuilder));
    }

    public static ClickHouseReplacingMergeTreeEngineBuilder HasReplacingMergeTreeEngine(
        this TableBuilder tableBuilder, string? version = null, string? isDeleted = null)
    {
        ArgumentNullException.ThrowIfNull(tableBuilder);
        return new(GetEntityType(tableBuilder), version, isDeleted);
    }

    public static ClickHouseSummingMergeTreeEngineBuilder HasSummingMergeTreeEngine(
        this TableBuilder tableBuilder, params string[] columns)
    {
        ArgumentNullException.ThrowIfNull(tableBuilder);
        return new(GetEntityType(tableBuilder), columns);
    }

    public static ClickHouseAggregatingMergeTreeEngineBuilder HasAggregatingMergeTreeEngine(
        this TableBuilder tableBuilder)
    {
        ArgumentNullException.ThrowIfNull(tableBuilder);
        return new(GetEntityType(tableBuilder));
    }

    public static ClickHouseCollapsingMergeTreeEngineBuilder HasCollapsingMergeTreeEngine(
        this TableBuilder tableBuilder, string sign)
    {
        ArgumentNullException.ThrowIfNull(tableBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(sign);
        return new(GetEntityType(tableBuilder), sign);
    }

    public static ClickHouseVersionedCollapsingMergeTreeEngineBuilder HasVersionedCollapsingMergeTreeEngine(
        this TableBuilder tableBuilder, string sign, string version)
    {
        ArgumentNullException.ThrowIfNull(tableBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(sign);
        ArgumentException.ThrowIfNullOrWhiteSpace(version);
        return new(GetEntityType(tableBuilder), sign, version);
    }

    public static ClickHouseGraphiteMergeTreeEngineBuilder HasGraphiteMergeTreeEngine(
        this TableBuilder tableBuilder, string configSection)
    {
        ArgumentNullException.ThrowIfNull(tableBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(configSection);
        return new(GetEntityType(tableBuilder), configSection);
    }

    public static ClickHouseSimpleEngineBuilder HasTinyLogEngine(this TableBuilder tableBuilder)
    {
        ArgumentNullException.ThrowIfNull(tableBuilder);
        return new(GetEntityType(tableBuilder), ClickHouseAnnotationNames.TinyLog);
    }

    public static ClickHouseSimpleEngineBuilder HasStripeLogEngine(this TableBuilder tableBuilder)
    {
        ArgumentNullException.ThrowIfNull(tableBuilder);
        return new(GetEntityType(tableBuilder), ClickHouseAnnotationNames.StripeLog);
    }

    public static ClickHouseSimpleEngineBuilder HasLogEngine(this TableBuilder tableBuilder)
    {
        ArgumentNullException.ThrowIfNull(tableBuilder);
        return new(GetEntityType(tableBuilder), ClickHouseAnnotationNames.Log);
    }

    public static ClickHouseSimpleEngineBuilder HasMemoryEngine(this TableBuilder tableBuilder)
    {
        ArgumentNullException.ThrowIfNull(tableBuilder);
        return new(GetEntityType(tableBuilder), ClickHouseAnnotationNames.Memory);
    }

    private static IMutableEntityType GetEntityType(TableBuilder tableBuilder)
        => (IMutableEntityType)tableBuilder.Metadata;
}
