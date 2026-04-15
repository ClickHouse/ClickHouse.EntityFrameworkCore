using ClickHouse.EntityFrameworkCore.Extensions;
using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Logging;

namespace ClickHouse.EntityFrameworkCore.Infrastructure.Internal;

public class ClickHouseModelValidator : RelationalModelValidator
{
    public ClickHouseModelValidator(
        ModelValidatorDependencies dependencies,
        RelationalModelValidatorDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
    }

    public override void Validate(IModel model, IDiagnosticsLogger<DbLoggerCategory.Model.Validation> logger)
    {
        base.Validate(model, logger);

        ValidateNoForeignKeys(model, logger);
        ValidateEngineConfiguration(model, logger);
    }

    private static void ValidateNoForeignKeys(
        IModel model,
        IDiagnosticsLogger<DbLoggerCategory.Model.Validation> logger)
    {
        foreach (var entityType in model.GetEntityTypes())
        {
            var foreignKeys = entityType.GetForeignKeys().ToList();
            if (foreignKeys.Count > 0)
            {
                foreach (var fk in foreignKeys)
                {
                    logger.Logger.Log(
                        LogLevel.Warning,
                        "Entity type '{EntityType}' has a foreign key to '{PrincipalType}'. " +
                        "ClickHouse does not support foreign key constraints. " +
                        "The navigation will work in LINQ but has no database enforcement.",
                        entityType.DisplayName(),
                        fk.PrincipalEntityType.DisplayName());
                }
            }
        }
    }

    private static void ValidateEngineConfiguration(
        IModel model,
        IDiagnosticsLogger<DbLoggerCategory.Model.Validation> logger)
    {
        foreach (var entityType in model.GetEntityTypes())
        {
            if (entityType.IsOwned() || entityType.GetTableName() is null)
                continue;

            var engine = entityType.GetEngine();
            if (engine is null)
                continue;

            // Log engines should not have ORDER BY/PARTITION BY
            if (engine is ClickHouseAnnotationNames.TinyLog
                or ClickHouseAnnotationNames.StripeLog
                or ClickHouseAnnotationNames.Log
                or ClickHouseAnnotationNames.Memory)
            {
                if (entityType.GetOrderBy() is not null)
                {
                    logger.Logger.Log(LogLevel.Warning,
                        "Entity type '{EntityType}' uses the '{Engine}' engine which does not support ORDER BY.",
                        entityType.DisplayName(), engine);
                }
            }

            // Validate CollapsingMergeTree sign column: must exist and be Int8
            if (engine is ClickHouseAnnotationNames.CollapsingMergeTree)
            {
                var sign = entityType.GetCollapsingMergeTreeSign();
                ValidateColumnReference(entityType, sign, "CollapsingMergeTree", "sign");
                ValidateColumnStoreType(entityType, sign, "Int8",
                    "CollapsingMergeTree", "sign");
            }

            // Validate VersionedCollapsingMergeTree: sign must be Int8, version must exist
            if (engine is ClickHouseAnnotationNames.VersionedCollapsingMergeTree)
            {
                var sign = entityType.GetVersionedCollapsingMergeTreeSign();
                ValidateColumnReference(entityType, sign, "VersionedCollapsingMergeTree", "sign");
                ValidateColumnStoreType(entityType, sign, "Int8",
                    "VersionedCollapsingMergeTree", "sign");

                ValidateColumnReference(entityType, entityType.GetVersionedCollapsingMergeTreeVersion(),
                    "VersionedCollapsingMergeTree", "version");
            }

            // Validate ReplacingMergeTree: version must exist, isDeleted must be UInt8
            if (engine is ClickHouseAnnotationNames.ReplacingMergeTree)
            {
                ValidateColumnReference(entityType, entityType.GetReplacingMergeTreeVersion(),
                    "ReplacingMergeTree", "version");

                var isDeleted = entityType.GetReplacingMergeTreeIsDeleted();
                ValidateColumnReference(entityType, isDeleted, "ReplacingMergeTree", "isDeleted");
                ValidateColumnStoreType(entityType, isDeleted, "UInt8",
                    "ReplacingMergeTree", "isDeleted");
            }

            // Validate SummingMergeTree columns exist
            if (engine is ClickHouseAnnotationNames.SummingMergeTree)
            {
                var columns = entityType.GetSummingMergeTreeColumns();
                if (columns is not null)
                {
                    foreach (var col in columns)
                    {
                        ValidateColumnReference(entityType, col, "SummingMergeTree", "sum column");
                    }
                }
            }
        }
    }

    private static void ValidateColumnReference(
        IEntityType entityType, string? columnName, string engineName, string parameterName)
    {
        if (columnName is not null && !HasPropertyWithColumn(entityType, columnName))
        {
            throw new InvalidOperationException(
                $"Entity type '{entityType.DisplayName()}' uses {engineName} with {parameterName} column " +
                $"'{columnName}' which does not match any mapped property.");
        }
    }

    private static void ValidateColumnStoreType(
        IEntityType entityType, string? columnName, string requiredStoreType,
        string engineName, string parameterName)
    {
        if (columnName is null)
            return;

        var property = FindPropertyByColumn(entityType, columnName);
        if (property is null)
            return; // existence check is handled by ValidateColumnReference

        var storeType = (property.FindTypeMapping() as RelationalTypeMapping)?.StoreType
            ?? property.GetColumnType();
        if (storeType is not null
            && !storeType.Equals(requiredStoreType, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"Entity type '{entityType.DisplayName()}' uses {engineName} with {parameterName} column " +
                $"'{columnName}' which must have store type {requiredStoreType}, " +
                $"but the resolved type is '{storeType}'.");
        }
    }

    private static IProperty? FindPropertyByColumn(IEntityType entityType, string columnName)
        => entityType.GetProperties().FirstOrDefault(p =>
            string.Equals(p.GetColumnName(), columnName, StringComparison.Ordinal)
            || string.Equals(p.Name, columnName, StringComparison.Ordinal));

    private static bool HasPropertyWithColumn(IEntityType entityType, string columnName)
        => entityType.GetProperties().Any(p =>
            string.Equals(p.GetColumnName(), columnName, StringComparison.Ordinal)
            || string.Equals(p.Name, columnName, StringComparison.Ordinal));
}
