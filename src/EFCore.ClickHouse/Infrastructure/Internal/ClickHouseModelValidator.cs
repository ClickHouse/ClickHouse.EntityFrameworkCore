using ClickHouse.EntityFrameworkCore.Extensions;
using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
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

            // Validate CollapsingMergeTree sign column exists
            if (engine is ClickHouseAnnotationNames.CollapsingMergeTree)
            {
                var sign = entityType.GetCollapsingMergeTreeSign();
                if (sign is not null && !HasPropertyWithColumn(entityType, sign))
                {
                    logger.Logger.Log(LogLevel.Warning,
                        "Entity type '{EntityType}' uses CollapsingMergeTree with sign column '{Sign}' " +
                        "which does not match any property.",
                        entityType.DisplayName(), sign);
                }
            }

            // Validate ReplacingMergeTree version/isDeleted columns exist
            if (engine is ClickHouseAnnotationNames.ReplacingMergeTree)
            {
                var version = entityType.GetReplacingMergeTreeVersion();
                if (version is not null && !HasPropertyWithColumn(entityType, version))
                {
                    logger.Logger.Log(LogLevel.Warning,
                        "Entity type '{EntityType}' uses ReplacingMergeTree with version column '{Version}' " +
                        "which does not match any property.",
                        entityType.DisplayName(), version);
                }

                var isDeleted = entityType.GetReplacingMergeTreeIsDeleted();
                if (isDeleted is not null && !HasPropertyWithColumn(entityType, isDeleted))
                {
                    logger.Logger.Log(LogLevel.Warning,
                        "Entity type '{EntityType}' uses ReplacingMergeTree with isDeleted column '{IsDeleted}' " +
                        "which does not match any property.",
                        entityType.DisplayName(), isDeleted);
                }
            }
        }
    }

    private static bool HasPropertyWithColumn(IEntityType entityType, string columnName)
        => entityType.GetProperties().Any(p =>
            string.Equals(p.GetColumnName(), columnName, StringComparison.Ordinal)
            || string.Equals(p.Name, columnName, StringComparison.Ordinal));
}
