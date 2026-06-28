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
        ValidateMaterializedViews(model, logger);
    }

    private static void ValidateMaterializedViews(
        IModel model,
        IDiagnosticsLogger<DbLoggerCategory.Model.Validation> logger)
    {
        var views = model.GetMaterializedViews();
        if (views.Count == 0)
            return;

        // Detect pending LINQ lambdas (set by .From<T>().Select(...)) so we don't reject those
        // as "missing SelectSql" — they're translated lazily by the differ.
        var viewsWithPendingLambda = new HashSet<string>(StringComparer.Ordinal);
        foreach (var annotation in model.GetAnnotations())
        {
            if (annotation.Name.StartsWith(ClickHouseAnnotationNames.MaterializedViewPrefix, StringComparison.Ordinal)
                && annotation.Name.EndsWith(":" + ClickHouseAnnotationNames.MaterializedViewPendingLambdaSuffix, StringComparison.Ordinal))
            {
                var rest = annotation.Name[ClickHouseAnnotationNames.MaterializedViewPrefix.Length..];
                var colonIdx = rest.IndexOf(':');
                if (colonIdx > 0)
                    viewsWithPendingLambda.Add(rest[..colonIdx]);
            }
        }

        foreach (var view in views)
        {
            var targetType = Type.GetType(view.TargetTypeName);
            if (targetType is null)
            {
                throw new InvalidOperationException(
                    $"Materialized view '{view.Name}' references target type '{view.TargetTypeName}' that could not be resolved.");
            }

            var entityType = model.FindEntityType(targetType);
            if (entityType is null)
            {
                throw new InvalidOperationException(
                    $"Materialized view '{view.Name}' target type '{targetType.Name}' is not a registered entity in the model. " +
                    "Call modelBuilder.Entity<T>() for the target type before defining the view.");
            }

            if (entityType.GetTableName() is null)
            {
                throw new InvalidOperationException(
                    $"Materialized view '{view.Name}' target entity '{targetType.Name}' is not mapped to a table.");
            }

            if (view.SelectSql is null && !viewsWithPendingLambda.Contains(view.Name))
            {
                throw new InvalidOperationException(
                    $"Materialized view '{view.Name}' has no SELECT body. Call .From<TSource>().Select(...) or .FromRaw(sql) on the builder.");
            }

            var targetEngine = entityType.GetEngine();
            if (targetEngine is ClickHouseAnnotationNames.Memory
                or ClickHouseAnnotationNames.Log
                or ClickHouseAnnotationNames.TinyLog
                or ClickHouseAnnotationNames.StripeLog)
            {
                logger.Logger.Log(
                    LogLevel.Warning,
                    "Materialized view '{ViewName}' targets entity '{EntityType}' which uses the '{Engine}' engine. " +
                    "This engine is unsuitable as a materialized view destination — data may be lost on restart or aggregations may not work as expected.",
                    view.Name,
                    entityType.DisplayName(),
                    targetEngine);
            }
        }
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
