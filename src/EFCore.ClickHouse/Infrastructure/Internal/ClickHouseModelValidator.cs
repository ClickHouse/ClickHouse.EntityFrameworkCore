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
}
