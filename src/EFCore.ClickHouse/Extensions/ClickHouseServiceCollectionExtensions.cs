using Microsoft.Extensions.DependencyInjection;
using ClickHouse.EntityFrameworkCore.Diagnostics.Internal;
using ClickHouse.EntityFrameworkCore.Infrastructure.Internal;
using ClickHouse.EntityFrameworkCore.Metadata.Conventions;
using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using ClickHouse.EntityFrameworkCore.Query;
using ClickHouse.EntityFrameworkCore.Query.ExpressionTranslators.Internal;
using ClickHouse.EntityFrameworkCore.Query.Internal;
using ClickHouse.EntityFrameworkCore.Storage.Internal;
using ClickHouse.EntityFrameworkCore.Update.Internal;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;

namespace ClickHouse.EntityFrameworkCore.Extensions;

public static class ClickHouseServiceCollectionExtensions
{
    public static IServiceCollection AddEntityFrameworkClickHouse(this IServiceCollection serviceCollection)
    {
        var builder = new EntityFrameworkRelationalServicesBuilder(serviceCollection)
            .TryAdd<LoggingDefinitions, ClickHouseLoggingDefinitions>()
            .TryAdd<IDatabaseProvider, DatabaseProvider<ClickHouseOptionsExtension>>()
            .TryAdd<IRelationalTypeMappingSource, ClickHouseTypeMappingSource>()
            .TryAdd<ISqlGenerationHelper, ClickHouseSqlGenerationHelper>()
            .TryAdd<IRelationalAnnotationProvider, ClickHouseAnnotationProvider>()
            .TryAdd<IModelValidator, ClickHouseModelValidator>()
            .TryAdd<IProviderConventionSetBuilder, ClickHouseConventionSetBuilder>()
            .TryAdd<IUpdateSqlGenerator, ClickHouseUpdateSqlGenerator>()
            .TryAdd<IModificationCommandBatchFactory, ClickHouseModificationCommandBatchFactory>()
            .TryAdd<IRelationalConnection>(p => p.GetRequiredService<IClickHouseRelationalConnection>())
            .TryAdd<IRelationalDatabaseCreator, ClickHouseDatabaseCreator>()
            .TryAdd<IExecutionStrategyFactory, ClickHouseExecutionStrategyFactory>()
            .TryAdd<IQueryableMethodTranslatingExpressionVisitorFactory, ClickHouseQueryableMethodTranslatingExpressionVisitorFactory>()
            .TryAdd<IMethodCallTranslatorProvider, ClickHouseMethodCallTranslatorProvider>()
            .TryAdd<IMemberTranslatorProvider, ClickHouseMemberTranslatorProvider>()
            .TryAdd<IEvaluatableExpressionFilter, ClickHouseEvaluatableExpressionFilter>()
            .TryAdd<IQuerySqlGeneratorFactory, ClickHouseQuerySqlGeneratorFactory>()
            .TryAdd<IRelationalSqlTranslatingExpressionVisitorFactory, ClickHouseSqlTranslatingExpressionVisitorFactory>()
            .TryAdd<IRelationalParameterBasedSqlProcessorFactory, ClickHouseParameterBasedSqlProcessorFactory>()
            .TryAdd<ISqlExpressionFactory, ClickHouseSqlExpressionFactory>()
            .TryAdd<IQueryCompilationContextFactory, ClickHouseQueryCompilationContextFactory>()
            .TryAdd<ISingletonOptions, IClickHouseSingletonOptions>(p => p.GetRequiredService<IClickHouseSingletonOptions>())
            .TryAddProviderSpecificServices(b => b
                .TryAddSingleton<IClickHouseSingletonOptions, ClickHouseSingletonOptions>()
                .TryAddSingleton<ClickHouseDataSourceManager, ClickHouseDataSourceManager>()
                .TryAddScoped<IClickHouseRelationalConnection, ClickHouseRelationalConnection>());

        builder.TryAddCoreServices();

        return serviceCollection;
    }
}
