using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace ClickHouse.EntityFrameworkCore.Extensions;

/// <summary>
/// A resolved materialized view definition extracted from model annotations. <see cref="TargetTable"/>
/// / <see cref="TargetSchema"/> are the physical name the <c>TO</c> clause resolves to for the model
/// this definition came from; they are the identity used to detect a target-table rename.
/// </summary>
public sealed record MaterializedViewDefinition(
    string Name,
    string TargetTypeName,
    string? TargetTable,
    string? TargetSchema,
    string? SelectSql,
    bool Populate,
    string? Cluster,
    string? Database);

/// <summary>
/// A resolved dictionary definition extracted from model annotations. Columns are resolved later
/// (at migration-differ time) from <see cref="DictTypeName"/>; the source table name is resolved
/// from <see cref="SourceTypeName"/>.
/// </summary>
public sealed record DictionaryDefinition(
    string Name,
    string DictTypeName,
    string? SourceTypeName,
    string? SourceTable,
    string? SourceSchema,
    string? SourceNamedCollection,
    string? SourceHost,
    int? SourcePort,
    string? SourceUser,
    string? SourcePassword,
    IReadOnlyList<string> ColumnNames,
    IReadOnlyList<string> ColumnClrTypes,
    IReadOnlyList<string> KeyColumns,
    string Layout,
    string? LayoutParams,
    int? LifetimeMin,
    int? LifetimeMax,
    string? Cluster,
    string? Database);

public static class ClickHouseModelExtensions
{
    // Resolves the physical table (name + schema) an assembly-qualified entity type maps to in this
    // model. Returns (null, null) when the type or its entity/table can't be resolved (e.g. a
    // snapshot referencing a type not loadable here) — callers fall back to the type-name identity.
    // Type.GetType ignores the assembly version for non-strong-named assemblies, so a stored
    // AssemblyQualifiedName whose Version differs from the loaded assembly still resolves.
    private static (string? Table, string? Schema) ResolveTable(IReadOnlyModel model, string? typeName)
    {
        if (string.IsNullOrEmpty(typeName))
            return (null, null);

        var type = Type.GetType(typeName, throwOnError: false);
        if (type is null)
            return (null, null);

        var entity = model.FindEntityType(type);
        return entity is null ? (null, null) : (entity.GetTableName(), entity.GetSchema());
    }

    public static IReadOnlyList<MaterializedViewDefinition> GetMaterializedViews(this IReadOnlyModel model)
    {
        var byName = new Dictionary<string, Dictionary<string, object?>>(StringComparer.Ordinal);

        foreach (var annotation in model.GetAnnotations())
        {
            if (!annotation.Name.StartsWith(ClickHouseAnnotationNames.MaterializedViewPrefix, StringComparison.Ordinal))
                continue;

            var rest = annotation.Name[ClickHouseAnnotationNames.MaterializedViewPrefix.Length..];
            var colonIdx = rest.IndexOf(':');
            if (colonIdx <= 0)
                continue;

            var viewName = rest[..colonIdx];
            var suffix = rest[(colonIdx + 1)..];

            if (suffix == ClickHouseAnnotationNames.MaterializedViewPendingLambdaSuffix)
                continue;

            if (!byName.TryGetValue(viewName, out var props))
            {
                props = new Dictionary<string, object?>(StringComparer.Ordinal);
                byName[viewName] = props;
            }

            props[suffix] = annotation.Value;
        }

        var result = new List<MaterializedViewDefinition>(byName.Count);
        foreach (var (name, props) in byName)
        {
            var targetType = props.GetValueOrDefault(ClickHouseAnnotationNames.MaterializedViewTargetTypeSuffix) as string;
            if (string.IsNullOrEmpty(targetType))
                continue;

            var (targetTable, targetSchema) = ResolveTable(model, targetType);

            result.Add(new MaterializedViewDefinition(
                Name: name,
                TargetTypeName: targetType,
                TargetTable: targetTable,
                TargetSchema: targetSchema,
                SelectSql: props.GetValueOrDefault(ClickHouseAnnotationNames.MaterializedViewSelectSqlSuffix) as string,
                Populate: props.GetValueOrDefault(ClickHouseAnnotationNames.MaterializedViewPopulateSuffix) is true,
                Cluster: props.GetValueOrDefault(ClickHouseAnnotationNames.MaterializedViewClusterSuffix) as string,
                Database: props.GetValueOrDefault(ClickHouseAnnotationNames.MaterializedViewDatabaseSuffix) as string));
        }

        return result;
    }

    public static void SetMaterializedView(this IMutableModel model, MaterializedViewDefinition definition)
    {
        var prefix = ClickHouseAnnotationNames.MaterializedViewPrefix + definition.Name + ":";

        model.SetOrRemoveAnnotation(prefix + ClickHouseAnnotationNames.MaterializedViewTargetTypeSuffix, definition.TargetTypeName);
        model.SetOrRemoveAnnotation(prefix + ClickHouseAnnotationNames.MaterializedViewSelectSqlSuffix, definition.SelectSql);
        model.SetOrRemoveAnnotation(prefix + ClickHouseAnnotationNames.MaterializedViewPopulateSuffix, definition.Populate ? (object)true : null);
        model.SetOrRemoveAnnotation(prefix + ClickHouseAnnotationNames.MaterializedViewClusterSuffix, definition.Cluster);
        model.SetOrRemoveAnnotation(prefix + ClickHouseAnnotationNames.MaterializedViewDatabaseSuffix, definition.Database);
    }

    public static void RemoveMaterializedView(this IMutableModel model, string viewName)
    {
        var prefix = ClickHouseAnnotationNames.MaterializedViewPrefix + viewName + ":";

        var toRemove = model.GetAnnotations()
            .Where(a => a.Name.StartsWith(prefix, StringComparison.Ordinal))
            .Select(a => a.Name)
            .ToList();

        foreach (var name in toRemove)
            model.RemoveAnnotation(name);
    }

    public static IReadOnlyList<DictionaryDefinition> GetDictionaries(this IReadOnlyModel model)
    {
        var byName = new Dictionary<string, Dictionary<string, object?>>(StringComparer.Ordinal);

        foreach (var annotation in model.GetAnnotations())
        {
            if (!annotation.Name.StartsWith(ClickHouseAnnotationNames.DictionaryPrefix, StringComparison.Ordinal))
                continue;

            var rest = annotation.Name[ClickHouseAnnotationNames.DictionaryPrefix.Length..];
            var colonIdx = rest.IndexOf(':');
            if (colonIdx <= 0)
                continue;

            var dictName = rest[..colonIdx];
            var suffix = rest[(colonIdx + 1)..];

            if (!byName.TryGetValue(dictName, out var props))
            {
                props = new Dictionary<string, object?>(StringComparer.Ordinal);
                byName[dictName] = props;
            }

            props[suffix] = annotation.Value;
        }

        var result = new List<DictionaryDefinition>(byName.Count);
        foreach (var (name, props) in byName)
        {
            var dictType = props.GetValueOrDefault(ClickHouseAnnotationNames.DictionaryTypeSuffix) as string;
            var layout = props.GetValueOrDefault(ClickHouseAnnotationNames.DictionaryLayoutSuffix) as string;
            if (string.IsNullOrEmpty(dictType) || string.IsNullOrEmpty(layout))
                continue;

            var sourceTypeName = props.GetValueOrDefault(ClickHouseAnnotationNames.DictionarySourceTypeSuffix) as string;
            var (sourceTable, sourceSchema) = ResolveTable(model, sourceTypeName);

            result.Add(new DictionaryDefinition(
                Name: name,
                DictTypeName: dictType,
                SourceTypeName: sourceTypeName,
                SourceTable: sourceTable,
                SourceSchema: sourceSchema,
                SourceNamedCollection: props.GetValueOrDefault(ClickHouseAnnotationNames.DictionarySourceNamedCollectionSuffix) as string,
                SourceHost: props.GetValueOrDefault(ClickHouseAnnotationNames.DictionarySourceHostSuffix) as string,
                SourcePort: props.GetValueOrDefault(ClickHouseAnnotationNames.DictionarySourcePortSuffix) as int?,
                SourceUser: props.GetValueOrDefault(ClickHouseAnnotationNames.DictionarySourceUserSuffix) as string,
                SourcePassword: props.GetValueOrDefault(ClickHouseAnnotationNames.DictionarySourcePasswordSuffix) as string,
                ColumnNames: props.GetValueOrDefault(ClickHouseAnnotationNames.DictionaryColumnNamesSuffix) as string[] ?? [],
                ColumnClrTypes: props.GetValueOrDefault(ClickHouseAnnotationNames.DictionaryColumnTypesSuffix) as string[] ?? [],
                KeyColumns: props.GetValueOrDefault(ClickHouseAnnotationNames.DictionaryKeySuffix) as string[] ?? [],
                Layout: layout,
                LayoutParams: props.GetValueOrDefault(ClickHouseAnnotationNames.DictionaryLayoutParamsSuffix) as string,
                LifetimeMin: props.GetValueOrDefault(ClickHouseAnnotationNames.DictionaryLifetimeMinSuffix) as int?,
                LifetimeMax: props.GetValueOrDefault(ClickHouseAnnotationNames.DictionaryLifetimeMaxSuffix) as int?,
                Cluster: props.GetValueOrDefault(ClickHouseAnnotationNames.DictionaryClusterSuffix) as string,
                Database: props.GetValueOrDefault(ClickHouseAnnotationNames.DictionaryDatabaseSuffix) as string));
        }

        return result;
    }

    public static void RemoveDictionary(this IMutableModel model, string dictionaryName)
    {
        var prefix = ClickHouseAnnotationNames.DictionaryPrefix + dictionaryName + ":";

        var toRemove = model.GetAnnotations()
            .Where(a => a.Name.StartsWith(prefix, StringComparison.Ordinal))
            .Select(a => a.Name)
            .ToList();

        foreach (var name in toRemove)
            model.RemoveAnnotation(name);
    }
}
