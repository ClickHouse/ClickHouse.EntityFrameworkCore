using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Metadata;

namespace ClickHouse.EntityFrameworkCore.Extensions;

/// <summary>
/// A resolved materialized view definition extracted from model annotations.
/// </summary>
public sealed record MaterializedViewDefinition(
    string Name,
    string TargetTypeName,
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

            result.Add(new MaterializedViewDefinition(
                Name: name,
                TargetTypeName: targetType,
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

            result.Add(new DictionaryDefinition(
                Name: name,
                DictTypeName: dictType,
                SourceTypeName: props.GetValueOrDefault(ClickHouseAnnotationNames.DictionarySourceTypeSuffix) as string,
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
