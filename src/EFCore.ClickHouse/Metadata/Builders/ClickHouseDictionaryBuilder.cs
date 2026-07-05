using System.Linq.Expressions;
using System.Reflection;
using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace ClickHouse.EntityFrameworkCore.Metadata.Builders;

/// <summary>
/// Builder returned by <c>ModelBuilder.HasDictionary&lt;TDictionary&gt;(name)</c>. The columns of the
/// dictionary are the public properties of <typeparamref name="TDictionary"/>; configure the source
/// ClickHouse table, primary key, layout, and lifetime here.
/// </summary>
public sealed class ClickHouseDictionaryBuilder<TDictionary> where TDictionary : class
{
    private readonly IMutableModel _model;
    private readonly string _name;

    internal ClickHouseDictionaryBuilder(IMutableModel model, string name)
    {
        _model = model;
        _name = name;
        DictionaryAnnotations.SetDictType(_model, _name, typeof(TDictionary));

        // Capture the columns (every public readable instance property) now, so that changes to the
        // shape are visible to the migration differ — the model snapshot stores only type names,
        // which don't change when a property is added/removed.
        var properties = typeof(TDictionary)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && p.GetIndexParameters().Length == 0)
            .ToList();
        DictionaryAnnotations.SetColumns(
            _model, _name,
            properties.Select(p => p.Name).ToArray(),
            properties.Select(p => p.PropertyType.AssemblyQualifiedName!).ToArray());

        // Sensible default; overridden by Layout(...).
        DictionaryAnnotations.SetLayout(_model, _name, ClickHouseDictionaryLayout.Hashed.ToClickHouseName());
    }

    /// <summary>The ClickHouse table the dictionary reads from (<c>SOURCE(CLICKHOUSE(TABLE …))</c>).</summary>
    public ClickHouseDictionaryBuilder<TDictionary> FromTable<TSource>() where TSource : class
    {
        DictionaryAnnotations.SetSourceType(_model, _name, typeof(TSource));
        return this;
    }

    /// <summary>
    /// Connection settings for the dictionary's <c>SOURCE(CLICKHOUSE(...))</c>. By default no
    /// credentials are emitted and the dictionary loads as the <c>default</c> user with an empty
    /// password — which fails to load on servers where <c>default</c> is password-protected. Set
    /// <paramref name="user"/>/<paramref name="password"/> (and optionally <paramref name="host"/>/
    /// <paramref name="port"/> for a remote source) so the dictionary can authenticate.
    /// <para>The password is written verbatim into the migration DDL. To keep credentials out of
    /// source control, prefer <see cref="FromNamedCollection"/>.</para>
    /// </summary>
    public ClickHouseDictionaryBuilder<TDictionary> WithSourceConnection(
        string? user = null,
        string? password = null,
        string? host = null,
        int? port = null)
    {
        DictionaryAnnotations.SetSourceConnection(_model, _name, host, port, user, password);
        return this;
    }

    /// <summary>
    /// Reference a ClickHouse named collection for the source's connection settings (host/port/user/
    /// password), keeping credentials out of the migration — the DDL emits only
    /// <c>SOURCE(CLICKHOUSE(NAME &lt;name&gt; ...))</c>. The named collection must already exist on the
    /// server (defined under <c>&lt;named_collections&gt;</c> in config, or created out of band).
    /// Any value passed to <see cref="WithSourceConnection"/> overrides the collection's field.
    /// </summary>
    public ClickHouseDictionaryBuilder<TDictionary> FromNamedCollection(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        DictionaryAnnotations.SetSourceNamedCollection(_model, _name, name);
        return this;
    }

    /// <summary>The dictionary's key column(s). Use a single member or an anonymous type for composite keys.</summary>
    public ClickHouseDictionaryBuilder<TDictionary> HasKey(Expression<Func<TDictionary, object?>> keyExpression)
    {
        ArgumentNullException.ThrowIfNull(keyExpression);
        return HasKey(ExtractMemberNames(keyExpression));
    }

    /// <summary>The dictionary's key column name(s).</summary>
    public ClickHouseDictionaryBuilder<TDictionary> HasKey(params string[] keyColumns)
    {
        if (keyColumns is not { Length: > 0 })
            throw new ArgumentException("A dictionary requires at least one key column.", nameof(keyColumns));
        DictionaryAnnotations.SetKey(_model, _name, keyColumns);
        return this;
    }

    public ClickHouseDictionaryBuilder<TDictionary> Layout(ClickHouseDictionaryLayout layout, string? parameters = null)
    {
        DictionaryAnnotations.SetLayout(_model, _name, layout.ToClickHouseName());
        DictionaryAnnotations.SetLayoutParams(_model, _name, parameters);
        return this;
    }

    /// <summary><c>LIFETIME(MAX maxSeconds)</c>.</summary>
    public ClickHouseDictionaryBuilder<TDictionary> Lifetime(int maxSeconds)
        => Lifetime(0, maxSeconds);

    /// <summary><c>LIFETIME(MIN minSeconds MAX maxSeconds)</c>.</summary>
    public ClickHouseDictionaryBuilder<TDictionary> Lifetime(int minSeconds, int maxSeconds)
    {
        DictionaryAnnotations.SetLifetime(_model, _name, minSeconds, maxSeconds);
        return this;
    }

    public ClickHouseDictionaryBuilder<TDictionary> OnCluster(string clusterName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterName);
        DictionaryAnnotations.SetCluster(_model, _name, clusterName);
        return this;
    }

    public ClickHouseDictionaryBuilder<TDictionary> InDatabase(string databaseName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(databaseName);
        DictionaryAnnotations.SetDatabase(_model, _name, databaseName);

        // Keep the queryable view mapping in the same database, so queries target `db`.`dict`.
        (_model.FindEntityType(typeof(TDictionary)) as IMutableEntityType)?.SetViewSchema(databaseName);
        return this;
    }

    private static string[] ExtractMemberNames(Expression<Func<TDictionary, object?>> expression)
    {
        return expression.Body switch
        {
            // x => x.Member   (value types are wrapped in a Convert to object)
            MemberExpression m => [m.Member.Name],
            UnaryExpression { Operand: MemberExpression m } => [m.Member.Name],
            // x => new { x.A, x.B }
            NewExpression n => n.Arguments
                .Select(a => a is UnaryExpression { Operand: MemberExpression um } ? um.Member.Name
                    : a is MemberExpression me ? me.Member.Name
                    : throw new ArgumentException("Dictionary key expression must reference properties.", nameof(expression)))
                .ToArray(),
            _ => throw new ArgumentException("Dictionary key expression must be a property or anonymous type of properties.", nameof(expression)),
        };
    }
}

internal static class DictionaryAnnotations
{
    private static string Prefix(string name) => ClickHouseAnnotationNames.DictionaryPrefix + name + ":";

    public static void SetDictType(IMutableModel model, string name, Type dictType)
        => model.SetOrRemoveAnnotation(Prefix(name) + ClickHouseAnnotationNames.DictionaryTypeSuffix, dictType.AssemblyQualifiedName);

    public static void SetSourceType(IMutableModel model, string name, Type sourceType)
        => model.SetOrRemoveAnnotation(Prefix(name) + ClickHouseAnnotationNames.DictionarySourceTypeSuffix, sourceType.AssemblyQualifiedName);

    public static void SetColumns(IMutableModel model, string name, string[] columnNames, string[] columnClrTypes)
    {
        model.SetOrRemoveAnnotation(Prefix(name) + ClickHouseAnnotationNames.DictionaryColumnNamesSuffix, columnNames);
        model.SetOrRemoveAnnotation(Prefix(name) + ClickHouseAnnotationNames.DictionaryColumnTypesSuffix, columnClrTypes);
    }

    public static void SetSourceNamedCollection(IMutableModel model, string name, string? collection)
        => model.SetOrRemoveAnnotation(Prefix(name) + ClickHouseAnnotationNames.DictionarySourceNamedCollectionSuffix, collection);

    public static void SetSourceConnection(
        IMutableModel model, string name, string? host, int? port, string? user, string? password)
    {
        model.SetOrRemoveAnnotation(Prefix(name) + ClickHouseAnnotationNames.DictionarySourceHostSuffix, host);
        model.SetOrRemoveAnnotation(Prefix(name) + ClickHouseAnnotationNames.DictionarySourcePortSuffix, port);
        model.SetOrRemoveAnnotation(Prefix(name) + ClickHouseAnnotationNames.DictionarySourceUserSuffix, user);
        model.SetOrRemoveAnnotation(Prefix(name) + ClickHouseAnnotationNames.DictionarySourcePasswordSuffix, password);
    }

    public static void SetKey(IMutableModel model, string name, string[] keyColumns)
        => model.SetOrRemoveAnnotation(Prefix(name) + ClickHouseAnnotationNames.DictionaryKeySuffix, keyColumns);

    public static void SetLayout(IMutableModel model, string name, string layout)
        => model.SetOrRemoveAnnotation(Prefix(name) + ClickHouseAnnotationNames.DictionaryLayoutSuffix, layout);

    public static void SetLayoutParams(IMutableModel model, string name, string? parameters)
        => model.SetOrRemoveAnnotation(Prefix(name) + ClickHouseAnnotationNames.DictionaryLayoutParamsSuffix, parameters);

    public static void SetLifetime(IMutableModel model, string name, int min, int max)
    {
        model.SetOrRemoveAnnotation(Prefix(name) + ClickHouseAnnotationNames.DictionaryLifetimeMinSuffix, min);
        model.SetOrRemoveAnnotation(Prefix(name) + ClickHouseAnnotationNames.DictionaryLifetimeMaxSuffix, max);
    }

    public static void SetCluster(IMutableModel model, string name, string clusterName)
        => model.SetOrRemoveAnnotation(Prefix(name) + ClickHouseAnnotationNames.DictionaryClusterSuffix, clusterName);

    public static void SetDatabase(IMutableModel model, string name, string databaseName)
        => model.SetOrRemoveAnnotation(Prefix(name) + ClickHouseAnnotationNames.DictionaryDatabaseSuffix, databaseName);
}
