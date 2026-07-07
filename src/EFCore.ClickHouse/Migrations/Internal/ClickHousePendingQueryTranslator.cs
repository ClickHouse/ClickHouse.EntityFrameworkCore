using System.Data.Common;
using System.Text.RegularExpressions;
using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using ClickHouse.EntityFrameworkCore.Migrations.Design;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Migrations.Internal;

/// <summary>
/// Translates the LINQ body of a materialized view or projection (a <c>Select(...)</c> lambda,
/// stashed on the model as a pending-query annotation) into the SQL baked into migration DDL.
/// Shared by the migration differ and the snapshot generator so both produce identical SQL.
/// <para>
/// The query is translated with a throwaway <see cref="DbContext"/> built over the finalized model,
/// then any captured-variable parameters are inlined as SQL literals — ClickHouse does not accept
/// query parameters inside a <c>CREATE MATERIALIZED VIEW</c> / <c>ADD PROJECTION</c> definition, so a
/// placeholder left in the text would fail at <c>database update</c>.
/// </para>
/// </summary>
internal static class ClickHousePendingQueryTranslator
{
    public static DbContext CreateTranslationContext(IReadOnlyModel model)
    {
        var options = new DbContextOptionsBuilder()
            .UseClickHouse("Host=localhost;Database=ch-translate")
            .UseModel((IModel)model)
            .Options;

        return new DbContext(options);
    }

    public static string TranslateMaterializedViewSql(DbContext context, PendingMaterializedViewQuery query)
        => Translate(context, query.SourceTypes, query.Lambda, "Materialized view");

    public static string TranslateProjectionSql(DbContext context, PendingProjectionQuery query)
        => ProjectionSqlRewriter.Rewrite(Translate(context, query.SourceTypes, query.Lambda, "Projection"));

    private static string Translate(DbContext context, Type[] sourceTypes, Delegate lambda, string objectKind)
    {
        var setMethod = typeof(DbContext).GetMethods()
            .First(m => m.Name == nameof(DbContext.Set)
                && m.IsGenericMethod
                && m.GetGenericArguments().Length == 1
                && m.GetParameters().Length == 0);

        var queryables = new object[sourceTypes.Length];
        for (var i = 0; i < sourceTypes.Length; i++)
            queryables[i] = setMethod.MakeGenericMethod(sourceTypes[i]).Invoke(context, null)!;

        var result = (IQueryable?)lambda.DynamicInvoke(queryables)
            ?? throw new InvalidOperationException($"{objectKind} query lambda returned null.");

        // CreateDbCommand() gives the raw SQL plus fully-materialized, provider-converted parameter
        // values without opening a connection. (Unlike ToQueryString(), it also omits the `-- name='v'`
        // preamble.) For a capture-free query the parameter collection is empty and CommandText is
        // exactly what ToQueryString() used to return.
        using var command = result.CreateDbCommand();
        return InlineParameters(
            command.CommandText, command, context.GetService<IRelationalTypeMappingSource>(), objectKind);
    }

    private static string InlineParameters(
        string sql, DbCommand command, IRelationalTypeMappingSource typeMappingSource, string objectKind)
    {
        if (command.Parameters.Count == 0)
            return sql;

        foreach (DbParameter parameter in command.Parameters)
        {
            var literal = LiteralFor(parameter, typeMappingSource, objectKind);
            // The provider renders a parameter as `{name:StoreType}` (an optional `:StoreType` part);
            // the DbParameter's name is the bare `name`. Replace the whole placeholder with the literal.
            var placeholder = @"\{" + Regex.Escape(parameter.ParameterName) + @"(:[^}]*)?\}";
            sql = Regex.Replace(sql, placeholder, literal.Replace("$", "$$"), RegexOptions.CultureInvariant);
        }

        return sql;
    }

    private static string LiteralFor(
        DbParameter parameter, IRelationalTypeMappingSource typeMappingSource, string objectKind)
    {
        var value = parameter.Value;
        if (value is null || value is DBNull)
            return "NULL";

        var mapping = typeMappingSource.FindMapping(value.GetType())
            ?? throw new InvalidOperationException(
                $"{objectKind} body captures a variable ('{parameter.ParameterName}') of type "
                + $"'{value.GetType().Name}' that cannot be inlined into ClickHouse DDL. Inline a constant "
                + "value or define the body with .FromRaw(...).");

        return mapping.GenerateSqlLiteral(value);
    }
}
