using System.Text;
using Microsoft.EntityFrameworkCore.Storage;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal;

public class ClickHouseSqlGenerationHelper : RelationalSqlGenerationHelper
{
    private const string ParameterFormat = "{{{0}:{1}}}";

    public ClickHouseSqlGenerationHelper(RelationalSqlGenerationHelperDependencies dependencies)
        : base(dependencies)
    {
    }

    public override string DelimitIdentifier(string identifier)
        => $"`{EscapeIdentifier(identifier)}`";

    public override void DelimitIdentifier(StringBuilder builder, string identifier)
    {
        builder.Append('`');
        EscapeIdentifier(builder, identifier);
        builder.Append('`');
    }

    public override string DelimitIdentifier(string name, string? schema)
        => DelimitIdentifier(name);

    public override void DelimitIdentifier(StringBuilder builder, string name, string? schema)
        => DelimitIdentifier(builder, name);

    public override string EscapeIdentifier(string identifier)
        => identifier.Replace("`", "``");

    public override void EscapeIdentifier(StringBuilder builder, string identifier)
        => builder.Append(identifier.Replace("`", "``"));

    public override string GenerateParameterName(string name)
        => name;

    public override void GenerateParameterName(StringBuilder builder, string name)
        => builder.Append(name);

    public string GenerateParameterName(string name, string storeType)
        => string.Format(ParameterFormat, name, storeType);

    public override string GenerateParameterNamePlaceholder(string name)
        => $"{{{name}}}";

    public override void GenerateParameterNamePlaceholder(StringBuilder builder, string name)
        => builder.Append('{').Append(name).Append('}');

    public void GenerateParameterNamePlaceholder(StringBuilder builder, string name, string storeType)
        => builder.AppendFormat(ParameterFormat, name, storeType);
}
