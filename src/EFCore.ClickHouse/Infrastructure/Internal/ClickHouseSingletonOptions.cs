using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace ClickHouse.EntityFrameworkCore.Infrastructure.Internal;

public class ClickHouseSingletonOptions : IClickHouseSingletonOptions
{
    public ParameterTranslationMode? ExplicitParameterizedCollectionMode { get; private set; }

    public void Initialize(IDbContextOptions options)
    {
        ExplicitParameterizedCollectionMode =
            options.FindExtension<ClickHouseOptionsExtension>()?.ExplicitParameterizedCollectionMode;
    }

    public void Validate(IDbContextOptions options)
    {
        var extension = options.FindExtension<ClickHouseOptionsExtension>();
        if (extension is not null && extension.ExplicitParameterizedCollectionMode != ExplicitParameterizedCollectionMode)
        {
            throw new InvalidOperationException(
                "UseParameterizedCollectionMode has a different value than when the internal service provider was "
                + "created. Use the same value for all contexts that share an internal service provider from "
                + "UseInternalServiceProvider.");
        }
    }
}
