using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace ClickHouse.EntityFrameworkCore.Infrastructure.Internal;

public interface IClickHouseSingletonOptions : ISingletonOptions
{
    /// <summary>
    /// The mode that the application set with <c>UseParameterizedCollectionMode</c>, or <see langword="null"/>
    /// when it did not set one.
    /// </summary>
    ParameterTranslationMode? ExplicitParameterizedCollectionMode { get; }
}
