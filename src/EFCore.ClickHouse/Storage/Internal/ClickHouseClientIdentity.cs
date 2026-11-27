using ClickHouse.Driver.ADO;

namespace ClickHouse.EntityFrameworkCore.Storage.Internal;

/// <summary>
/// Tags the ClickHouse client's <c>User-Agent</c> with this provider's identity so that
/// queries it issues are attributable server-side via
/// <see cref="ClickHouseClientSettings.ApplicationInfo"/> (ClickHouse.Driver 1.3.0+).
/// </summary>
internal static class ClickHouseClientIdentity
{
    /// <summary>
    /// Value of the <c>lib</c> User-Agent tag identifying this library.
    /// </summary>
    internal const string LibraryName = "ClickHouse.EntityFrameworkCore";

    /// <summary>
    /// Builds <see cref="ClickHouseClientSettings"/> from a connection string with the
    /// <c>lib</c> User-Agent tag set to <see cref="LibraryName"/>.
    /// </summary>
    internal static ClickHouseClientSettings CreateSettings(string connectionString)
        => new(connectionString)
        {
            ApplicationInfo = new Dictionary<string, string> { ["lib"] = LibraryName },
        };
}
