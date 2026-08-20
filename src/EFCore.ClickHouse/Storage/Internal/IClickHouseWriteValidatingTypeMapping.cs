namespace ClickHouse.EntityFrameworkCore.Storage.Internal;

/// <summary>
/// Implemented by a type mapping whose store type cannot hold every value of its CLR type, so that
/// a value outside the store type's range is reported rather than written wrong.
/// </summary>
/// <remarks>
/// The bulk insert path gives the driver the model values directly and does not consult the type
/// mapping, so a mapping cannot guard its own writes there. The batch calls this instead. ClickHouse
/// stores a <c>DateTime64(P)</c> as an <see cref="long"/> count of 10^-P seconds, which wraps rather
/// than reports when the value does not fit, so the check has to happen on the client.
/// </remarks>
internal interface IClickHouseWriteValidatingTypeMapping
{
    /// <summary>
    /// Throws when <paramref name="value"/> cannot be written to this store type.
    /// </summary>
    /// <param name="value">The model value about to be written. Never <see langword="null"/>.</param>
    /// <param name="columnName">The column being written, for the error message.</param>
    void ValidateWriteValue(object value, string? columnName);
}
