namespace ClickHouse.EntityFrameworkCore.Metadata;

/// <summary>
/// Identifies the interval unit used by <c>EF.Functions.ToStartOfInterval</c>, which maps to the
/// ClickHouse <c>toStartOfInterval(t, INTERVAL n unit)</c> function. Each value corresponds to a
/// ClickHouse <c>toInterval*</c> helper (for example <see cref="Minute"/> emits <c>toIntervalMinute</c>).
/// </summary>
public enum ClickHouseInterval
{
    /// <summary>Second interval (<c>toIntervalSecond</c>).</summary>
    Second,

    /// <summary>Minute interval (<c>toIntervalMinute</c>).</summary>
    Minute,

    /// <summary>Hour interval (<c>toIntervalHour</c>).</summary>
    Hour,

    /// <summary>Day interval (<c>toIntervalDay</c>).</summary>
    Day,

    /// <summary>Week interval (<c>toIntervalWeek</c>).</summary>
    Week,

    /// <summary>Month interval (<c>toIntervalMonth</c>).</summary>
    Month,

    /// <summary>Quarter interval (<c>toIntervalQuarter</c>).</summary>
    Quarter,

    /// <summary>Year interval (<c>toIntervalYear</c>).</summary>
    Year
}
