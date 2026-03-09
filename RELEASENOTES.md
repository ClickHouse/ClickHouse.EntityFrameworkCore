v0.1.0
---
Initial preview release.

* Read-only functionality.
* Limited type support: `String`, `Bool`, `Int8`/`Int16`/`Int32`/`Int64`, `UInt8`/`UInt16`/`UInt32`/`UInt64`, `Float32`/`Float64`, `Decimal(P, S)`, `Date`/`Date32`, `DateTime`, `DateTime64(P, 'TZ')`, `FixedString(N)`, `UUID`.
* Basic aggregations: `Where`, `OrderBy`, `Take`, `Skip`, `Select`, `First`, `Single`, `Any`, `Count`, `Sum`, `Min`, `Max`, `Average`, `Distinct`, `GroupBy`.
* String methods: `Contains`, `StartsWith`, `EndsWith`, `IndexOf`, `Replace`, `Substring`, `Trim`, `ToLower`, `ToUpper`, `Length`.