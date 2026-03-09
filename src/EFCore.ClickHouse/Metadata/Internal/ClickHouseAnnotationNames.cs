namespace ClickHouse.EntityFrameworkCore.Metadata.Internal;

public static class ClickHouseAnnotationNames
{
    public const string Prefix = "ClickHouse:";
    public const string Engine = Prefix + "Engine";
    public const string OrderBy = Prefix + "OrderBy";
    public const string PartitionBy = Prefix + "PartitionBy";
    public const string PrimaryKey = Prefix + "PrimaryKey";
}
