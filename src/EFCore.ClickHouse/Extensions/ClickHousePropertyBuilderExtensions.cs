using ClickHouse.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace ClickHouse.EntityFrameworkCore.Extensions;

public static class ClickHousePropertyBuilderExtensions
{
    public static PropertyBuilder HasCodec(this PropertyBuilder propertyBuilder, string codec)
    {
        propertyBuilder.Metadata.SetCodec(codec);
        return propertyBuilder;
    }

    public static PropertyBuilder<TProperty> HasCodec<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder, string codec)
    {
        propertyBuilder.Metadata.SetCodec(codec);
        return propertyBuilder;
    }

    public static PropertyBuilder HasColumnTtl(this PropertyBuilder propertyBuilder, string ttlExpression)
    {
        propertyBuilder.Metadata.SetColumnTtl(ttlExpression);
        return propertyBuilder;
    }

    public static PropertyBuilder<TProperty> HasColumnTtl<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder, string ttlExpression)
    {
        propertyBuilder.Metadata.SetColumnTtl(ttlExpression);
        return propertyBuilder;
    }

    public static PropertyBuilder HasColumnComment(this PropertyBuilder propertyBuilder, string comment)
    {
        propertyBuilder.Metadata.SetColumnComment(comment);
        return propertyBuilder;
    }

    public static PropertyBuilder<TProperty> HasColumnComment<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder, string comment)
    {
        propertyBuilder.Metadata.SetColumnComment(comment);
        return propertyBuilder;
    }
}
