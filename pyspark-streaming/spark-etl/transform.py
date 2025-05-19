#!/usr/bin/env python3
# transform.py - Data transformation module
from pyspark.shell import spark
from pyspark.sql.functions import (
    col, explode, lit, when, regexp_replace,
    to_date, dayofmonth, month, year, date_format,
    dayofweek, expr, coalesce, from_unixtime,
    concat, substring, array_contains, array_max
)
import logging

logger = logging.getLogger("transform")

def clean_value(df, column):
    """Clean values by removing 'not available in demo dataset' strings"""
    return df.withColumn(
        column,
        when(col(column).contains("not available in demo dataset"), None)
        .otherwise(col(column))
    )


def process_dim_date(df):
    """Transform and create date dimension"""
    logger.info("🗓️ Procesando dimensión de fechas...")

    # Convert GA date format (YYYYMMDD) to date
    date_df = df.select(col("date").cast("string").alias("date_str")).distinct()

    # Create date dimension with calculated fields
    date_dim = date_df.select(
        to_date(col("date_str"), "yyyyMMdd").alias("date"),
        dayofmonth(to_date(col("date_str"), "yyyyMMdd")).alias("day"),
        month(to_date(col("date_str"), "yyyyMMdd")).alias("month"),
        year(to_date(col("date_str"), "yyyyMMdd")).alias("year"),
        date_format(to_date(col("date_str"), "yyyyMMdd"), "EEEE").alias("weekday"),
        (dayofweek(to_date(col("date_str"), "yyyyMMdd")).isin(1, 7)).alias("is_weekend")
    )

    return date_dim.withColumn("date_id", expr("uuid()"))


def process_dim_visitor(df):
    """Transform y creación de dimensión visitante con mecanismo para evitar duplicados"""
    logger.info("👤 Procesando dimensión de visitantes...")

    # Asegurar que cada visitante exista solo una vez en la dimensión
    visitor_dim = df.select(
        col("fullVisitorId").alias("visitor_id"),
        coalesce(col("socialEngagementType"), lit("Not Socially Engaged")).alias("social_engagement_type")
    ).distinct()

    # Generar un ID subrogado artificial para cada visitante único
    visitor_dim = visitor_dim.withColumn("visitor_id", expr("uuid()"))

    return visitor_dim


def process_dim_device(df):
    """Transform and create device dimension with UUID keys"""
    logger.info("💻 Procesando dimensión de dispositivos...")

    device_dim = df.select(
        coalesce(col("device.deviceCategory"), lit("unknown")).alias("device_category")
    ).distinct()

    # Generate UUID as binary
    return device_dim.withColumn("device_id", expr("uuid()"))


def process_dim_geo(df):
    logger.info("🌍 Procesando dimensión geográfica...")

    geo_dim = df.select(
        coalesce(col("geoNetwork.continent"), lit("Unknown")).alias("continent"),
        coalesce(col("geoNetwork.country"), lit("Unknown")).alias("country")
    ).distinct()

    return geo_dim.withColumn("geo_id", expr("uuid()"))


def process_dim_channel(df):
    """Transform and create channel dimension"""
    logger.info("🔗 Procesando dimensión de canales...")

    channel_dim = df.select(
        coalesce(col("channelGrouping"), lit("Unknown")).alias("channel_grouping")
    ).distinct()
    return channel_dim.withColumn("channel_id", expr("uuid()"))


def process_dim_traffic_source(df):
    """Transform and create traffic source dimension"""
    logger.info("🚦 Procesando dimensión de tráfico...")

    ts_dim = df.select(
        coalesce(col("trafficSource.source"), lit("Unknown")).alias("source"),
        coalesce(col("trafficSource.medium"), lit("Unknown")).alias("medium")
    ).distinct()
    return ts_dim.withColumn("ts_id", expr("uuid()"))


def process_dim_campaign(df):
    """Transform and create campaign dimension"""
    logger.info("🏷️ Procesando dimensión de campañas...")

    # Start with adding an "Unknown" campaign
    campaign_dim = df.select(
        coalesce(col("trafficSource.campaign"), lit("Unknown")).alias("campaign_name")
    ).distinct()
    return campaign_dim.withColumn("campaign_id", expr("uuid()"))


def process_dim_coupon(df, spark):
    """Transform y creación de dimensión de cupones con ID subrogado para evitar duplicados"""
    logger.info("🎟️ Procesando dimensión de cupones...")

    # Explode hits array para acceder a productos con cupones
    hits_df = df.select(
        col("fullVisitorId"),
        explode(col("hits")).alias("hit")
    )

    # Explode products array para acceder a los cupones de productos
    products_df = hits_df.select(
        col("fullVisitorId"),
        explode(col("hit.product")).alias("product")
    )

    # Extraer códigos de cupón
    coupon_dim = products_df.select(
        col("product.productCouponCode").alias("coupon_code")
    ).filter(col("coupon_code").isNotNull() &
             (~col("coupon_code").contains("not available")) &
             (col("coupon_code") != "(not set)"))

    # Añadir entrada por defecto "sin cupón"
    coupon_dim = coupon_dim.union(
        spark.createDataFrame([("(no coupon)",)], ["coupon_code"])
    ).distinct()

    # Añadir un valor de descuento placeholder
    coupon_dim = coupon_dim.withColumn("discount_value", lit(0.0))

    return coupon_dim


def process_fact_table(df, dim_date, dim_visitor, dim_device,
                       dim_geo, dim_channel, dim_ts, dim_campaign):
    """Create the fact table from transformed dimensions"""
    logger.info("📊 Procesando tabla de hechos...")

    df = df.withColumn("date_str", col("date").cast("string"))

    # Join dimension tables to get IDs
    fact_df = df.join(
        dim_date.select("date_id", "date").withColumnRenamed("date", "dim_date"),
        to_date(col("date_str"), "yyyyMMdd") == col("dim_date"),
        "left"
    ).join(
        dim_visitor.select("visitor_id", "visitor_id"),
        col("visitor_id") == dim_visitor["visitor_id"],
        "left"
    ).join(
        dim_device.select("device_id", "device_category"),
        df["device.deviceCategory"] == dim_device["device_category"],
        "left"
    ).join(
        dim_geo.select("geo_id", "continent", "country"),
        (df["geoNetwork.continent"] == dim_geo["continent"]) &
        (df["geoNetwork.country"] == dim_geo["country"]),
        "left"
    ).join(
        dim_channel.select("channel_id", "channel_grouping"),
        df["channelGrouping"] == dim_channel["channel_grouping"],
        "left"
    ).join(
        dim_ts.select("ts_id", "source", "medium"),
        (df["trafficSource.source"] == dim_ts["source"]) &
        (df["trafficSource.medium"] == dim_ts["medium"]),
        "left"
    ).join(
        dim_campaign.select("campaign_id", "campaign_name"),
        df["trafficSource.campaign"] == dim_campaign["campaign_name"],
        "left"
    )

    # Build fact table with proper type handling
    from pyspark.sql.types import IntegerType, StringType, DoubleType, BooleanType

    fact_table = fact_df.select(
        col("date_id"),
        col("visitor_id").cast(StringType()).alias("visitor_id"),
        col("device_id"),
        col("geo_id"),
        col("channel_id"),
        col("ts_id"),
        col("campaign_id"),
        # Be explicit with types for all columns
        lit(None).cast(StringType()).alias("coupon_code"),
        coalesce(col("visitNumber"), lit(0)).cast(IntegerType()).alias("visit_number"),
        coalesce(col("totals.timeOnSite").cast(IntegerType()), lit(0)).alias("session_duration"),
        coalesce(col("totals.pageviews").cast(IntegerType()), lit(0)).alias("pageviews"),
        coalesce(col("totals.hits").cast(IntegerType()), lit(0)).alias("hits"),
        (coalesce(col("totals.newVisits"), lit("0")) == "1").cast(BooleanType()).alias("new_visit_flag"),
        coalesce(col("totals.sessionQualityDim").cast(IntegerType()), lit(0)).alias("session_quality"),
        # No transaction_revenue in data, defaulting to 0
        lit(0.0).cast(DoubleType()).alias("transaction_revenue")
    )

    return fact_table


def transform_data(df, spark):
    """
    Transform extracted data into dimensional model

    Args:
        df: DataFrame with extracted GA data
        spark: SparkSession object

    Returns:
        Dict of DataFrames for each dimension and fact table
    """
    logger.info("📊 Procesando tabla de hechos...")

    try:
        # Clean the data - remove "not available in demo dataset" values
        cleaned_df = df
        for column in ["socialEngagementType", "channelGrouping"]:
            if column in df.columns:
                cleaned_df = clean_value(cleaned_df, column)

        # Process dimension tables
        dim_date = process_dim_date(cleaned_df)
        dim_visitor = process_dim_visitor(cleaned_df)
        dim_device = process_dim_device(cleaned_df)
        dim_geo = process_dim_geo(cleaned_df)
        dim_channel = process_dim_channel(cleaned_df)
        dim_ts = process_dim_traffic_source(cleaned_df)
        dim_campaign = process_dim_campaign(cleaned_df)
        dim_coupon = process_dim_coupon(cleaned_df, spark)

        # Process fact table
        fact_table = process_fact_table(
            cleaned_df, dim_date, dim_visitor, dim_device,
            dim_geo, dim_channel, dim_ts, dim_campaign
        )

        # Return all transformed tables
        transformed_data = {
            "dim_date": dim_date,
            "dim_visitor": dim_visitor,
            "dim_device": dim_device,
            "dim_geo": dim_geo,
            "dim_channel": dim_channel,
            "dim_traffic_source": dim_ts,
            "dim_campaign": dim_campaign,
            "dim_coupon": dim_coupon,
            "visit_sales_fact": fact_table
        }

        logger.info("✅ Transformación completada")
        return transformed_data

    except Exception as e:
        logger.error("💥 Error transformación: {e}")

        raise