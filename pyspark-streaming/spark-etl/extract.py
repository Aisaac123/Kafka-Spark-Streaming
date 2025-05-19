import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, from_json, struct, to_json
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    BooleanType, ArrayType, LongType, DoubleType
)
import logging

logger = logging.getLogger("extract")


def define_schema():
    """Define the schema for the incoming Google Analytics data"""

    # Define nested schema for device
    device_schema = StructType([
        StructField("deviceCategory", StringType(), True),
        StructField("browser", StringType(), True),
        StructField("operatingSystem", StringType(), True),
        StructField("isMobile", BooleanType(), True)
        # Other fields omitted for simplicity
    ])

    # Define nested schema for geoNetwork
    geo_schema = StructType([
        StructField("continent", StringType(), True),
        StructField("country", StringType(), True),
        StructField("subContinent", StringType(), True),
        StructField("networkDomain", StringType(), True)
        # Other fields omitted for simplicity
    ])

    # Define nested schema for traffic source
    traffic_schema = StructType([
        StructField("source", StringType(), True),
        StructField("medium", StringType(), True),
        StructField("campaign", StringType(), True),
        StructField("keyword", StringType(), True)
        # Other fields omitted for simplicity
    ])

    # Define schema for product
    product_schema = StructType([
        StructField("productSKU", StringType(), True),
        StructField("v2ProductName", StringType(), True),
        StructField("v2ProductCategory", StringType(), True),
        StructField("productPrice", StringType(), True),
        StructField("productQuantity", IntegerType(), True),
        StructField("productCouponCode", StringType(), True),
        # Other fields omitted for simplicity
    ])

    # Define schema for promotion
    promotion_schema = StructType([
        StructField("promoId", StringType(), True),
        StructField("promoName", StringType(), True)
        # Other fields omitted for simplicity
    ])

    # Define schema for hit
    hit_schema = StructType([
        StructField("hitNumber", StringType(), True),
        StructField("time", StringType(), True),
        StructField("hour", StringType(), True),
        StructField("minute", StringType(), True),
        StructField("isInteraction", BooleanType(), True),
        StructField("type", StringType(), True),
        StructField("product", ArrayType(product_schema), True),
        StructField("promotion", ArrayType(promotion_schema), True)
        # Other complex fields omitted for brevity
    ])

    # Main schema for the GA data
    schema = StructType([
        StructField("channelGrouping", StringType(), True),
        StructField("date", IntegerType(), True),
        StructField("fullVisitorId", StringType(), True),
        StructField("visitId", LongType(), True),
        StructField("visitNumber", IntegerType(), True),
        StructField("visitStartTime", LongType(), True),
        StructField("device", device_schema, True),
        StructField("geoNetwork", geo_schema, True),
        StructField("trafficSource", traffic_schema, True),
        StructField("socialEngagementType", StringType(), True),
        StructField("hits", ArrayType(hit_schema), True),
        StructField("totals", StructType([
            StructField("visits", StringType(), True),
            StructField("hits", StringType(), True),
            StructField("pageviews", StringType(), True),
            StructField("timeOnSite", StringType(), True),
            StructField("newVisits", StringType(), True),
            StructField("sessionQualityDim", StringType(), True)
        ]), True)
    ])

    return schema


def extract_data(df, spark: SparkSession):
    """Extrae y parsea JSON de los mensajes Kafka"""
    logger.info("📤 Iniciando extracción")
    if df.isEmpty():
        logger.warning("⚠️ DataFrame vacío, retorno esquema vacío")
        return spark.createDataFrame([], define_schema())

    try:
        schema = define_schema()
        parsed = df.selectExpr("CAST(message AS STRING)") \
                   .select(from_json("message", schema).alias("data")) \
                   .select("data.*")
        filtered = parsed.na.drop(how="all")
        count = filtered.count()
        logger.info(f"✅ Registros extraídos: {count}")
        return filtered
    except Exception as e:
        logger.error(f"💥 Error en extracción: {e}")
        return spark.createDataFrame([], define_schema())