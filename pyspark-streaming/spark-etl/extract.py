from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, explode, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType, MapType, DoubleType

import logging
logger = logging.getLogger("spark-streaming-etl")

def extract_data(df: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Extrae y parsea los mensajes JSON de Kafka.

    Args:
        df: DataFrame con los mensajes de Kafka
        spark: SparkSession activa

    Returns:
        DataFrame estructurado con los datos extraídos
    """
    logger.info("🔍 Extrayendo datos de los mensajes")

    if df.isEmpty():
        logger.info("⚠️ DataFrame vacío en extract_data")
        return df

    try:
        # Parsear el JSON de los mensajes
        parsed_df = df.selectExpr("CAST(message AS STRING)")

        # Log the raw message for debugging
        first_message = parsed_df.select("message").first()
        if first_message:
            logger.info(f"Raw message from Kafka: {first_message['message']}")

        # Definir el esquema para el JSON
        schema = StructType([
            StructField("channelGrouping", StringType(), True),
            StructField("customDimensions", ArrayType(
                StructType([
                    StructField("index", StringType(), True),
                    StructField("value", StringType(), True)
                ])
            ), True),
            StructField("date", StringType(), True),
            StructField("device", StructType([
                StructField("browser", StringType(), True),
                StructField("operatingSystem", StringType(), True),
                StructField("isMobile", BooleanType(), True),
                StructField("deviceCategory", StringType(), True)
            ]), True),
            StructField("fullVisitorId", StringType(), True),  # Using StringType for compatibility, even though it's a number in the JSON
            StructField("geoNetwork", StructType([
                StructField("continent", StringType(), True),
                StructField("subContinent", StringType(), True),
                StructField("country", StringType(), True),
                StructField("region", StringType(), True),
                StructField("city", StringType(), True),
                StructField("networkDomain", StringType(), True)
            ]), True),
            StructField("hits", ArrayType(
                StructType([
                    StructField("hitNumber", StringType(), True),
                    StructField("time", StringType(), True),
                    StructField("hour", StringType(), True),
                    StructField("minute", StringType(), True),
                    StructField("isInteraction", BooleanType(), True),
                    StructField("isEntrance", BooleanType(), True),
                    StructField("isExit", BooleanType(), True),
                    StructField("referer", StringType(), True),
                    StructField("page", StructType([
                        StructField("pagePath", StringType(), True),
                        StructField("hostname", StringType(), True),
                        StructField("pageTitle", StringType(), True),
                        StructField("pagePathLevel1", StringType(), True),
                        StructField("pagePathLevel2", StringType(), True),
                        StructField("pagePathLevel3", StringType(), True),
                        StructField("pagePathLevel4", StringType(), True)
                    ]), True),
                    StructField("transaction", StructType([
                        StructField("currencyCode", StringType(), True)
                    ]), True),
                    StructField("item", StructType([
                        StructField("currencyCode", StringType(), True)
                    ]), True),
                    StructField("appInfo", StructType([
                        StructField("screenName", StringType(), True),
                        StructField("landingScreenName", StringType(), True),
                        StructField("exitScreenName", StringType(), True),
                        StructField("screenDepth", StringType(), True)
                    ]), True),
                    StructField("exceptionInfo", StructType([
                        StructField("isFatal", BooleanType(), True)
                    ]), True),
                    StructField("eventInfo", StructType([
                        StructField("eventCategory", StringType(), True),
                        StructField("eventAction", StringType(), True)
                    ]), True),
                    StructField("product", ArrayType(
                        StructType([
                            StructField("productSKU", StringType(), True),
                            StructField("v2ProductName", StringType(), True),
                            StructField("v2ProductCategory", StringType(), True),
                            StructField("productVariant", StringType(), True),
                            StructField("productBrand", StringType(), True),
                            StructField("productPrice", StringType(), True),
                            StructField("localProductPrice", StringType(), True),
                            StructField("isImpression", BooleanType(), True),
                            StructField("customDimensions", ArrayType(StructType([])), True),
                            StructField("customMetrics", ArrayType(StructType([])), True),
                            StructField("productListName", StringType(), True),
                            StructField("productListPosition", StringType(), True),
                            StructField("productCouponCode", StringType(), True)
                        ])
                    ), True),
                    StructField("promotion", ArrayType(
                        StructType([
                            StructField("promoId", StringType(), True),
                            StructField("promoName", StringType(), True),
                            StructField("promoCreative", StringType(), True),
                            StructField("promoPosition", StringType(), True)
                        ])
                    ), True),
                    StructField("promotionActionInfo", StructType([
                        StructField("promoIsView", BooleanType(), True),
                        StructField("promoIsClick", BooleanType(), True)
                    ]), True),
                    StructField("eCommerceAction", StructType([
                        StructField("action_type", StringType(), True),
                        StructField("step", StringType(), True)
                    ]), True),
                    StructField("experiment", ArrayType(StructType([])), True),
                    StructField("type", StringType(), True),
                    StructField("social", StructType([
                        StructField("socialNetwork", StringType(), True),
                        StructField("hasSocialSourceReferral", StringType(), True),
                        StructField("socialInteractionNetworkAction", StringType(), True)
                    ]), True),
                    StructField("contentGroup", StructType([
                        StructField("contentGroup1", StringType(), True),
                        StructField("contentGroup2", StringType(), True),
                        StructField("contentGroup3", StringType(), True),
                        StructField("contentGroup4", StringType(), True),
                        StructField("contentGroup5", StringType(), True),
                        StructField("previousContentGroup1", StringType(), True),
                        StructField("previousContentGroup2", StringType(), True),
                        StructField("previousContentGroup3", StringType(), True),
                        StructField("previousContentGroup4", StringType(), True),
                        StructField("previousContentGroup5", StringType(), True),
                        StructField("contentGroupUniqueViews2", StringType(), True)
                    ]), True),
                    StructField("customVariables", ArrayType(StructType([])), True),
                    StructField("customDimensions", ArrayType(StructType([])), True),
                    StructField("customMetrics", ArrayType(StructType([])), True),
                    StructField("dataSource", StringType(), True),
                    StructField("publisher_infos", ArrayType(StructType([])), True)
                ])
            ), True),
            StructField("socialEngagementType", StringType(), True),
            StructField("totals", StructType([
                StructField("visits", StringType(), True),
                StructField("hits", StringType(), True),
                StructField("pageviews", StringType(), True),
                StructField("sessionQualityDim", StringType(), True),
                StructField("newVisits", StringType(), True),
                StructField("bounces", StringType(), True),
                StructField("transactionRevenue", StringType(), True)
            ]), True),
            StructField("trafficSource", StructType([
                StructField("campaign", StringType(), True),
                StructField("source", StringType(), True),
                StructField("medium", StringType(), True),
                StructField("keyword", StringType(), True),
                StructField("adwordsClickInfo", StructType([]), True)
            ]), True),
            StructField("visitId", StringType(), True),
            StructField("visitNumber", IntegerType(), True),
            StructField("visitStartTime", StringType(), True)
        ])

        # Aplicar el esquema al JSON
        extracted_df = parsed_df.select(
            from_json(col("message"), schema).alias("data")
        ).select("data.*")

        # Log the schema of the extracted DataFrame
        logger.info("Schema of extracted DataFrame:")
        extracted_df.printSchema()

        # Check if geoNetwork exists in the extracted DataFrame
        if "geoNetwork" in extracted_df.columns:
            logger.info("geoNetwork column exists in extracted DataFrame")

            # Log a sample of the geoNetwork data
            sample_geo = extracted_df.select("geoNetwork.*").first()
            if sample_geo:
                logger.info(f"Sample geoNetwork data after extraction: {sample_geo}")
                # Print each field individually for debugging
                for field in sample_geo.asDict():
                    logger.info(f"  - {field}: {sample_geo[field]} (type: {type(sample_geo[field])})")
            else:
                logger.warning("No geoNetwork data found in the first row")
        else:
            logger.warning("geoNetwork column does not exist in extracted DataFrame")

        logger.info(f"✅ Extracción completada: {extracted_df.count()} registros")
        return extracted_df

    except Exception as e:
        logger.error(f"💥 Error en extract_data: {e}")
        raise
