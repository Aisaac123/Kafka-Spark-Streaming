from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, to_date, dayofmonth, month, year, dayofweek,
    date_format, when, expr, udf, concat, sha2,
    current_timestamp, coalesce, regexp_replace, size, explode, concat_ws
)
from pyspark.sql.types import StringType, IntegerType, BooleanType, DoubleType
import uuid
import logging
from datetime import datetime

logger = logging.getLogger("spark-streaming-etl")

# Función para generar UUIDs consistentes basados en valores de entrada
def generate_uuid(values):
    if values is None:
        return None
    # Concatenar valores y generar un hash SHA-256
    combined = "".join([str(v) for v in values if v is not None])
    return sha2(combined, 256)

# Registrar UDF
generate_uuid_udf = udf(lambda x: str(uuid.uuid4()), StringType())

def transform_data(df: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Transforma los datos extraídos para ajustarse al esquema del data warehouse.
    Implementa deduplicación y asegura consistencia en la estructura.

    Args:
        df: DataFrame con los datos extraídos
        spark: SparkSession activa

    Returns:
        DataFrame transformado listo para cargar en el data warehouse
    """
    logger.info("🔄 Transformando datos")

    if df.isEmpty():
        logger.info("⚠️ DataFrame vacío en transform_data")
        # Devolver un DataFrame vacío pero con la estructura correcta
        return create_empty_dataframe(spark)

    # Print geoNetwork structure if it exists
    if "geoNetwork" in df.columns:
        geo_data = df.select("geoNetwork.*").first()
        if geo_data:
            try:
                continent_value = df.select("geoNetwork.continent").first()

                # Try to show all values of geoNetwork.continent
                continent_values = df.select("geoNetwork.continent").collect()

            except Exception as e:
                logger.error(f"Error accessing geoNetwork.continent in transform_data: {e}")
        else:
            logger.warning("No geoNetwork data found in the first row")

    try:
        # 1. Crear dimensiones
        dim_time_df = create_dim_time(df, spark)
        dim_visitor_df = create_dim_visitor(df)
        dim_device_df = create_dim_device(df)
        dim_geo_df = create_dim_geo(df)
        dim_channel_df = create_dim_channel(df)
        dim_traffic_source_df = create_dim_traffic_source(df)
        dim_product_df = create_dim_product(df)
        dim_promotion_df = create_dim_promotion(df)

        # 2. Crear tabla de hechos
        fact_visits_df = create_fact_visits(
            df, 
            dim_time_df, 
            dim_visitor_df,
            dim_device_df,
            dim_geo_df,
            dim_channel_df,
            dim_traffic_source_df
        )

        # 3. Crear tablas de relación
        fact_visits_products_df = create_fact_visits_products(df, fact_visits_df, dim_product_df)
        fact_visits_promotions_df = create_fact_visits_promotions(df, fact_visits_df, dim_promotion_df)

        # 4. Eliminar duplicados
        fact_visits_df = fact_visits_df.dropDuplicates(["visit_id"])
        fact_visits_products_df = fact_visits_products_df.dropDuplicates(["visit_id", "product_sku"])
        fact_visits_promotions_df = fact_visits_promotions_df.dropDuplicates(["visit_id", "promo_id"])

        # 5. Unir todas las tablas en un único DataFrame
        result = {
            "dim_time": dim_time_df,
            "dim_visitor": dim_visitor_df,
            "dim_device": dim_device_df,
            "dim_geo": dim_geo_df,
            "dim_channel": dim_channel_df,
            "dim_traffic_source": dim_traffic_source_df,
            "dim_product": dim_product_df,
            "dim_promotion": dim_promotion_df,
            "fact_visits": fact_visits_df,
            "fact_visits_products": fact_visits_products_df,
            "fact_visits_promotions": fact_visits_promotions_df
        }

        logger.info(f"✅ Transformación completada")
        return result

    except Exception as e:
        logger.error(f"💥 Error en transform_data: {e}")
        raise

def create_empty_dataframe(spark: SparkSession) -> dict:
    """Crea un DataFrame vacío con la estructura correcta"""
    # Crear dimensiones vacías
    dim_time_schema = "time_id STRING, date INT, day INT, month INT, year INT, quarter INT"
    dim_visitor_schema = "full_visitor_id STRING, visit_number INT, custom_dimensions_value STRING, is_new_visitor BOOLEAN"
    dim_device_schema = "device_id STRING, browser STRING, operating_system STRING, device_category STRING, is_mobile BOOLEAN"
    dim_geo_schema = "geo_id STRING, continent STRING, country STRING, region STRING, city STRING, network_domain STRING"
    dim_channel_schema = "channel_id STRING, channel_grouping STRING"
    dim_traffic_schema = "traffic_source_id STRING, source STRING, medium STRING, campaign STRING"
    dim_product_schema = "product_id STRING, product_sku STRING, v2_product_name STRING, v2_product_category STRING, product_brand STRING, product_variant STRING, product_price DOUBLE"
    dim_promotion_schema = "promo_id STRING, promo_name STRING, promo_creative STRING, promo_position STRING"

    # Crear fact table vacía
    fact_visits_schema = """
        visit_id STRING, time_id STRING, full_visitor_id STRING, device_id STRING, 
        geo_id STRING, channel_id STRING, traffic_source_id STRING, totals_visits INT, 
        social_engagement_type INT, totals_hits INT, totals_pageviews INT, 
        totals_bounces INT, totals_time_on_site INT, totals_transaction_revenue DOUBLE
    """

    # Crear tablas de relación vacías
    fact_visits_products_schema = """
        visit_id STRING, product_id STRING, quantity INT, local_product_price DOUBLE,
        is_impression BOOLEAN, product_list_position INT, product_coupon_code STRING
    """

    fact_visits_promotions_schema = """
        visit_id STRING, promo_id STRING, promo_is_view BOOLEAN, promo_is_click BOOLEAN
    """

    return {
        "dim_time": spark.createDataFrame([], dim_time_schema),
        "dim_visitor": spark.createDataFrame([], dim_visitor_schema),
        "dim_device": spark.createDataFrame([], dim_device_schema),
        "dim_geo": spark.createDataFrame([], dim_geo_schema),
        "dim_channel": spark.createDataFrame([], dim_channel_schema),
        "dim_traffic_source": spark.createDataFrame([], dim_traffic_schema),
        "dim_product": spark.createDataFrame([], dim_product_schema),
        "dim_promotion": spark.createDataFrame([], dim_promotion_schema),
        "fact_visits": spark.createDataFrame([], fact_visits_schema),
        "fact_visits_products": spark.createDataFrame([], fact_visits_products_schema),
        "fact_visits_promotions": spark.createDataFrame([], fact_visits_promotions_schema)
    }

def create_dim_time(df: DataFrame, spark: SparkSession) -> DataFrame:
    """Crea la dimensión de tiempo"""
    # Extraer fechas únicas
    dates_df = df.select(col("date")).distinct()

    # Transformar a formato de fecha
    return dates_df \
        .withColumn("date_int", col("date").cast(IntegerType())) \
        .withColumn("date_str", col("date").cast(StringType())) \
        .withColumn("date_dt", to_date(col("date_str"), "yyyyMMdd")) \
        .withColumn("day", dayofmonth(col("date_dt"))) \
        .withColumn("month", month(col("date_dt"))) \
        .withColumn("year", year(col("date_dt"))) \
        .withColumn("quarter", expr("quarter(date_dt)")) \
        .withColumn("time_id", expr("monotonically_increasing_id() + 1").cast(IntegerType())) \
        .select("time_id", col("date_int").alias("date"), "day", "month", "year", "quarter")

def create_dim_visitor(df: DataFrame) -> DataFrame:
    """Crea la dimensión de visitante"""
    # Extraer el valor de customDimensions si existe
    custom_dim_value = when(
        size(col("customDimensions")) > 0,
        expr("element_at(transform(customDimensions, x -> x.value), 1)")
    ).otherwise(lit(None))

    # Verificar si existe la estructura totals y el campo newVisits
    is_new_visitor_value = lit(False)  # Valor predeterminado

    if "totals" in df.columns:
        # Obtener las columnas disponibles en totals
        totals_columns = df.select("totals.*").columns

        # Determinar el valor de is_new_visitor basado en totals.newVisits
        if "newVisits" in totals_columns:
            # Crear una expresión para evaluar is_new_visitor
            is_new_visitor_value = when(
                df["totals"]["newVisits"].isNotNull() & (df["totals"]["newVisits"] == "1"), 
                lit(True)
            ).otherwise(lit(False))

    # Crear un DataFrame base con los campos básicos y el valor calculado de is_new_visitor
    visitor_df = df.select(
        col("fullVisitorId").cast(StringType()).alias("full_visitor_id"),
        col("visitNumber").alias("visit_number"),
        custom_dim_value.alias("custom_dimensions_value"),
        is_new_visitor_value.alias("is_new_visitor")
    )

    # Generate visitor_id using SHA2 hash of concatenated fields
    return visitor_df.distinct().withColumn(
        "full_visitor_id",
        sha2(concat_ws("|", 
                      col("full_visitor_id"), 
                      coalesce(col("visit_number"), lit("0")), 
                      coalesce(col("custom_dimensions_value"), lit("unknown")), 
                      col("is_new_visitor").cast(StringType())), 
             256)
    )

def create_dim_device(df: DataFrame) -> DataFrame:
    """Crea la dimensión de dispositivo"""
    # Verificar si la estructura device existe
    if "device" not in df.columns:
        # Si no existe, crear un DataFrame con valores predeterminados
        devices = df.select(
            lit("unknown").alias("browser"),
            lit("unknown").alias("operating_system"),
            lit("unknown").alias("device_category"),
            lit(False).alias("is_mobile")
        )
        return devices.distinct().withColumn("device_id", 
                                sha2(concat_ws("|", 
                                              col("browser"), 
                                              col("operating_system"), 
                                              col("device_category"), 
                                              col("is_mobile").cast(StringType())), 
                                     256))

    # Obtener las columnas disponibles en device
    columns = df.select("device.*").columns

    # Crear un DataFrame con todas las combinaciones únicas de dispositivos
    devices_df = df.select(
        coalesce(col("device.browser"), lit("unknown")).alias("browser"),
        coalesce(col("device.operatingSystem"), lit("unknown")).alias("operating_system"),
        coalesce(col("device.deviceCategory"), lit("unknown")).alias("device_category"),
        coalesce(col("device.isMobile"), lit(False)).alias("is_mobile")
    )

    # Agregar el ID y devolver el DataFrame con todas las combinaciones únicas
    return devices_df.distinct().withColumn("device_id",
                                         sha2(concat_ws("|", 
                                                       col("browser"), 
                                                       col("operating_system"), 
                                                       col("device_category"), 
                                                       col("is_mobile").cast(StringType())), 
                                              256))


def create_dim_geo(df: DataFrame) -> DataFrame:
    """Crea la dimensión geográfica"""
    if "geoNetwork" not in df.columns:
        logger.warning("No se encontró la estructura geoNetwork en el DataFrame")
        return create_default_geo(df)

    try:

        from pyspark.sql.functions import from_json, schema_of_json
        import json

        # Verificar si geoNetwork es string (JSON sin parsear)
        if isinstance(df.schema["geoNetwork"].dataType, StringType):

            sample_json = json.dumps({"continent": "Asia", "country": "India", "region": "Delhi",
                                      "city": "Mumbai", "networkDomain": "unknown.unknown"})
            json_schema = schema_of_json(lit(sample_json))

            df = df.withColumn("geoNetwork", from_json(col("geoNetwork"), json_schema))

        # Extract fields directly using dot notation for nested fields
        geo = df.select(
            coalesce(col("geoNetwork.continent"), lit("Unknown")).alias("continent"),
            coalesce(col("geoNetwork.country"), lit("Unknown")).alias("country"),
            coalesce(col("geoNetwork.region"), lit("Unknown")).alias("region"),
            coalesce(col("geoNetwork.city"), lit("Unknown")).alias("city"),
            coalesce(col("geoNetwork.networkDomain"), lit("Unknown")).alias("network_domain")
        ).distinct()

        # Generate geo_id using SHA2 hash of concatenated fields
        geo = geo.withColumn(
            "geo_id", 
            sha2(concat_ws("|", col("continent"), col("country"), col("region"), col("city"), col("network_domain")), 256)
        )

        return geo
    except Exception as e:
        logger.error(f"Error creando dim_geo: {str(e)}")
        return create_default_geo(df)


def create_default_geo(df: DataFrame) -> DataFrame:
    """Crea datos geográficos por defecto"""
    # Create a default geo DataFrame using the SparkSession from the input DataFrame
    spark = df.sparkSession
    default_geo = spark.createDataFrame(
        [("Unknown", "Unknown", "Unknown", "Unknown", "Unknown")],
        ["continent", "country", "region", "city", "network_domain"]
    )

    # Generate geo_id using SHA2 hash of concatenated fields
    return default_geo.withColumn(
        "geo_id", 
        sha2(concat_ws("|", col("continent"), col("country"), col("region"), col("city"), col("network_domain")), 256)
    )

def create_dim_channel(df: DataFrame) -> DataFrame:
    """Crea la dimensión de canal"""
    channel = df.select(
        coalesce(col("channelGrouping"), lit("Unknown")).alias("channel_grouping")
    ).distinct()

    return channel.withColumn("channel_id", 
                             sha2(col("channel_grouping"), 256))

def create_dim_traffic_source(df: DataFrame) -> DataFrame:
    """Crea la dimensión de fuente de tráfico"""
    # Verificar si la estructura trafficSource existe
    if "trafficSource" not in df.columns:
        # Si no existe, crear un DataFrame con valores predeterminados
        traffic = df.select(
            lit("Unknown").alias("source"),
            lit("Unknown").alias("medium"),
            lit("(not set)").alias("campaign")
        )
        return traffic.distinct().withColumn("traffic_source_id", 
                                sha2(concat_ws("|", col("source"), col("medium"), col("campaign")), 256))

    # Obtener las columnas disponibles en trafficSource
    columns = df.select("trafficSource.*").columns

    # Crear un DataFrame con todas las combinaciones únicas de fuentes de tráfico
    traffic_df = df.select(
        coalesce(col("trafficSource.source"), lit("Unknown")).alias("source"),
        coalesce(col("trafficSource.medium"), lit("Unknown")).alias("medium"),
        coalesce(col("trafficSource.campaign"), lit("(not set)")).alias("campaign")
    )

    # Agregar el ID y devolver el DataFrame con todas las combinaciones únicas
    return traffic_df.distinct().withColumn("traffic_source_id", 
                                         sha2(concat_ws("|", col("source"), col("medium"), col("campaign")), 256))

def create_dim_campaign(df: DataFrame) -> DataFrame:
    """Crea la dimensión de campaña"""
    # Crear un DataFrame base con valores predeterminados
    campaign = df.select(lit("(not set)").alias("campaign_name"))

    # Verificar si la estructura trafficSource existe
    if "trafficSource" in df.columns:
        # Obtener las columnas disponibles en trafficSource
        try:
            columns = df.select("trafficSource.*").columns

            # Handle campaign more carefully
            if "campaign" in columns:
                try:
                    campaign = df.select(
                        coalesce(col("trafficSource.campaign"), lit("(not set)")).alias("campaign_name")
                    )
                except Exception as e:
                    logger.warning(f"Could not access trafficSource.campaign: {e}")
                    # Keep the default "(not set)" value already set
        except Exception as e:
            logger.warning(f"Could not access trafficSource structure: {e}")
            # Keep the default "(not set)" value already set

    return campaign.distinct().withColumn("campaign_id", 
                                sha2(col("campaign_name"), 256))

def create_dim_product(df: DataFrame) -> DataFrame:
    """Crea la dimensión de producto"""
    # Verificar si la estructura hits existe
    if "hits" not in df.columns:
        # Si no existe, crear un DataFrame vacío con la estructura correcta
        empty_df = df.select(
            lit("EMPTY_SKU").alias("product_sku"),
            lit("Empty Product").alias("v2_product_name"),
            lit("Empty Category").alias("v2_product_category"),
            lit("(not set)").alias("product_brand"),
            lit("(not set)").alias("product_variant"),
            lit(0.0).alias("product_price")
        ).where(lit(False))  # Crear un DataFrame vacío con la estructura correcta

        # Add product_id column
        return empty_df.withColumn(
            "product_id",
            sha2(concat_ws("|", 
                          col("product_sku"), 
                          col("v2_product_name"), 
                          col("v2_product_category"),
                          col("product_brand"),
                          col("product_variant"),
                          col("product_price").cast(StringType())), 
                 256)
        )

    try:
        # Explotar el array de hits y luego el array de productos
        exploded_hits = df.select(
            col("visitId").cast(IntegerType()).alias("visit_id"),
            explode(col("hits")).alias("hit")
        )

        # Verificar si el campo product existe en los hits
        hit_columns = exploded_hits.select("hit.*").columns
        if "product" not in hit_columns:
            empty_df = df.select(
                lit("EMPTY_SKU").alias("product_sku"),
                lit("Empty Product").alias("v2_product_name"),
                lit("Empty Category").alias("v2_product_category"),
                lit("(not set)").alias("product_brand"),
                lit("(not set)").alias("product_variant"),
                lit(0.0).alias("product_price")
            ).where(lit(False))  # Crear un DataFrame vacío con la estructura correcta

            # Add product_id column
            return empty_df.withColumn(
                "product_id",
                sha2(concat_ws("|", 
                              col("product_sku"), 
                              col("v2_product_name"), 
                              col("v2_product_category"),
                              col("product_brand"),
                              col("product_variant"),
                              col("product_price").cast(StringType())), 
                     256)
            )

        # Explotar los productos dentro de cada hit
        exploded_products = exploded_hits.select(
            col("visit_id"),
            explode(col("hit.product")).alias("product")
        ).filter(col("product").isNotNull())

        # Si no hay productos, devolver un DataFrame vacío
        if exploded_products.isEmpty():
            empty_df = df.select(
                lit("EMPTY_SKU").alias("product_sku"),
                lit("Empty Product").alias("v2_product_name"),
                lit("Empty Category").alias("v2_product_category"),
                lit("(not set)").alias("product_brand"),
                lit("(not set)").alias("product_variant"),
                lit(0.0).alias("product_price")
            ).where(lit(False))  # Crear un DataFrame vacío con la estructura correcta

            # Add product_id column
            return empty_df.withColumn(
                "product_id",
                sha2(concat_ws("|", 
                              col("product_sku"), 
                              col("v2_product_name"), 
                              col("v2_product_category"),
                              col("product_brand"),
                              col("product_variant"),
                              col("product_price").cast(StringType())), 
                     256)
            )

        # Obtener las columnas disponibles en product
        product_columns = exploded_products.select("product.*").columns

        # Crear un DataFrame con todas las combinaciones únicas de productos
        # Usar coalesce para manejar valores nulos y proporcionar valores predeterminados
        products = exploded_products.select(
            coalesce(col("product.productSKU"), lit("UNKNOWN_SKU")).alias("product_sku"),
            coalesce(col("product.v2ProductName"), lit("Unknown Product")).alias("v2_product_name"),
            coalesce(col("product.v2ProductCategory"), lit("Unknown Category")).alias("v2_product_category"),
            coalesce(col("product.productBrand"), lit("(not set)")).alias("product_brand"),
            coalesce(col("product.productVariant"), lit("(not set)")).alias("product_variant"),
            (coalesce(col("product.productPrice").cast(DoubleType()), lit(0.0)) / 1000000).alias("product_price")
        ).distinct()

        # Crear ID único basado en todos los campos
        return products.withColumn(
            "product_id",
            sha2(
                concat_ws("|",
                          col("product_sku"),
                          col("v2_product_name"),
                          col("v2_product_category"),
                          col("product_brand"),
                          col("product_variant"),
                          col("product_price").cast(StringType())
                          ),
                256
            )
        )

    except Exception as e:
        # En caso de error, devolver un DataFrame vacío con la estructura correcta
        return df.select(
            lit("ERROR_SKU").alias("product_sku"),
            lit("Error Product").alias("v2_product_name"),
            lit("Error Category").alias("v2_product_category"),
            lit("(not set)").alias("product_brand"),
            lit("(not set)").alias("product_variant"),
            lit(0.0).alias("product_price")
        ).where(lit(False))

def create_dim_promotion(df: DataFrame) -> DataFrame:
    """Crea la dimensión de promoción"""
    # Verificar si la estructura hits existe
    if "hits" not in df.columns:
        # Si no existe, crear un DataFrame vacío con la estructura correcta
        empty_df = df.select(
            lit("EMPTY_PROMO").alias("original_promo_id"),
            lit("(not set)").alias("promo_name"),
            lit("(not set)").alias("promo_creative"),
            lit("(not set)").alias("promo_position")
        ).where(lit(False))  # Crear un DataFrame vacío con la estructura correcta

        # Generate promo_id using SHA2 hash of concatenated fields
        return empty_df.withColumn(
            "promo_id",
            sha2(concat_ws("|", 
                          col("original_promo_id"), 
                          col("promo_name"), 
                          col("promo_creative"),
                          col("promo_position")), 
                 256)
        )

    try:
        # Explotar el array de hits y luego el array de promociones
        exploded_hits = df.select(
            col("visitId").cast(IntegerType()).alias("visit_id"),
            explode(col("hits")).alias("hit")
        )

        # Verificar si el campo promotion existe en los hits
        hit_columns = exploded_hits.select("hit.*").columns
        if "promotion" not in hit_columns:
            empty_df = df.select(
                lit("EMPTY_PROMO").alias("original_promo_id"),
                lit("(not set)").alias("promo_name"),
                lit("(not set)").alias("promo_creative"),
                lit("(not set)").alias("promo_position")
            ).where(lit(False))  # Crear un DataFrame vacío con la estructura correcta

            # Generate promo_id using SHA2 hash of concatenated fields
            return empty_df.withColumn(
                "promo_id",
                sha2(concat_ws("|", 
                              col("original_promo_id"), 
                              col("promo_name"), 
                              col("promo_creative"),
                              col("promo_position")), 
                     256)
            )

        # Explotar las promociones dentro de cada hit
        exploded_promos = exploded_hits.select(
            col("visit_id"),
            explode(col("hit.promotion")).alias("promotion")
        ).filter(col("promotion").isNotNull())

        # Si no hay promociones, devolver un DataFrame vacío
        if exploded_promos.isEmpty():
            return df.select(
                lit("EMPTY_PROMO").alias("promo_id"),
                lit("(not set)").alias("promo_name"),
                lit("(not set)").alias("promo_creative"),
                lit("(not set)").alias("promo_position")
            ).where(lit(False))  # Crear un DataFrame vacío con la estructura correcta

        # Obtener las columnas disponibles en promotion
        promo_columns = exploded_promos.select("promotion.*").columns

        # Crear un DataFrame con todas las combinaciones únicas de promociones
        # Usar coalesce para manejar valores nulos y proporcionar valores predeterminados
        promotions = exploded_promos.select(
            coalesce(col("promotion.promoId"), lit("UNKNOWN_PROMO")).alias("promo_id"),
            coalesce(col("promotion.promoName"), lit("(not set)")).alias("promo_name"),
            coalesce(col("promotion.promoCreative"), lit("(not set)")).alias("promo_creative"),
            coalesce(col("promotion.promoPosition"), lit("(not set)")).alias("promo_position")
        )

        # Devolver el DataFrame con todas las combinaciones únicas
        return promotions.distinct()
    except Exception as e:
        # En caso de error, devolver un DataFrame vacío con la estructura correcta
        return df.select(
            lit("ERROR_PROMO").alias("promo_id"),
            lit("(not set)").alias("promo_name"),
            lit("(not set)").alias("promo_creative"),
            lit("(not set)").alias("promo_position")
        ).where(lit(False))

def create_fact_visits(df: DataFrame, dim_time_df: DataFrame, dim_visitor_df: DataFrame,
                     dim_device_df: DataFrame, dim_geo_df: DataFrame, 
                     dim_channel_df: DataFrame, dim_traffic_df: DataFrame) -> DataFrame:
    """Crea la tabla de hechos de visitas"""
    # Preparar datos base
    base_df = df.withColumn("visit_id", col("visitId").cast(IntegerType()))

    # Verificar si existe la columna totals
    columns = df.columns
    if "totals" in columns:
        # Obtener los campos disponibles en la estructura totals
        totals_fields = df.select("totals.*").columns

        # Agregar columnas solo si existen en la estructura totals
        if "visits" in totals_fields:
            base_df = base_df.withColumn("totals_visits", 
                       when(col("totals.visits").isNotNull(), col("totals.visits").cast(IntegerType()))
                       .otherwise(lit(0)))
        else:
            base_df = base_df.withColumn("totals_visits", lit(0))

        if "hits" in totals_fields:
            base_df = base_df.withColumn("totals_hits", 
                       when(col("totals.hits").isNotNull(), col("totals.hits").cast(IntegerType()))
                       .otherwise(lit(0)))
        else:
            base_df = base_df.withColumn("totals_hits", lit(0))

        if "pageviews" in totals_fields:
            base_df = base_df.withColumn("totals_pageviews", 
                       when(col("totals.pageviews").isNotNull(), col("totals.pageviews").cast(IntegerType()))
                       .otherwise(lit(0)))
        else:
            base_df = base_df.withColumn("totals_pageviews", lit(0))

        if "bounces" in totals_fields:
            base_df = base_df.withColumn("totals_bounces", 
                       when(col("totals.bounces").isNotNull(), col("totals.bounces").cast(IntegerType()))
                       .otherwise(lit(0)))
        else:
            base_df = base_df.withColumn("totals_bounces", lit(0))

        if "timeOnSite" in totals_fields:
            base_df = base_df.withColumn("totals_time_on_site", 
                       when(col("totals.timeOnSite").isNotNull(), col("totals.timeOnSite").cast(IntegerType()))
                       .otherwise(lit(0)))
        else:
            base_df = base_df.withColumn("totals_time_on_site", lit(0))

        if "transactionRevenue" in totals_fields:
            base_df = base_df.withColumn("totals_transaction_revenue", 
                       when(col("totals.transactionRevenue").isNotNull(), 
                            (col("totals.transactionRevenue").cast(DoubleType()) / 1000000))
                       .otherwise(lit(0.0)))
        else:
            base_df = base_df.withColumn("totals_transaction_revenue", lit(0.0))
    else:
        # Si no existe la estructura totals, agregar columnas con valores predeterminados
        base_df = base_df.withColumn("totals_visits", lit(0)) \
            .withColumn("totals_hits", lit(0)) \
            .withColumn("totals_pageviews", lit(0)) \
            .withColumn("totals_bounces", lit(0)) \
            .withColumn("totals_time_on_site", lit(0)) \
            .withColumn("totals_transaction_revenue", lit(0.0))

    # Convertir socialEngagementType a un valor numérico
    base_df = base_df.withColumn("social_engagement_type", 
                                when(col("socialEngagementType") == "Socially Engaged", 1)
                                .otherwise(0))

    # Unir con dimensiones para obtener claves foráneas
    result = base_df \
        .join(dim_time_df, base_df.date.cast(IntegerType()) == dim_time_df.date, "left") \
        .join(dim_visitor_df, base_df.fullVisitorId.cast(StringType()) == dim_visitor_df.full_visitor_id, "left")

    # Verificar si existe la estructura device
    device_join_condition = lit("unknown") == dim_device_df.device_category

    if "device" in base_df.columns:
        try:
            # Obtener las columnas disponibles en device
            device_columns = base_df.select("device.*").columns

            # Crear condiciones de join solo con las columnas que existen
            if "deviceCategory" in device_columns:
                try:
                    device_join_condition = coalesce(col("device.deviceCategory"), lit("unknown")) == dim_device_df.device_category
                except Exception as e:
                    logger.warning(f"Could not access device.deviceCategory in join: {e}")
                    # Keep the default join condition
            # else: keep the default join condition
        except Exception as e:
            logger.warning(f"Could not access device structure in join: {e}")
            # Keep the default join condition

    # Join with device dimension
    result = result.join(dim_device_df, device_join_condition, "left")

    # Verificar si existe la estructura geoNetwork
    geo_join_condition = (lit("Unknown") == dim_geo_df.continent) & (lit("Unknown") == dim_geo_df.country)

    if "geoNetwork" in base_df.columns:
        try:
            # Log the geoNetwork data before creating the join condition
            logger.info("geoNetwork data in create_fact_visits before join:")
            sample_geo = base_df.select("geoNetwork.*").first()
            if sample_geo:
                for field in sample_geo.asDict():
                    logger.info(f"  - {field}: {sample_geo[field]} (type: {type(sample_geo[field])})")

            # Log the dim_geo_df data before join
            logger.info("dim_geo_df data before join:")
            sample_dim_geo = dim_geo_df.limit(1).collect()
            if sample_dim_geo:
                logger.info(f"Sample dim_geo data: {sample_dim_geo[0]}")

            # Try to access specific fields directly to see if they exist
            try:
                continent_value = base_df.select("geoNetwork.continent").first()
                logger.info(f"Direct access to geoNetwork.continent in create_fact_visits: {continent_value}")
            except Exception as e:
                logger.error(f"Error accessing geoNetwork.continent in create_fact_visits: {e}")

            # Crear una condición de join usando dot notation para acceder a los campos de geoNetwork
            geo_join_condition = (
                coalesce(col("geoNetwork.continent"), lit("Unknown")) == dim_geo_df.continent
            ) & (
                coalesce(col("geoNetwork.country"), lit("Unknown")) == dim_geo_df.country
            )
            logger.info("Using geoNetwork fields for join condition")
        except Exception as e:
            logger.warning(f"Could not access geoNetwork structure in join: {e}")
            # Use default join condition defined at the beginning

    # Show a sample of the data before join
    base_df_sample = base_df.select("geoNetwork.*").limit(1).collect()
    if base_df_sample:
        logger.info(f"Base df geoNetwork: {base_df_sample[0]}")

    dim_geo_sample = dim_geo_df.limit(1).collect()
    if dim_geo_sample:
        logger.info(f"Dim geo: {dim_geo_sample[0]}")

    # Join with geo dimension - use a more explicit join condition
    try:
        # Extract geoNetwork fields to separate columns for easier joining
        base_df_with_geo = base_df
        if "geoNetwork" in base_df.columns:
            base_df_with_geo = base_df.withColumn("continent",
                                                 coalesce(col("geoNetwork.continent"), lit("Unknown")))
            base_df_with_geo = base_df_with_geo.withColumn("country",
                                                          coalesce(col("geoNetwork.country"), lit("Unknown")))


            new_geo_join_condition = (
                col("continent") == dim_geo_df.continent
            ) & (
                col("country") == dim_geo_df.country
            )

            # Add the extracted columns to the result DataFrame
            for column in base_df_with_geo.columns:
                if column not in result.columns and column in ["continent", "country"]:
                    result = result.withColumn(column, base_df_with_geo[column])

            # Join using the new condition
            result = result.join(dim_geo_df, new_geo_join_condition, "left")
        else:
            # Fallback to the original join if geoNetwork doesn't exist
            result = result.join(dim_geo_df, geo_join_condition, "left")
    except Exception as e:
        logger.error(f"Error during geo join: {e}")
        # Fallback to the original join
        result = result.join(dim_geo_df, geo_join_condition, "left")

    # Log the result after join
    logger.info("Result after geo join:")
    sample_result = result.select("geo_id").limit(1).collect()
    if sample_result:
        logger.info(f"Sample result geo_id: {sample_result[0]['geo_id']}")

        # Try to get the corresponding geo record
        if sample_result[0]['geo_id'] is not None:
            geo_record = dim_geo_df.filter(col("geo_id") == sample_result[0]['geo_id']).collect()

        # Check if we got Unknown values
        if result.filter(col("geo_id").isNotNull()).count() > 0:
            # Get the actual geo values for the first row
            geo_values = result.select(
                "geo_id",
                col("geoNetwork.continent").alias("continent"),
                col("geoNetwork.country").alias("country"),
                "continent",
                "country"
            ).limit(1).collect()

            if geo_values:
                logger.info(f"Geo values after join: {geo_values[0]}")

                # Get the actual dim_geo record
                if geo_values[0]["geo_id"] is not None:
                    dim_geo_record = dim_geo_df.filter(col("geo_id") == geo_values[0]["geo_id"]).collect()
                    if dim_geo_record:
                        logger.info(f"Matched dim_geo record: {dim_geo_record[0]}")
        else:
            logger.warning("All geo_id values are NULL after join")

    # Continuar con el resto de joins
    result = result \
        .join(dim_channel_df,
             coalesce(base_df.channelGrouping, lit("Unknown")) == dim_channel_df.channel_grouping, "left")

    # Verificar si existe la estructura trafficSource
    traffic_join_condition = (lit("Unknown") == dim_traffic_df.source) & (lit("Unknown") == dim_traffic_df.medium)

    if "trafficSource" in base_df.columns:
        try:
            # Obtener las columnas disponibles en trafficSource
            traffic_columns = base_df.select("trafficSource.*").columns

            # Crear condiciones de join solo con las columnas que existen
            traffic_join_condition = lit(True)

            if "source" in traffic_columns:
                try:
                    traffic_join_condition = traffic_join_condition & (coalesce(col("trafficSource.source"), lit("Unknown")) == dim_traffic_df.source)
                except Exception as e:
                    logger.warning(f"Could not access trafficSource.source in join: {e}")
                    traffic_join_condition = traffic_join_condition & (lit("Unknown") == dim_traffic_df.source)
            else:
                traffic_join_condition = traffic_join_condition & (lit("Unknown") == dim_traffic_df.source)

            if "medium" in traffic_columns:
                try:
                    traffic_join_condition = traffic_join_condition & (coalesce(col("trafficSource.medium"), lit("Unknown")) == dim_traffic_df.medium)
                except Exception as e:
                    logger.warning(f"Could not access trafficSource.medium in join: {e}")
                    traffic_join_condition = traffic_join_condition & (lit("Unknown") == dim_traffic_df.medium)
            else:
                traffic_join_condition = traffic_join_condition & (lit("Unknown") == dim_traffic_df.medium)
        except Exception as e:
            logger.warning(f"Could not access trafficSource structure in join: {e}")
            # Use default join condition defined at the beginning

    # Join with traffic source dimension
    result = result.join(dim_traffic_df, traffic_join_condition, "left")

    # Log the columns available in the result DataFrame
    logger.info("Columns available in result DataFrame:")
    for column in result.columns:
        logger.info(f"  - {column}")

    # Check if the geo columns exist in the result DataFrame
    has_geo_continent = "continent" in result.columns
    has_geo_country = "country" in result.columns

    if has_geo_continent and has_geo_country:
        logger.info("Geo columns found in result DataFrame")

        # Check if any rows have geo_id NULL but geo_continent/geo_country not NULL
        missing_geo_id_count = result.filter(
            col("geo_id").isNull() &
            col("continent").isNotNull() &
            col("country").isNotNull()
        ).count()

        if missing_geo_id_count > 0:
            logger.warning(f"Found {missing_geo_id_count} rows with NULL geo_id but non-NULL geo_continent/geo_country")

            # Try to fix these rows by looking up the correct geo_id from dim_geo_df
            # First, collect the distinct geo_continent and geo_country values that need fixing
            missing_geo_rows = result.filter(
                col("geo_id").isNull() &
                col("continent").isNotNull() &
                col("country").isNotNull()
            ).select("continent", "country").distinct().collect()

            logger.info(f"Distinct geo values needing fix: {missing_geo_rows}")

            # For each missing geo, find the corresponding geo_id in dim_geo_df
            for row in missing_geo_rows:
                continent = row["continent"]
                country = row["country"]

                # Look up the geo_id in dim_geo_df
                matching_geo = dim_geo_df.filter(
                    (col("continent") == continent) &
                    (col("country") == country)
                ).select("geo_id").first()

                if matching_geo:
                    geo_id = matching_geo["geo_id"]
                    logger.info(f"Found matching geo_id {geo_id} for {continent}/{country}")

                    # Update the result DataFrame with the correct geo_id
                    result = result.withColumn(
                        "geo_id",
                        when(
                            (col("geo_id").isNull()) &
                            (col("continent") == continent) &
                            (col("country") == country),
                            lit(geo_id)
                        ).otherwise(col("geo_id"))
                    )
                else:
                    logger.warning(f"No matching geo_id found for {continent}/{country}")

                    # Create a new entry in dim_geo_df for this geo
                    # Note: This is just for logging, we can't actually modify dim_geo_df here
                    logger.info(f"Would create new dim_geo entry for {continent}/{country}")

                    # For now, use a default geo_id of 1
                    result = result.withColumn(
                        "geo_id",
                        when(
                            (col("geo_id").isNull()) &
                            (col("continent") == continent) &
                            (col("country") == country),
                            lit(1)  # Default geo_id
                        ).otherwise(col("geo_id"))
                    )

            # Log the fix
            logger.info("Applied fix for NULL geo_id values")
    else:
        logger.warning("Geo columns not found in result DataFrame")

    # Seleccionar columnas finales para la tabla de hechos
    return result.select(
        col("visit_id"),
        col("time_id"),
        col("full_visitor_id"),
        col("device_id"),
        col("geo_id"),
        col("channel_id"),
        col("traffic_source_id"),
        col("totals_visits"),
        col("social_engagement_type"),
        col("totals_hits"),
        col("totals_pageviews"),
        col("totals_bounces"),
        col("totals_time_on_site"),
        col("totals_transaction_revenue")
    )

def create_fact_visits_products(df: DataFrame, fact_visits_df: DataFrame, dim_product_df: DataFrame) -> DataFrame:
    """Crea la tabla de relación entre visitas y productos"""
    # Verificar si la estructura hits existe
    if "hits" not in df.columns:
        # Si no existe, crear un DataFrame vacío con la estructura correcta
        return df.select(
            lit(0).cast(IntegerType()).alias("visit_id"),
            lit("EMPTY_SKU").alias("product_sku"),
            lit(0).cast(IntegerType()).alias("quantity"),
            lit(0.0).cast(DoubleType()).alias("local_product_price"),
            lit(False).cast(BooleanType()).alias("is_impression"),
            lit(0).cast(IntegerType()).alias("product_list_position"),
            lit(None).cast(StringType()).alias("product_coupon_code")
        ).where(lit(False))  # Crear un DataFrame vacío con la estructura correcta

    try:
        # Explotar el array de hits y luego el array de productos
        exploded_hits = df.select(
            col("visitId").cast(IntegerType()).alias("visit_id"),
            explode(col("hits")).alias("hit")
        )

        # Verificar si el campo product existe en los hits
        hit_columns = exploded_hits.select("hit.*").columns
        if "product" not in hit_columns:
            return df.select(
                lit(0).cast(IntegerType()).alias("visit_id"),
                lit("EMPTY_SKU").alias("product_sku"),
                lit(0).cast(IntegerType()).alias("quantity"),
                lit(0.0).cast(DoubleType()).alias("local_product_price"),
                lit(False).cast(BooleanType()).alias("is_impression"),
                lit(0).cast(IntegerType()).alias("product_list_position"),
                lit(None).cast(StringType()).alias("product_coupon_code")
            ).where(lit(False))  # Crear un DataFrame vacío con la estructura correcta

        # Explotar los productos dentro de cada hit
        exploded_products = exploded_hits.select(
            col("visit_id"),
            explode(col("hit.product")).alias("product")
        ).filter(col("product").isNotNull())

        # Si no hay productos, devolver un DataFrame vacío
        if exploded_products.isEmpty():
            return df.select(
                lit(0).cast(IntegerType()).alias("visit_id"),
                lit("EMPTY_SKU").alias("product_sku"),
                lit(0).cast(IntegerType()).alias("quantity"),
                lit(0.0).cast(DoubleType()).alias("local_product_price"),
                lit(False).cast(BooleanType()).alias("is_impression"),
                lit(0).cast(IntegerType()).alias("product_list_position"),
                lit(None).cast(StringType()).alias("product_coupon_code")
            ).where(lit(False))  # Crear un DataFrame vacío con la estructura correcta

        # Obtener las columnas disponibles en product
        product_columns = exploded_products.select("product.*").columns

        # Crear un DataFrame base con valores predeterminados
        visit_products = exploded_products.select(col("visit_id"))

        # Agregar columnas solo si existen
        if "productSKU" in product_columns:
            visit_products = visit_products.withColumn("product_sku", col("product.productSKU"))
        else:
            visit_products = visit_products.withColumn("product_sku", lit("UNKNOWN_SKU"))

        # Asumimos cantidad 1 por defecto
        visit_products = visit_products.withColumn("quantity", lit(1))

        if "localProductPrice" in product_columns:
            visit_products = visit_products.withColumn(
                "local_product_price", 
                (col("product.localProductPrice").cast(DoubleType()) / 1000000)
            )
        else:
            visit_products = visit_products.withColumn("local_product_price", lit(0.0))

        if "isImpression" in product_columns:
            visit_products = visit_products.withColumn(
                "is_impression", 
                coalesce(col("product.isImpression"), lit(False))
            )
        else:
            visit_products = visit_products.withColumn("is_impression", lit(False))

        if "productListPosition" in product_columns:
            visit_products = visit_products.withColumn(
                "product_list_position", 
                col("product.productListPosition").cast(IntegerType())
            )
        else:
            visit_products = visit_products.withColumn("product_list_position", lit(0))

        if "productCouponCode" in product_columns:
            visit_products = visit_products.withColumn(
                "product_coupon_code", 
                coalesce(col("product.productCouponCode"), lit(None))
            )
        else:
            visit_products = visit_products.withColumn("product_coupon_code", lit(None))

        # Unir con la tabla de hechos para asegurar que solo incluimos visitas válidas
        return visit_products.join(
            fact_visits_df.select("visit_id"),
            "visit_id",
            "inner"
        ).join(
            dim_product_df.select("product_sku"),
            "product_sku",
            "inner"
        )
    except Exception as e:
        # En caso de error, devolver un DataFrame vacío con la estructura correcta
        return df.select(
            lit(0).cast(IntegerType()).alias("visit_id"),
            lit("ERROR_SKU").alias("product_sku"),
            lit(0).cast(IntegerType()).alias("quantity"),
            lit(0.0).cast(DoubleType()).alias("local_product_price"),
            lit(False).cast(BooleanType()).alias("is_impression"),
            lit(0).cast(IntegerType()).alias("product_list_position"),
            lit(None).cast(StringType()).alias("product_coupon_code")
        ).where(lit(False))

def create_fact_visits_promotions(df: DataFrame, fact_visits_df: DataFrame, dim_promotion_df: DataFrame) -> DataFrame:
    """Crea la tabla de relación entre visitas y promociones"""
    # Verificar si la estructura hits existe
    if "hits" not in df.columns:
        # Si no existe, crear un DataFrame vacío con la estructura correcta
        return df.select(
            lit(0).cast(IntegerType()).alias("visit_id"),
            lit("EMPTY_PROMO").alias("promo_id"),
            lit(False).cast(BooleanType()).alias("promo_is_view"),
            lit(False).cast(BooleanType()).alias("promo_is_click")
        ).where(lit(False))  # Crear un DataFrame vacío con la estructura correcta

    try:
        # Explotar el array de hits y luego el array de promociones
        exploded_hits = df.select(
            col("visitId").cast(IntegerType()).alias("visit_id"),
            explode(col("hits")).alias("hit")
        )

        # Verificar si el campo promotion existe en los hits
        hit_columns = exploded_hits.select("hit.*").columns
        if "promotion" not in hit_columns:
            return df.select(
                lit(0).cast(IntegerType()).alias("visit_id"),
                lit("EMPTY_PROMO").alias("promo_id"),
                lit(False).cast(BooleanType()).alias("promo_is_view"),
                lit(False).cast(BooleanType()).alias("promo_is_click")
            ).where(lit(False))  # Crear un DataFrame vacío con la estructura correcta

        # Verificar si existe el campo promotionActionInfo
        has_promo_action = "promotionActionInfo" in hit_columns

        # Explotar las promociones dentro de cada hit
        if has_promo_action:
            exploded_promos = exploded_hits.select(
                col("visit_id"),
                explode(col("hit.promotion")).alias("promotion"),
                col("hit.promotionActionInfo").alias("promo_action")
            ).filter(col("promotion").isNotNull())
        else:
            exploded_promos = exploded_hits.select(
                col("visit_id"),
                explode(col("hit.promotion")).alias("promotion")
            ).filter(col("promotion").isNotNull())

        # Si no hay promociones, devolver un DataFrame vacío
        if exploded_promos.isEmpty():
            return df.select(
                lit(0).cast(IntegerType()).alias("visit_id"),
                lit("EMPTY_PROMO").alias("promo_id"),
                lit(False).cast(BooleanType()).alias("promo_is_view"),
                lit(False).cast(BooleanType()).alias("promo_is_click")
            ).where(lit(False))  # Crear un DataFrame vacío con la estructura correcta

        # Obtener las columnas disponibles en promotion
        promo_columns = exploded_promos.select("promotion.*").columns

        # Crear un DataFrame base con valores predeterminados
        visit_promos = exploded_promos.select(col("visit_id"))

        # Agregar columnas solo si existen
        if "promoId" in promo_columns:
            visit_promos = visit_promos.withColumn("promo_id", col("promotion.promoId"))
        else:
            visit_promos = visit_promos.withColumn("promo_id", lit("UNKNOWN_PROMO"))

        # Agregar campos de acción de promoción si existe promotionActionInfo
        if has_promo_action:
            # Obtener las columnas disponibles en promo_action
            promo_action_columns = exploded_promos.select("promo_action.*").columns if has_promo_action else []

            if "promoIsView" in promo_action_columns:
                visit_promos = visit_promos.withColumn(
                    "promo_is_view", 
                    coalesce(col("promo_action.promoIsView"), lit(False))
                )
            else:
                visit_promos = visit_promos.withColumn("promo_is_view", lit(False))

            if "promoIsClick" in promo_action_columns:
                visit_promos = visit_promos.withColumn(
                    "promo_is_click", 
                    coalesce(col("promo_action.promoIsClick"), lit(False))
                )
            else:
                visit_promos = visit_promos.withColumn("promo_is_click", lit(False))
        else:
            # Si no existe promotionActionInfo, usar valores predeterminados
            visit_promos = visit_promos.withColumn("promo_is_view", lit(False))
            visit_promos = visit_promos.withColumn("promo_is_click", lit(False))

        # Unir con la tabla de hechos para asegurar que solo incluimos visitas válidas
        return visit_promos.join(
            fact_visits_df.select("visit_id"), 
            "visit_id", 
            "inner"
        ).join(
            dim_promotion_df.select("promo_id"), 
            "promo_id", 
            "inner"
        )
    except Exception as e:
        # En caso de error, devolver un DataFrame vacío con la estructura correcta
        return df.select(
            lit(0).cast(IntegerType()).alias("visit_id"),
            lit("ERROR_PROMO").alias("promo_id"),
            lit(False).cast(BooleanType()).alias("promo_is_view"),
            lit(False).cast(BooleanType()).alias("promo_is_click")
        ).where(lit(False))
