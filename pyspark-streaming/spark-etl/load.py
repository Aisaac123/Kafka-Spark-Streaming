# load.py - Carga simple que ignora errores de PK

import logging
from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame

logger = logging.getLogger("load")

def safe_load(df: DataFrame, table: str, jdbc_url: str, db_properties: dict):
    """Carga datos ignorando errores de duplicados en la clave primaria."""
    try:
        df.write.jdbc(
            url=jdbc_url,
            table=table,
            mode="append",
            properties=db_properties
        )
        logger.info(f"📥 {table}: datos cargados exitosamente")
    except Py4JJavaError as e:
        msg = str(e.java_exception.getMessage()).lower()
        if "duplicate key" in msg or "violates unique constraint" in msg:
            logger.warning(f"⚠️ {table}: duplicado ignorado")
        else:
            raise
    except Exception:
        raise


def load_to_warehouse(transformed_data: dict, jdbc_url: str, db_properties: dict):
    """Carga todas las tablas sin manejar duplicados"""
    logger.info("📦 Carga iniciada...")

    load_order = [
        "dim_date", "dim_visitor", "dim_device", "dim_geo",
        "dim_channel", "dim_traffic_source", "dim_campaign", "dim_coupon",
        "visit_sales_fact"
    ]

    for table in load_order:
        if table in transformed_data:
            safe_load(transformed_data[table], table, jdbc_url, db_properties)

    logger.info("✅ Carga finalizada (duplicados ignorados)")
