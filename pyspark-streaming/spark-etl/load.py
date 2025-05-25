import subprocess

from pyspark.sql import DataFrame, SparkSession
import logging
from datetime import datetime
import os

from pyspark.sql.functions import expr

logger = logging.getLogger("spark-streaming-etl")


def get_primary_key(table_name):
    """
    Devuelve el nombre de la columna de clave primaria para una tabla dada.
    """
    primary_keys = {
        "dim_time": "time_id",
        "dim_visitor": "full_visitor_id",
        "dim_device": "device_id",
        "dim_geo": "geo_id",
        "dim_channel": "channel_id",
        "dim_traffic_source": "traffic_source_id",
        "dim_product": "product_id",
        "dim_promotion": "promo_id",
        "fact_visits": "visit_id",
        "fact_visits_products": None,
        "fact_visits_promotions": None
    }
    return primary_keys.get(table_name)

# Main load function
def load_to_warehouse(data_dict: dict, *args) -> None:
    """
    Router principal de carga que maneja múltiples outputs
    """
    output_type = args[0]

    if output_type == 'db':
        _load_to_database(data_dict, args[1], args[2])
    elif output_type == 'csv':
        _load_to_csv(data_dict, args[1], args[2])
    elif output_type == 'hdfs':  # Nueva opción
        _load_to_hdfs(data_dict, args[1])
    else:
        raise ValueError(f"Tipo de output no soportado: {output_type}")

# Load to HDFS
def _load_to_hdfs(data_dict: dict, hdfs_path: str) -> None:
    """Carga datos en HDFS: particiona fact_visits y las intermedias, sin particionar dimensiones."""
    logger.info("📥 Cargando datos en HDFS")

    hdfs_namenode = "hdfs://namenode:8020"
    full_hdfs_path = f"{hdfs_namenode}{hdfs_path}"

    dimensiones = {
        "dim_time", "dim_visitor", "dim_device",
        "dim_geo", "dim_channel", "dim_traffic_source",
        "dim_product", "dim_promotion"
    }
    hecho     = {"fact_visits"}
    intermedias = {"fact_visits_products", "fact_visits_promotions"}

    # Prepara un pequeño DataFrame con visit_id+full_date para los joins intermedios
    fact_visits_df = data_dict.get("fact_visits")
    if fact_visits_df is None:
        raise ValueError("fact_visits no está en data_dict")

    fact_dates = fact_visits_df.select("visit_id", "full_date").alias("fv_dates")

    for table_name, df in data_dict.items():
        if df.isEmpty():
            continue

        target_path = f"{full_hdfs_path}/{table_name}"

        if table_name in dimensiones:
            # 1) Dimension: vuelca plano, sin particionar
            df.write.mode("overwrite").parquet(target_path)
            logger.info(f"✅ {table_name} (dimensión) en HDFS: {target_path}")

        elif table_name in hecho:
            # 2) Hecho: particiona por full_date
            df2 = df.withColumn("full_date", expr("to_date(full_date)"))
            df2.write \
               .partitionBy("full_date") \
               .mode("overwrite") \
               .parquet(target_path)
            logger.info(f"✅ {table_name} particionado en HDFS: {target_path}")

        elif table_name in intermedias:
            # 3) Intermedia: le agrego full_date vía join con fact_visits
            df2 = df.join(
                fact_dates,
                on="visit_id",
                how="left"
            ).withColumn("full_date", expr("to_date(full_date)"))
            df2.write \
               .partitionBy("full_date") \
               .mode("overwrite") \
               .parquet(target_path)
            logger.info(f"✅ {table_name} (intermedia) particionado en HDFS: {target_path}")

        else:
            # Por si hay alguna tabla extra: vuelca plano
            df.write.mode("overwrite").parquet(target_path)
            logger.info(f"✅ {table_name} (otro) en HDFS: {target_path}")
def _hadoop_mkdir(path: str) -> None:
    """Crea directorios en HDFS usando comandos nativos"""
    try:
        subprocess.run([
            "hadoop",
            "fs",
            "-mkdir",
            "-p",
            path
        ], check=True)
    except subprocess.CalledProcessError as e:
        logger.warning(f"No se pudo crear directorio HDFS: {e}")

# Load to DATA BASE
def _load_to_database(data_dict: dict, jdbc_url: str, db_properties: dict) -> None:
    """Carga los datos en PostgreSQL"""
    logger.info("📥 Cargando datos en PostgreSQL")

    if not data_dict:
        logger.info("⚠️ No hay datos para cargar")
        return

    try:
        load_order = [
            "dim_time", "dim_visitor", "dim_device", "dim_geo",
            "dim_channel", "dim_traffic_source", "dim_product",
            "dim_promotion", "fact_visits", "fact_visits_products",
            "fact_visits_promotions"
        ]

        for table_name in load_order:
            if table_name not in data_dict:
                logger.warning(f"⚠️ Tabla {table_name} no encontrada")
                continue

            df = data_dict[table_name]
            if df.isEmpty():
                logger.info(f"⏭️ Tabla {table_name} vacía")
                continue

            _load_db_table(df, table_name, jdbc_url, db_properties)

        logger.info("✅ Carga en PostgreSQL completada")

    except Exception as e:
        logger.error(f"💥 Error en carga DB: {e}")
        raise
def _load_db_table(df: DataFrame, table_name: str, jdbc_url: str, db_properties: dict) -> None:
    """Carga una tabla individual en PostgreSQL"""
    try:
        logger.info(f"📤 Cargando {table_name} ({df.count()} registros)")

        write_options = {
            "url": jdbc_url,
            "dbtable": table_name,
            "user": db_properties["user"],
            "password": db_properties["password"],
            "driver": db_properties["driver"]
        }

        write_mode = "append"
        pk = get_primary_key(table_name)

        if pk and table_name.startswith("dim_"):
            try:
                existing_ids = df.sparkSession.read \
                    .format("jdbc") \
                    .options(**write_options) \
                    .option("dbtable", f"(SELECT {pk} FROM {table_name}) AS existing") \
                    .load() \
                    .select(pk) \
                    .rdd.flatMap(lambda x: x).collect()

                if existing_ids:
                    df = df.filter(~df[pk].isin(existing_ids))
                    logger.info(f"Filtrados {len(existing_ids)} duplicados en {table_name}")
            except Exception as e:
                logger.warning(f"No se pudieron filtrar duplicados: {e}")
                write_mode = "ignore"

        df.write.format("jdbc") \
            .options(**write_options) \
            .mode(write_mode) \
            .save()

        logger.info(f"✅ {table_name} cargada en DB")

    except Exception as e:
        logger.error(f"💥 Error cargando {table_name}: {e}")
        raise

# Load to CSV
def _load_to_csv(data_dict: dict, output_path: str, batch_id: int) -> None:
    """Carga datos en CSVs únicos por tabla"""
    try:
        load_order = [
            "dim_time", "dim_visitor", "dim_device",
            "dim_geo", "dim_channel", "dim_traffic_source",
            "dim_product", "dim_promotion", "fact_visits",
            "fact_visits_products", "fact_visits_promotions"
        ]

        # Ruta única para todos los batches
        base_path = os.path.join(output_path, "consolidated_data")

        for table_name in load_order:
            if table_name not in data_dict:
                continue

            df = data_dict[table_name]
            if df.isEmpty():
                continue

            table_dir = os.path.join(base_path, table_name)
            os.makedirs(table_dir, exist_ok=True)

            _write_csv_file(df, table_dir, table_name)

        logger.info(f"✅ Datos consolidados en {base_path}")

    except Exception as e:
        logger.error(f"💥 Error en carga CSV: {e}")
        raise
def _write_csv_file(df: DataFrame, path: str, table_name: str) -> None:
    """Escribe en un único CSV por tabla, acumulando batches"""
    try:
        final_path = os.path.join(path, table_name + ".csv")
        header = not os.path.exists(final_path)  # Solo header si no existe

        # Leer CSV existente y unir con nuevos datos
        if os.path.exists(final_path):
            existing_df = df.sparkSession.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(final_path)
            df = existing_df.union(df)

        # Escribir todo en modo overwrite
        (df.repartition(1)
           .write
           .mode("overwrite")
           .option("header", "true" if header else "false")
           .option("delimiter", "|")
           .option("encoding", "UTF-8")
           .csv(os.path.dirname(final_path)))

        # Renombrar archivo temporal
        temp_file = [f for f in os.listdir(os.path.dirname(final_path))
                    if f.startswith("part-00000")][0]
        os.rename(
            os.path.join(os.path.dirname(final_path), temp_file),
            final_path
        )

        logger.info(f"✅ {table_name} actualizado en {final_path}")

    except Exception as e:
        logger.error(f"💥 Error escribiendo CSV: {e}")
        raise