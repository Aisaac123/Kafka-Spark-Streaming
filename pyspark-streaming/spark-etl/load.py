from pyspark.sql import DataFrame
import logging

logger = logging.getLogger("spark-streaming-etl")

def get_primary_key(table_name):
    """
    Devuelve el nombre de la columna de clave primaria para una tabla dada.

    Args:
        table_name: Nombre de la tabla

    Returns:
        Nombre de la columna de clave primaria o None si no se conoce
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
        "fact_visits_products": None,  # Clave compuesta, no se maneja aquí
        "fact_visits_promotions": None  # Clave compuesta, no se maneja aquí
    }

    return primary_keys.get(table_name)

def load_to_warehouse(data_dict: dict, jdbc_url: str, db_properties: dict) -> None:
    """
    Carga los datos transformados en el data warehouse.

    Args:
        data_dict: Diccionario con DataFrames transformados
        jdbc_url: URL de conexión JDBC a PostgreSQL
        db_properties: Propiedades de conexión a la base de datos
    """
    logger.info("📥 Cargando datos en el data warehouse")

    if not data_dict:
        logger.info("⚠️ No hay datos para cargar")
        return

    try:
        # Orden de carga: primero dimensiones, luego hechos, luego relaciones
        load_order = [
            "dim_time", 
            "dim_visitor", 
            "dim_device", 
            "dim_geo", 
            "dim_channel", 
            "dim_traffic_source", 
            "dim_product",
            "dim_promotion",
            "fact_visits",
            "fact_visits_products",
            "fact_visits_promotions"
        ]

        for table_name in load_order:
            if table_name not in data_dict:
                logger.warning(f"⚠️ Tabla {table_name} no encontrada en los datos transformados")
                continue

            df = data_dict[table_name]

            if df.isEmpty():
                logger.info(f"⏭️ Tabla {table_name} vacía, omitiendo")
                continue

            # Cargar datos en modo upsert (actualizar si existe, insertar si no)
            load_table(df, table_name, jdbc_url, db_properties)

        logger.info("✅ Carga completada exitosamente")

    except Exception as e:
        logger.error(f"💥 Error al cargar datos: {e}")
        raise

def load_table(df: DataFrame, table_name: str, jdbc_url: str, db_properties: dict) -> None:
    """
    Carga un DataFrame en una tabla específica del data warehouse.

    Args:
        df: DataFrame a cargar
        table_name: Nombre de la tabla destino
        jdbc_url: URL de conexión JDBC a PostgreSQL
        db_properties: Propiedades de conexión a la base de datos
    """
    try:
        logger.info(f"📤 Cargando tabla {table_name} ({df.count()} registros)")

        # Configurar opciones de escritura
        write_options = {
            "url": jdbc_url,
            "dbtable": table_name,
            "user": db_properties["user"],
            "password": db_properties["password"],
            "driver": db_properties["driver"]
        }

        # Determinar el modo de escritura según la tabla
        # Para tablas de dimensiones, necesitamos evitar duplicados
        # Para la tabla de hechos, usamos "append" ya que la deduplicación ya se hizo en transform
        write_mode = "append"  # Valor predeterminado

        if table_name.startswith("dim_"):
            # Para tablas de dimensiones, filtrar registros que ya existen en la base de datos
            # Obtener la clave primaria según la tabla
            primary_key = get_primary_key(table_name)

            # Filtrar registros que ya existen en la base de datos
            if primary_key:
                try:
                    # Leer los IDs existentes en la base de datos
                    existing_ids_df = df.sparkSession.read \
                        .format("jdbc") \
                        .options(**write_options) \
                        .option("dbtable", f"(SELECT {primary_key} FROM {table_name}) AS existing_ids") \
                        .load()

                    # Obtener los IDs como una lista
                    existing_ids = [row[0] for row in existing_ids_df.select(primary_key).collect()]

                    # Filtrar el DataFrame para excluir registros con IDs existentes
                    if existing_ids:
                        df = df.filter(~df[primary_key].isin(existing_ids))
                        logger.info(f"Filtrando {len(existing_ids)} registros existentes de {table_name}")
                except Exception as e:
                    logger.warning(f"No se pudieron filtrar registros existentes: {e}")
                    # Si no podemos filtrar, usar el modo "ignore" para evitar errores de duplicados
                    write_mode = "ignore"
                    logger.info(f"Usando modo 'ignore' para {table_name} para evitar duplicados")

        # Escribir en la base de datos
        df.write.format("jdbc") \
            .options(**write_options) \
            .mode(write_mode) \
            .save()

        logger.info(f"✅ Tabla {table_name} cargada correctamente")

    except Exception as e:
        logger.error(f"💥 Error al cargar tabla {table_name}: {e}")
        raise
