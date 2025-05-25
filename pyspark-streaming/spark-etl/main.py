# spark_streaming_etl.py - Orquestación del pipeline con asignación de particiones y CLI mínimos

from pyspark.sql import SparkSession
import sys
import logging
import json
import argparse

# Importar módulos ETL
from extract import extract_data
from transform import transform_data
from load import load_to_warehouse

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger("spark-streaming-etl")

def setup_spark():
    """Crea y configura la SparkSession"""
    spark = (SparkSession.builder
             .appName("GAnalytics_Streaming_ETL")
             .master("local[*]")
             .config("spark.jars.packages",
                     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
                     "org.postgresql:postgresql:42.5.1")
             .config("spark.streaming.stopGracefullyOnShutdown", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    logger.info("✨ Spark inicializada")
    return spark


def setup_kafka_stream(spark, topic, partitions, bootstrap_servers="ed-kafka:29092"):
    """Configura el stream de Kafka con asignación estática de particiones"""
    assignment = json.dumps({topic: partitions})
    logger.info(f"🔌 Kafka assign: {assignment}")
    raw_df = (spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", bootstrap_servers)
              .option("assign", assignment)
              .option("startingOffsets", "latest")
              .option("failOnDataLoss", "false")
              .option("maxOffsetsPerTrigger", 600004)
              .load()
              .selectExpr("CAST(value AS STRING) as message"))
    return raw_df


def parse_partition_ranges(arg: str) -> list:
    """
    Convierte '0-24,30,32-35' en lista [0,1,...24,30,32,...35]
    """
    parts = []
    for segment in arg.split(','):
        if '-' in segment:
            start, end = segment.split('-', 1)
            parts.extend(range(int(start), int(end) + 1))
        else:
            parts.append(int(segment))
    return sorted(set(parts))


def parse_args():
    parser = argparse.ArgumentParser(description="Spark Streaming ETL CLI con soporte multi-output")
    parser.add_argument(
        '--partitions', '-p', required=True,
        help="Rangos de particiones para assign, e.g. '0-24,30-35'"
    )
    parser.add_argument(
        '--topic', '-k', required=True,
        help="Topic de Kafka a consumir"
    )
    parser.add_argument(
        '--output',
        choices=['db', 'csv'],
        default='db',
        help="Tipo de output: 'db' para PostgreSQL, 'csv' para archivos"
    )
    parser.add_argument(
        '--output-path',
        help="Ruta base para guardar CSVs (requerido si output=csv)"
    )
    return parser.parse_args()


def process_batch(df, batch_id, spark, jdbc_url, db_properties, args):
    """Procesa cada batch con configuración de output"""
    logger.info(f"🔄 Batch {batch_id}: procesando")
    if df.isEmpty():
        logger.info(f"⏭️ Batch {batch_id} vacío, omitido")
        return

    try:
        extracted = extract_data(df, spark)
        if extracted.isEmpty():
            logger.info(f"⏭️ Batch {batch_id}: nada que extraer")
            return

        transformed = transform_data(extracted, spark)

        if args.output == "db":
            load_to_warehouse(transformed, 'db', jdbc_url, db_properties)
        elif args.output == "csv":
            load_to_warehouse(transformed, 'csv', args.output_path, batch_id)

        logger.info(f"✅ Batch {batch_id} completado")
    except Exception as e:
        logger.error(f"💥 Batch {batch_id} falló: {e}")
        raise


def main():
    args = parse_args()

    # Validar argumentos
    if args.output == "csv" and not args.output_path:
        logger.error("Se requiere --output-path para modo CSV")
        sys.exit(1)

    # Configuración común
    partitions = parse_partition_ranges(args.partitions)
    topic = args.topic
    bootstrap_servers = "ed-kafka:29092"
    trigger_interval = "1 minute"
    checkpoint_base = "/tmp/checkpoints/etl"
    checkpoint_path = f"{checkpoint_base}/partitions_{args.partitions.replace(',', '_')}"

    # Configuración de PostgreSQL
    jdbc_url = "jdbc:postgresql://ed-postgres:5432/spark_results"
    db_props = {"user": "spark", "password": "root", "driver": "org.postgresql.Driver"}

    spark = setup_spark()
    stream_df = setup_kafka_stream(spark, topic, partitions, bootstrap_servers)

    query = (stream_df.writeStream
             .foreachBatch(lambda df, bid: process_batch(df, bid, spark, jdbc_url, db_props, args))
             .outputMode("append")
             .trigger(processingTime=trigger_interval)
             .option("checkpointLocation", checkpoint_path)
             .start())

    logger.info(f"🚀 Stream iniciado (output={args.output})")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("🛑 Parando stream...")
        query.stop()
        logger.info("🛑 Stream detenido")

if __name__ == "__main__":
    main()
