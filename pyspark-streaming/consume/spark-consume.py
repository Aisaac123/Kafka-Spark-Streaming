from pyspark.sql import SparkSession
import argparse
import json
import logging

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("spark-kafka-consume")

def parse_partition_ranges(arg: str) -> list:
    """Convierte '0-2,4' en [0,1,2,4]"""
    parts = []
    for segment in arg.split(','):
        if '-' in segment:
            start, end = segment.split('-', 1)
            parts.extend(range(int(start), int(end) + 1))
        else:
            parts.append(int(segment))
    return sorted(set(parts))


def setup_spark():
    spark = SparkSession.builder \
        .appName("KafkaConsumer_Assign") \
        .master("local[*]") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def setup_kafka_stream(spark, topic, partitions, bootstrap_servers="ed-kafka:29092"):
    assignment = json.dumps({topic: partitions})
    logger.info(f"🔌 Kafka assign: {assignment}")

    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("assign", assignment) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load() \
        .selectExpr("CAST(value AS STRING) as message")

    return raw_df


def process_batch(df, batch_id):
    logger.info(f"🔄 Batch {batch_id} procesando...")
    for row in df.select("message").collect():
        print(f"📥 Mensaje recibido:\n{row['message']}")
    logger.info(f"✅ Batch {batch_id} finalizado\n")


def parse_args():
    parser = argparse.ArgumentParser(description="Consumidor Kafka con Spark + Assign")
    parser.add_argument('--topic', '-k', required=True, help="Topic de Kafka")
    parser.add_argument('--partitions', '-p', required=True, help="Rangos de particiones (ej: 0-2,4,7)")
    return parser.parse_args()


def main():
    args = parse_args()
    topic = args.topic
    partitions = parse_partition_ranges(args.partitions)

    spark = setup_spark()
    df = setup_kafka_stream(spark, topic, partitions)

    query = df.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .start()

    logger.info(f"🚀 Consumidor iniciado (topic={topic}, partitions={partitions})")
    query.awaitTermination()


if __name__ == "__main__":
    main()
