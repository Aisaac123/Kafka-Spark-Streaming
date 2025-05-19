from pyspark.sql import SparkSession
import sys

def setup_spark_kafka_stream(group_id,
                             bootstrap_servers="ed-kafka:29092",
                             topic="visits"):
    spark = SparkSession.builder \
        .appName(f"KafkaConsumer_{group_id}") \
        .master("local[*]") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("kafka.group.id", group_id) \
        .load() \
        .selectExpr("CAST(value AS STRING) as message")

    def process_batch(df, batch_id):
        print(f"\n👤 [{group_id}] Procesando batch {batch_id}…")
        for row in df.select("message").collect():
            print(f"\n-- GET MESSAGE FROM [{group_id}] --")
            print(row["message"])
        print(f"✅ [{group_id}] Batch {batch_id} finalizado.\n")

    query = raw_df.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .start()

    return query


if __name__ == "__main__":
    group_id = sys.argv[1] if len(sys.argv) > 1 else "sells-consumer"
    query = setup_spark_kafka_stream(group_id)
    query.awaitTermination()