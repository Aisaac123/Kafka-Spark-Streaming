import json
import os
from kafka import KafkaConsumer
import csv
from io import StringIO

# Configuración
KAFKA_TOPIC = 'ventas'
KAFKA_SERVER = 'localhost:9092'
CSV_HEADERS = [
    "channelGrouping", "customDimensions", "date", "device",
    "fullVisitorId", "geoNetwork", "hits", "socialEngagementType",
    "totals", "trafficSource", "visitId", "visitNumber", "visitStartTime"
]

# Consumidor Kafka
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='csv_json_consumer',
    value_deserializer=lambda m: m.decode('utf-8')
)

print("👂 Escuchando mensajes de Kafka y transformando a JSON...")

for message in consumer:
    csv_line = message.value
    try:
        # Parsear la línea CSV usando csv.reader (para respetar las comillas y escapes)
        row = next(csv.reader(StringIO(csv_line)))
        json_data = dict(zip(CSV_HEADERS, row))

        # Imprimir como JSON formateado
        print("📦 Mensaje recibido:")
        print(json.dumps(json_data, indent=2))

    except Exception as e:
        print(f"❌ Error al procesar mensaje: {e}")
