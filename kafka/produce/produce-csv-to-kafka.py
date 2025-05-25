import csv
import sys
import os
import json
import time
import ast
import argparse
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Aumentamos el límite al máximo que permite el sistema
csv.field_size_limit(sys.maxsize)

# Columnas en el orden del CSV
CSV_COLUMNS = [
    "channelGrouping", "customDimensions", "date", "device", "fullVisitorId",
    "geoNetwork", "hits", "socialEngagementType", "totals", "trafficSource",
    "visitId", "visitNumber", "visitStartTime"
]


def parse_args():
    parser = argparse.ArgumentParser(description="Producer de CSV a Kafka con configuración por CLI")
    parser.add_argument(
        '--csv-path', type=str,
        default=os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'data_02.csv'),
        help='Ruta relativa desde raíz/data al CSV'
    )
    parser.add_argument(
        '--kafka-topic', type=str,
        default='visits', help='Topic de Kafka'
    )
    parser.add_argument(
        '--kafka-server', type=str,
        default='localhost:9092', help='Bootstrap server de Kafka'
    )
    parser.add_argument(
        '--batch-size', type=int,
        default=100, help='Filas por batch'
    )
    parser.add_argument(
        '--num-workers', type=int,
        default=1, help='Número de hilos en paralelo'
    )
    parser.add_argument(
        '--sleep-time', type=float,
        default=0, help='Segundos de espera entre mensajes'
    )
    parser.add_argument(
        '--max-lines', type=int,
        default=0, help='Número total de líneas a leer (0 = todas)'
    )
    parser.add_argument(
        '--start-line', type=int,
        default=1,
        help='Número de línea (basado en datos) donde iniciar la lectura (1 = primera línea después del header)'
    )
    return parser.parse_args()


def remove_unavailable(obj):
    if isinstance(obj, dict):
        return {
            k: remove_unavailable(v)
            for k, v in obj.items()
            if v != "not available in demo dataset"
        }
    elif isinstance(obj, list):
        return [remove_unavailable(elem) for elem in obj]
    return obj

def parse_field(raw: str):
    v = raw.strip()
    if v.startswith('"') and v.endswith('"'):
        v = v[1:-1]
    try:
        if (v.startswith('{') and v.endswith('}')) or (v.startswith('[') and v.endswith(']')):
            return ast.literal_eval(v)
    except Exception:
        pass
    try:
        return json.loads(v)
    except Exception:
        return v


def process_csv_line(line: list) -> None | dict[Any, Any] | list[Any] | dict | list:
    if len(line) != len(CSV_COLUMNS):
        return None
    record = {CSV_COLUMNS[i]: parse_field(raw) for i, raw in enumerate(line)}
    return remove_unavailable(record)


def send_batch(producer: KafkaProducer, batch: list, batch_id: int, topic: str, sleep_time: float):
    start = datetime.now().strftime('%H:%M:%S')
    print(f"🚀 [Batch {batch_id}] Iniciando envío a las {start} (tamaño={len(batch)})")
    for row in batch:
        record = process_csv_line(row)
        if record is None:
            print(f"⚠️ [Batch {batch_id}] Línea malformada, saltando")
            continue
        producer.send(topic, record)
        if sleep_time:
            time.sleep(sleep_time)
    producer.flush()
    end = datetime.now().strftime('%H:%M:%S')
    print(f"✅ [Batch {batch_id}] Finalizado correctamente a las {end}")


def callback_handler(future):
    """Manejador de callbacks para los futures"""
    try:
        future.result()
    except Exception as e:
        print(f"⚠️ Excepción en worker de batch: {e}")


def main():
    args = parse_args()
    csv_path = os.path.abspath(args.csv_path)
    kafka_topic = args.kafka_topic
    kafka_server = args.kafka_server
    batch_size = args.batch_size
    num_workers = args.num_workers
    sleep_time = args.sleep_time
    max_lines = args.max_lines
    start_line = args.start_line

    # Mensaje de cuántas líneas se van a leer
    if max_lines == 0:
        print("📊 Se van a leer: todas las líneas.")
    else:
        print(f"📊 Se van a leer: {max_lines} líneas.")
    print(f"🔍 Se iniciará lectura desde la línea de datos {start_line} después del header.\n")

    producer = KafkaProducer(
        bootstrap_servers=kafka_server,
        linger_ms=10,
        batch_size=32768,
        buffer_memory=67108864,
        retries=3,
        value_serializer=lambda v: json.dumps(v, indent=2, ensure_ascii=False).encode('utf-8')
    )

    lines_read = 0
    with open(csv_path, mode='r', encoding='utf-8') as csvfile, \
         ThreadPoolExecutor(max_workers=num_workers) as executor:

        reader = csv.reader(csvfile)
        # Saltar header
        next(reader, None)
        # Saltar hasta start_line
        for _ in range(start_line - 1):
            if next(reader, None) is None:
                break

        batch = []
        batch_id = 1

        for row in reader:
            if 0 < max_lines <= lines_read:
                break

            batch.append(row)
            lines_read += 1

            if len(batch) >= batch_size:
                future = executor.submit(
                    send_batch,
                    producer,
                    batch.copy(),
                    batch_id,
                    kafka_topic,
                    sleep_time
                )
                future.add_done_callback(callback_handler)
                batch_id += 1
                batch.clear()

        # Enviar último batch parcial
        if batch:
            future = executor.submit(
                send_batch,
                producer,
                batch.copy(),
                batch_id,
                kafka_topic,
                sleep_time
            )
            future.add_done_callback(callback_handler)

        print("🚀 Todos los trabajos han sido programados")

    # Mensaje final
    if max_lines == 0:
        print(f"\n🏁 Leídas {lines_read} líneas (todo el CSV). Cerrando productor...")
    else:
        print(f"\n🏁 Leídas {lines_read}/{max_lines} líneas. Cerrando productor...")

    producer.close()
    print("🎉 Proceso completado exitosamente.")


if __name__ == '__main__':
    main()
