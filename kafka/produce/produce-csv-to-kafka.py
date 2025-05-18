import csv
import sys
import os
import json
import time
import ast
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

# Configuración
CSV_PATH     = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'example.csv')
KAFKA_TOPIC  = 'ventas'
KAFKA_SERVER = 'localhost:9092'
BATCH_SIZE   = 1      # Filas por batch
NUM_WORKERS  = 1      # Hilos en paralelo
SLEEP_TIME   = 1      # Segundos de espera entre mensajes (0 = sin retardo)
MAX_LINES    = 1      # Número total de líneas a leer (0 = todas)


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


def process_csv_line(line: list) -> dict:
    if len(line) != len(CSV_COLUMNS):
        return None
    return {CSV_COLUMNS[i]: parse_field(raw) for i, raw in enumerate(line)}


def send_batch(producer: KafkaProducer, batch: list, batch_id: int):
    start = datetime.now().strftime('%H:%M:%S')
    print(f"🚀 [Batch {batch_id}] Iniciando envío a las {start} (tamaño={len(batch)})")
    for row in batch:
        record = process_csv_line(row)
        if record is None:
            print(f"⚠️ [Batch {batch_id}] Línea malformada, saltando")
            continue
        producer.send(KAFKA_TOPIC, record) \
                .add_errback(lambda e, b=batch_id: print(f"❌ [Batch {b}] Error enviando mensaje: {e}"))
        if SLEEP_TIME:
            time.sleep(SLEEP_TIME)
    producer.flush()
    end = datetime.now().strftime('%H:%M:%S')
    print(f"✅ [Batch {batch_id}] Finalizado correctamente a las {end}")


def main():
    # Mensaje de cuántas líneas se van a leer
    if MAX_LINES == 0:
        print("📊 Se van a leer: todas las líneas.\n")
    else:
        print(f"📊 Se van a leer: {MAX_LINES} líneas.\n")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        linger_ms=10,
        batch_size=32768,
        buffer_memory=67108864,
        retries=3,
        value_serializer=lambda v: json.dumps(v, indent=2, ensure_ascii=False).encode('utf-8')
    )

    lines_read = 0
    with open(CSV_PATH, mode='r', encoding='utf-8') as csvfile, \
         ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:

        reader = csv.reader(csvfile)
        next(reader, None)  # saltar header

        batch = []
        batch_id = 1
        futures = []

        for row in reader:
            # Si tenemos límite y ya lo alcanzamos, paramos
            if 0 < MAX_LINES <= lines_read:
                break

            batch.append(row)
            lines_read += 1

            if len(batch) >= BATCH_SIZE:
                futures.append(executor.submit(send_batch, producer, batch.copy(), batch_id))
                batch_id += 1
                batch.clear()

        # Enviar último batch parcial
        if batch:
            futures.append(executor.submit(send_batch, producer, batch.copy(), batch_id))

        # Esperar a que terminen los envíos
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"⚠️ Excepción en worker de batch: {e}")

    # Mensaje final
    if MAX_LINES == 0:
        print(f"\n🏁 Leídas {lines_read} líneas (todo el CSV). Cerrando productor...")
    else:
        print(f"\n🏁 Leídas {lines_read}/{MAX_LINES} líneas. Cerrando productor...")

    producer.close()
    print("🎉 Proceso completado exitosamente.")


if __name__ == '__main__':
    main()
