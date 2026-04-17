import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
import os
import time
import glob
from datetime import datetime
from confluent_kafka import Consumer, KafkaError

# ---------- Configuración ----------
KAFKA_BROKER = "localhost:9092"
TOPIC = "social-events"
OUTPUT_FILE = "output/trend_data.json"
BEAM_TMP = "output/_beam_tmp"
WINDOW_SECONDS = 10

os.makedirs("output", exist_ok=True)


# ---------- Consumidor Kafka en batch ----------
def consume_kafka_batch(max_messages=200, timeout_sec=5):
    """Lee un batch de mensajes de Kafka y los retorna como lista."""
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": "beam-trend-detector",
            "auto.offset.reset": "latest",
        }
    )
    consumer.subscribe([TOPIC])

    messages = []
    deadline = time.time() + timeout_sec

    while time.time() < deadline and len(messages) < max_messages:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"  Error Kafka: {msg.error()}")
            continue
        try:
            data = json.loads(msg.value().decode("utf-8"))
            messages.append(data)
        except json.JSONDecodeError:
            pass

    consumer.close()
    return messages


# ---------- Beam Pipeline ----------
def run_pipeline_batch(messages):
    """Ejecuta Beam sobre los mensajes y retorna resultados."""
    if not messages:
        return []

    # Limpiar archivos temporales previos
    for f in glob.glob(f"{BEAM_TMP}*"):
        os.remove(f)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    options = PipelineOptions(runner="DirectRunner")

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Crear colección" >> beam.Create(messages)
            | "Extraer KV" >> beam.Map(lambda x: (x.get("topic", "unknown"), 1))
            | "Contar por topic" >> beam.CombinePerKey(sum)
            | "A JSON" >> beam.Map(
                lambda x: json.dumps(
                    {"topic": x[0], "count": x[1], "window_end": now}
                )
            )
            | "Escribir" >> beam.io.WriteToText(
                BEAM_TMP,
                file_name_suffix=".jsonl",
                shard_name_template="",
            )
        )

    # Leer resultados del archivo que Beam escribió
    results = []
    output_path = f"{BEAM_TMP}.jsonl"
    if os.path.exists(output_path):
        with open(output_path, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    results.append(json.loads(line))
        os.remove(output_path)

    return results


def append_results(new_results):
    """Agrega resultados al archivo JSON acumulado para Streamlit."""
    existing = []
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, "r") as f:
                existing = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            existing = []

    existing.extend(new_results)
    existing = existing[-500:]

    with open(OUTPUT_FILE, "w") as f:
        json.dump(existing, f, indent=2)


# ---------- Loop principal ----------
def main():
    print("=" * 50)
    print(" Trend Detector - Apache Beam Pipeline")
    print(f" Broker: {KAFKA_BROKER}")
    print(f" Topic:  {TOPIC}")
    print(f" Output: {OUTPUT_FILE}")
    print("=" * 50)

    cycle = 0
    while True:
        cycle += 1
        print(f"\n--- Ciclo {cycle} ---")

        messages = consume_kafka_batch(max_messages=100, timeout_sec=WINDOW_SECONDS)
        print(f"  Mensajes recibidos: {len(messages)}")

        if messages:
            results = run_pipeline_batch(messages)
            print(f"  Resultados Beam:")
            for r in results:
                print(f"    {r['topic']}: {r['count']}")

            append_results(results)
            print(f"  Guardado en {OUTPUT_FILE}")
        else:
            print("  Sin mensajes, esperando...")

        time.sleep(2)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nPipeline detenido")
