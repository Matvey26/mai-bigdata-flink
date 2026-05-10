"""
Читает все csv-файлы из папки 'исходные данные' и шлёт каждую строку
как json-сообщение в kafka-топик 'sales'.
"""

import csv
import glob
import json
import os
import time

from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "sales")
DATA_DIR = os.environ.get("DATA_DIR", "исходные данные")


def make_producer():
    # ретраи на случай если кафка ещё не поднялась
    for _ in range(30):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            )
        except Exception as e:
            print("kafka not ready, retry:", e)
            time.sleep(2)
    raise RuntimeError("Kafka unavailable")


def main():
    producer = make_producer()
    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.csv")))
    print("files:", files)

    # id'шники в каждом csv начинаются с 1 — добавляем смещение по номеру файла,
    # чтобы они стали глобально уникальными
    ID_FIELDS = ("id", "sale_customer_id", "sale_seller_id", "sale_product_id")
    OFFSET_STEP = 1000

    sent = 0
    for file_idx, path in enumerate(files):
        offset = file_idx * OFFSET_STEP
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # чистим пустые ключи (в csv бывает trailing comma)
                row = {k: v for k, v in row.items() if k}
                for field in ID_FIELDS:
                    if row.get(field):
                        try:
                            row[field] = int(row[field]) + offset
                        except ValueError:
                            pass
                producer.send(TOPIC, row)
                sent += 1
                if sent % 500 == 0:
                    print(f"sent {sent}")
                    producer.flush()

    producer.flush()
    print(f"done, total sent: {sent}")


if __name__ == "__main__":
    main()
