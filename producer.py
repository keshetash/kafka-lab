import json
import time
from kafka import KafkaProducer
from message_generator import generate_repair_order, message_to_json

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "repair_orders"
MESSAGE_INTERVAL_SECONDS = 3


def create_producer():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None
    )
    return producer


def send_message(producer, topic, message):
    key = message.get("order_id")
    future = producer.send(topic, key=key, value=message)
    record_metadata = future.get(timeout=10)
    print(f"[PRODUCER] Сообщение отправлено | Партиция: {record_metadata.partition} | Смещение: {record_metadata.offset}")


def main():
    print("=" * 60)
    print("  KAFKA PRODUCER — Сервис по ремонту техники")
    print("=" * 60)
    print(f"  Брокер  : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"  Топик   : {KAFKA_TOPIC}")
    print(f"  Интервал: {MESSAGE_INTERVAL_SECONDS} сек.")
    print("=" * 60)
    print("Нажмите Ctrl+C для остановки\n")

    producer = create_producer()

    invalid_message = {
        "order_id": "X",
        "client": "",
        "device_type": "Неизвестное устройство",
        "fault_type": "Неизвестная неисправность",
        "engineer": "",
        "part": "Деталь",
        "status": "НЕИЗВЕСТЕН",
        "price": -500,
        "timestamp": "bad-date"
    }

    try:
        print("\n[PRODUCER] Отправка НЕВАЛИДНОГО сообщения:")
        print(json.dumps(invalid_message, ensure_ascii=False, indent=2))
        send_message(producer, KAFKA_TOPIC, invalid_message)
        time.sleep(1)

        while True:
            message = generate_repair_order()
            print("\n[PRODUCER] Сгенерировано сообщение:")
            print(message_to_json(message))
            send_message(producer, KAFKA_TOPIC, message)
            time.sleep(MESSAGE_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\n[PRODUCER] Остановка (Ctrl+C)")

    finally:
        producer.flush()
        producer.close()
        print("[PRODUCER] Соединение закрыто.")


if __name__ == "__main__":
    main()
