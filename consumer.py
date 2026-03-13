import json
from datetime import datetime
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "repair_orders"
CONSUMER_GROUP_ID = "repair_consumer_group"

REQUIRED_FIELDS = [
    "order_id", "client", "device_type", "fault_type",
    "engineer", "part", "status", "price", "timestamp"
]

VALID_STATUSES = {"Принят", "В работе", "Ожидание запчасти", "Готов", "Выдан"}
MIN_PRICE = 1
MAX_PRICE = 100000
ORDER_ID_PREFIX = "RP-"
ORDER_ID_MIN_LEN = 7


def validate_message(message: dict) -> tuple[bool, list[str]]:
    errors = []

    for field in REQUIRED_FIELDS:
        if field not in message:
            errors.append(f"Отсутствует обязательное поле: '{field}'")

    if errors:
        return False, errors

    status = message.get("status", "")
    if status not in VALID_STATUSES:
        errors.append(f"Недопустимый статус заказа: '{status}'. Допустимые: {VALID_STATUSES}")

    price = message.get("price")
    if not isinstance(price, int) or not (MIN_PRICE <= price <= MAX_PRICE):
        errors.append(f"Некорректная стоимость: {price}. Допустимо: {MIN_PRICE}-{MAX_PRICE}")

    order_id = message.get("order_id", "")
    if not order_id.startswith(ORDER_ID_PREFIX) or len(order_id) < ORDER_ID_MIN_LEN:
        errors.append(f"Некорректный номер заказа: '{order_id}'. Ожидается формат RP-XXXX")

    client = message.get("client", "")
    if not client or len(client.strip()) == 0:
        errors.append("Поле 'client' не может быть пустым")

    engineer = message.get("engineer", "")
    if not engineer or len(engineer.strip()) == 0:
        errors.append("Поле 'engineer' не может быть пустым")

    is_valid = len(errors) == 0
    return is_valid, errors


def create_consumer():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )
    return consumer


def process_message(message_value: dict) -> None:
    print("\n" + "-" * 60)
    print("[CONSUMER] Получено сообщение:")
    print(json.dumps(message_value, ensure_ascii=False, indent=2))

    is_valid, errors = validate_message(message_value)

    if is_valid:
        order = message_value.get("order_id", "???")
        client = message_value.get("client", "???")
        status = message_value.get("status", "???")
        print(f"\n[CONSUMER] VALID  |  Заказ {order} | Клиент: {client} | Статус: {status}")
    else:
        print("\n[CONSUMER] NOT VALID")
        for err in errors:
            print(f"           - {err}")
    print("-" * 60)


def main():
    print("=" * 60)
    print("  KAFKA CONSUMER — Сервис по ремонту техники")
    print("=" * 60)
    print(f"  Брокер : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"  Топик  : {KAFKA_TOPIC}")
    print(f"  Группа : {CONSUMER_GROUP_ID}")
    print("=" * 60)
    print("Ожидание сообщений... Нажмите Ctrl+C для остановки\n")

    consumer = create_consumer()

    try:
        for record in consumer:
            process_message(record.value)

    except KeyboardInterrupt:
        print("\n[CONSUMER] Остановка (Ctrl+C)")

    finally:
        consumer.close()
        print("[CONSUMER] Соединение закрыто.")


if __name__ == "__main__":
    main()
