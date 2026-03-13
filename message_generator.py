import random
import json
from datetime import datetime

CLIENTS = ["Иванов А.С.", "Петрова М.В.", "Сидоров К.Н.", "Козлова Е.О.", "Новиков Д.П.",
           "Морозова Т.И.", "Федоров В.А.", "Лебедева Н.С.", "Соколов Р.Г.", "Попова Ю.В."]

DEVICE_TYPES = ["Смартфон", "Ноутбук", "Планшет", "Телевизор", "Холодильник",
                "Стиральная машина", "Микроволновая печь", "Пылесос"]

FAULT_TYPES = ["Не включается", "Треснул экран", "Не заряжается", "Перегрев",
               "Механическое повреждение", "Не работает клавиатура", "Замена аккумулятора",
               "Попадание влаги", "Программный сбой"]

ENGINEERS = ["Смирнов И.В.", "Кузнецов А.Д.", "Попов Е.С.", "Волков М.Р.", "Захаров Н.О."]

PARTS = ["Аккумулятор", "Дисплей", "Материнская плата", "Зарядный порт", "Динамик",
         "Камера", "Кнопка питания", "Корпус", "Шлейф", "Не требуется"]

STATUSES = ["Принят", "В работе", "Ожидание запчасти", "Готов", "Выдан"]


def generate_repair_order() -> dict:
    order_id = f"RP-{random.randint(1000, 9999)}"

    message = {
        "order_id": order_id,
        "client": random.choice(CLIENTS),
        "device_type": random.choice(DEVICE_TYPES),
        "fault_type": random.choice(FAULT_TYPES),
        "engineer": random.choice(ENGINEERS),
        "part": random.choice(PARTS),
        "status": random.choice(STATUSES),
        "price": random.randint(500, 15000),
        "timestamp": datetime.now().isoformat()
    }

    return message


def message_to_json(message: dict) -> str:
    return json.dumps(message, ensure_ascii=False, indent=2)
