import os
import json
import time
import logging
import threading
from datetime import datetime
from confluent_kafka import Producer, Consumer, KafkaException

# логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s - %(message)s')

# настройки из переменных окружения (или значения по умолчанию)
BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'kafka1:9092,kafka2:9092,kafka3:9092')
TOPIC_NAME = os.getenv('TOPIC_NAME', 'my-topic')


# класс сообщения — для сериализации/десериализации
class Message:
    def __init__(self, msg_id, payload):
        self.id = msg_id
        self.timestamp = datetime.now().isoformat()
        self.payload = payload

    # сериализация в JSON
    def to_json(self):
        return json.dumps(self.__dict__, ensure_ascii=False)

    # десериализация из JSON
    @staticmethod
    def from_json(json_str):
        data = json.loads(json_str)
        return Message(data['id'], data['payload'])


# продюсер
def run_producer():
    # настройка продюсера для гарантии "At Least Once"
    config = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'acks': 'all',              # ждём подтверждения от всех реплик
        'retries': 5,               # повторные попытки при сбое
        'enable.idempotence': True, # защита от дублей при повторной отправке
    }

    producer = Producer(config)
    logging.info("Продюсер запущен.")

    for i in range(1, 51):  # отправка 50 сообщений
        msg = Message(msg_id=i, payload=f"Тестовое сообщение #{i}")

        try:
            json_data = msg.to_json()
            logging.info(f"[PRODUCER] Отправка: {json_data}")

            producer.produce(
                topic=TOPIC_NAME,
                value=json_data.encode('utf-8')
            )
        except Exception as e:
            # если ошибка сериалзации  -логируем и идём дальше
            logging.error(f"[PRODUCER] Ошибка: {e}")

        time.sleep(0.5)

    # ждём пока все сообщения улетят на брокер
    producer.flush()
    logging.info("Продюсер завершил работу.")


# консьюмер 1 — читает по 1 сообщению, авто-коммит
def run_single_consumer():
    config = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': 'single-consumer-group',   # уникальная группа
        'enable.auto.commit': True,            # автоматический коммит оффсета
        'auto.offset.reset': 'earliest',       # читаать с начала если оффсетов ещё нет
    }

    consumer = Consumer(config)
    consumer.subscribe([TOPIC_NAME])
    logging.info("SingleMessageConsumer запущен.")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                # вслучае  ошибки — логируем и продолжаем работать
                logging.error(f"[SINGLE] Ошибка: {msg.error()}")
                continue

            try:
                # десериализация
                message = Message.from_json(msg.value().decode('utf-8'))
                logging.info(f"[SINGLE] Получено: ID={message.id}, Payload={message.payload}")
            except Exception as e:
                # если ошибка десериализации — логируем и идём дальше
                logging.error(f"[SINGLE] Ошибка десериализации: {e}")
    finally:
        consumer.close()


# Консьюмер 2 — читает пачками, ручной коммит
def run_batch_consumer():
    config = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': 'batch-consumer-group',    # уникальная группа 2 и она читает те же сообщения
        'enable.auto.commit': False,           # отключаем авто-коммит
        'auto.offset.reset': 'earliest',
        'fetch.min.bytes': 1024,               # ждём минимум 1 КБ данных от брокера
        'fetch.wait.max.ms': 3000,             # максимум 3 секунды ожидания
    }

    consumer = Consumer(config)
    consumer.subscribe([TOPIC_NAME])
    logging.info("BatchMessageConsumer запущен.")

    try:
        while True:
            # consume() — штатный метод для получения пачки сообщений
            messages = consumer.consume(num_messages=10, timeout=2.0)

            if not messages:
                continue

            # фильтруем сообщения с ошибками
            valid_messages = []
            for msg in messages:
                if msg.error():
                    logging.error(f"[BATCH] Ошибка: {msg.error()}")
                else:
                    valid_messages.append(msg)

            if not valid_messages:
                continue

            logging.info(f"[BATCH] Получена пачка из {len(valid_messages)} сообщений.")

            # обрабатываем все сообщения в цикле
            for msg in valid_messages:
                try:
                    message = Message.from_json(msg.value().decode('utf-8'))
                    logging.info(f"[BATCH] Обработка: ID={message.id}, Payload={message.payload}")
                except Exception as e:
                    logging.error(f"[BATCH] Ошибка десериализации: {e}")

            # оин коммит после обработки всей пачки
            try:
                consumer.commit(asynchronous=False)
                logging.info("[BATCH] Оффсет закоммичен после пачки.")
            except KafkaException as e:
                logging.error(f"[BATCH] Ошибка коммита: {e}")
    finally:
        consumer.close()


# вход — запускаем всё параллельно
if __name__ == "__main__":
    # запуск продюсера и двух консьюмеров в разных потоках
    t1 = threading.Thread(target=run_producer, name="Producer")
    t2 = threading.Thread(target=run_single_consumer, name="SingleConsumer")
    t3 = threading.Thread(target=run_batch_consumer, name="BatchConsumer")

    t1.start()
    t2.start()
    t3.start()

    t1.join()
    t2.join()
    t3.join()