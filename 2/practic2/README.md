# Задание 2 (дополнительно) Анализ и агрегирование сообщений с помощью ksqlDB
Задача. Реализуйте аналитику для системы обмена сообщениями с помощью ksqlDB.  
Считайте, что сообщения в JSON-формате содержат поля user_id, recipient_id, message и timestamp. 

Подзадача 1. Анализ сообщений в реальном времени
Создайте поток в ksqlDB (messages_stream), который будет принимать входящие сообщения из Kafka.
Реализуйте таблицы, подсчитывающие:
общее количество отправленных сообщений;
количество уникальных получателей сообщений;

Подзадача 2. Сбор статистики по пользователям
Создайте таблицу user_statistics для агрегирования данных по каждому пользователю:
сообщения, отправленные каждым пользователем;
количество уникальных получателей для каждого пользователя;
Сохраняйте данные в таблице для последующего использования.

Инфраструктура
Используйте Docker-сompose для развёртывания ksqlDB.
Подготовьте топики Kafka:
messages ― входящие сообщения,
user_statistics ― результаты агрегирования.
Как должен выглядеть результат

В системе: 
Пользователи могут выполнять запросы к ksqlDB для получения статистики.
Отображаются данные в реальном времени на основе входящих сообщений.

Выполненное задание должно включать:
Docker-compose файл, который развернёт инфраструктуру для выполнения задания.
ksqldb-queries.sql c выполненными запросами:
создание исходного потока;
таблицы общего количества отправленных сообщений;
таблицы с числом уникальных получателей для всех сообщений;
таблицы с количеством сообщений, отправленных каждым пользователем;
таблицы для агрегирования данных по каждому пользователю.

Файл Readme.md, в котором есть:
инструкция по запуску и тестированию,
тестовые данные и команды для отправки их в топик,
тестовые запросы в ksqlDB для проверки:
 общего количества отправленных сообщений;
числа уникальных получателей;


## описание

### поток messages_stream
Исходный поток, читающий JSON-сообщения из топика messages поля: user_id, recipient_id, message, timestamp

### таблицы агрегации
- total_messages_count : общее количество всех отправленных сообщений.
- unique_recipients_count : количество уникальных получателей по всем сообщениям.
- user_message_counts количество сообщений, отправленных каждым пользователем группировка по user_id
- user_unique_recipients количество уникальных получателей для каждого пользователя
- user_statistics итоговая таблица, объединяющая user_message_counts и user_unique_recipients через INNER JOIN

---

### запуск инфры
docker compose up --build -d

### отправка 5 сообщений
echo '{"user_id": "ivan", "recipient_id": "vova", "message": "Привет!", "timestamp": 1700000000000}' | docker exec -i kafka kafka-console-producer.sh --bootstrap-server kafka:9092 --topic messages
echo '{"user_id": "ivan", "recipient_id": "alex", "message": "Как дела?", "timestamp": 1700000001000}' | docker exec -i kafka kafka-console-producer.sh --bootstrap-server kafka:9092 --topic messages
echo '{"user_id": "vova", "recipient_id": "ivan", "message": "Нормально", "timestamp": 1700000002000}' | docker exec -i kafka kafka-console-producer.sh --bootstrap-server kafka:9092 --topic messages
echo '{"user_id": "ivan", "recipient_id": "vova", "message": "Ещё раз привет", "timestamp": 1700000003000}' | docker exec -i kafka kafka-console-producer.sh --bootstrap-server kafka:9092 --topic messages
echo '{"user_id": "alex", "recipient_id": "ivan", "message": "Добрый день", "timestamp": 1700000004000}' | docker exec -i kafka kafka-console-producer.sh --bootstrap-server kafka:9092 --topic messages

### проверка работы
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
и выполняем: 
SHOW TABLES; # смотрим таблицы - список табилц
SELECT * FROM total_messages_count EMIT CHANGES; # общее количество отправленных сообщений - TOTAL_COUNT = 5
SELECT * FROM unique_recipients_count EMIT CHANGES; # число уникальных получателей - UNIQUE_RECIPIENTS = 3
SELECT * FROM user_statistics EMIT CHANGES; # статистика по пользователям




