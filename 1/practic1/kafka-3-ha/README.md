Описание параметров

1. Настройки режима KRaft (без ZooKeeper)
KAFKA_ENABLE_KRAFT yes #Включение режима KRaft.
KAFKA_CFG_PROCESS_ROLES broker,controller  тута указываем роли - узел хранит данные (broker) и участвует в кворуме управления метаданными (controller).
KAFKA_KRAFT_CLUSTER_ID ХХХХХХХХХ #Уникальный ID кластера. Должен быть строго одинаковым на всех 3 узлах.
KAFKA_CFG_NODE_ID 1, 2, 3 #Уникальный числовой идентификатор конкретного узла в кластере.
KAFKA_CFG_CONTROLLER_QUORUM_VOTERS 1@kafka1:9093,... #Список всех контроллеров кластера в формате ID@hostname:port. Необходим для формирования кворума.

2. Сетевые настройки (Dual Listeners)
Используются два слушателя, чтобы решить проблему маршрутизации между Docker-сетью и хостом.
KAFKA_CFG_LISTENERS PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:9094 #На каких портах контейнер слушает входящие соединения.
KAFKA_CFG_ADVERTISED_LISTENERS PLAINTEXT://kafkaX:9092,EXTERNAL://localhost:909X #Какие адреса брокер отдает клиентам. Внутри докера используется имя контейнера, снаружи — localhost с проброшенным портом.
KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP #Сопоставляет имена listener'ов с протоколами безопасности (здесь везде PLAINTEXT).
KAFKA_CFG_CONTROLLER_LISTENER_NAMES CONTROLLER #Указывает, какой listener использовать для общения между контроллерами.
KAFKA_CFG_INTER_BROKER_LISTENER_NAME PLAINTEXT #Указывает, какой listener использовать брокерам для общения друг с другом.

3. Хранилище данных топиков (Log Dirs)
KAFKA_CFG_LOG_DIRS /bitnami/kafka/data #Абсолютный путь внутри контейнера, где физически хранятся файлы партиций (сообщения). Должен совпадать с путем в volumes.
KAFKA_CFG_NUM_RECOVERY_THREADS 2 #Количество потоков для восстановления логов при перезапуске (ускоряет старт).
KAFKA_CFG_LOG_FLUSH_SCHEDULER 1000 #Как часто (в мс) Kafka принудительно сбрасывает данные на диск (1 сек = надежно, но снижает I/O производительность).
KAFKA_CFG_LOG_RETENTION_HOURS 168 #Сколько часов хранить сообщения перед удалением (168 часов = 7 дней).
KAFKA_CFG_LOG_SEGMENT_BYTES 1073741824 #Максимальный размер одного файла лога перед созданием нового (1 ГБ).
KAFKA_CFG_LOG_RETENTION_CHECK 300000 #Как часто (в мс) проверять логи на предмет удаления старых сегментов (5 минут).

4. Отказоустойчивость и Репликация
Эти параметры гарантируют, что кластер выдержит падение 1 узла из 3 без потери данных и остановки записи.
KAFKA_CFG_DEFAULT_REPLICATION_FACTOR 3 #По умолчанию создавать 3 копии каждой партиции (на всех 3 узлах).
KAFKA_CFG_MIN_INSYNC_REPLICAS 2 #Минимальное число синхронизированных копий, которые должны подтвердить запись. Ключевой параметр отказоустойчивости.
KAFKA_CFG_OFFSETS_TOPIC_REPLICATION 3 #Фактор репликации для внутреннего топика хранения смещений (offsets) консьюмеров.
KAFKA_CFG_TRANSACTION_STATE_LOG_REPL 3 #Фактор репликации для внутреннего топика состояния транзакций.
KAFKA_CFG_TRANSACTION_STATE_LOG_MIN_ISR 2 #Минимальное число реплик в синхронизации для топика транзакций.
KAFKA_CFG_NUM_PARTITIONS 3 #Количество партиций по умолчанию для новых автоматически создаваемых топиков.
KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE true #Разрешить автоматическое создание топика при первой попытке записи в него (удобно для dev/test).

5. Healthcheck и Volumes
healthcheck #Периодически выполняет команду kafka-topics.sh --list. Если команда успешна, контейнер помечается как healthy.
depends_on: condition: service_healthy #Заставляет kafka-ui ждать полной готовности всех 3 брокеров перед собственным запуском, избегая ошибок "Cluster unreachable".
volumes #Именованные тома (kafka1_data и т.д.), смонтированные  в /bitnami/kafka/data. Для сохранности данных между перезапусками.
