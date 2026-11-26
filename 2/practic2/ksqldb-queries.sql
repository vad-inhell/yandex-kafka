-- читсаем с самого начала топика
SET 'auto.offset.reset' = 'earliest';

-- исходный поток
CREATE STREAM messages_stream (
    user_id VARCHAR,
    recipient_id VARCHAR,
    message VARCHAR,
    timestamp BIGINT
) WITH (
    KAFKA_TOPIC = 'messages',
    VALUE_FORMAT = 'JSON',
    TIMESTAMP = 'timestamp'
);

-- общее количество сообщений
CREATE TABLE total_messages_count AS
    SELECT 'all' AS dummy, COUNT(*) AS total_count
    FROM messages_stream
    GROUP BY 'all'
    EMIT CHANGES;

-- количество уникальных получателей (ARRAY_LENGTH + COLLECT_SET)
CREATE TABLE unique_recipients_count AS
    SELECT 'all' AS dummy, ARRAY_LENGTH(COLLECT_SET(recipient_id)) AS unique_recipients
    FROM messages_stream
    GROUP BY 'all'
    EMIT CHANGES;

-- соообщения от каждого пользователя
CREATE TABLE user_message_counts AS
    SELECT user_id, COUNT(*) AS sent_messages
    FROM messages_stream
    GROUP BY user_id
    EMIT CHANGES;

-- уникальные получатели для каждого пользователя
CREATE TABLE user_unique_recipients AS
    SELECT user_id, ARRAY_LENGTH(COLLECT_SET(recipient_id)) AS unique_recipients
    FROM messages_stream
    GROUP BY user_id
    EMIT CHANGES;

-- итоговая таблица статистики
CREATE TABLE user_statistics AS
    SELECT m.user_id, m.sent_messages, r.unique_recipients
    FROM user_message_counts m
    INNER JOIN user_unique_recipients r
    ON m.user_id = r.user_id
    EMIT CHANGES;