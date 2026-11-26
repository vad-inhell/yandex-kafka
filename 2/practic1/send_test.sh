#!/bin/bash
# добавляем запрещенные слова
echo '{"word": "дурак", "action": "add"}' | docker exec -i kafka kafka-console-producer.sh --bootstrap-server kafka:9092 --topic banned_words
echo '{"word": "идиот", "action": "add"}' | docker exec -i kafka kafka-console-producer.sh --bootstrap-server kafka:9092 --topic banned_words
sleep 2

# блокируем пользователя
echo '{"user": "ivan", "blocked": "alex", "action": "block"}' | docker exec -i kafka kafka-console-producer.sh --bootstrap-server kafka:9092 --topic blocked_users
sleep 2

# отправляем сообщения"
echo '{"from_user": "ivan", "to_user": "vova", "text": "Прива, как дела?"}' | docker exec -i kafka kafka-console-producer.sh --bootstrap-server kafka:9092 --topic messages
echo '{"from_user": "vova", "to_user": "ivan", "text": "Шалом, дурак!"}' | docker exec -i kafka kafka-console-producer.sh --bootstrap-server kafka:9092 --topic messages
echo '{"from_user": "alex", "to_user": "ivan", "text": "Ivan, привет!"}' | docker exec -i kafka kafka-console-producer.sh --bootstrap-server kafka:9092 --topic messages
