import faust
import re

#app faust
app = faust.App(
    'stream-filter-app',
    broker='kafka://kafka:9092',
    store='memory://',  # персистентное хранилище таблиц в памяти
)

#1 модели данных сериализация/десериализация json
class BlockEvent(faust.Record):
    user: str
    blocked: str
    action: str

class BannedWordEvent(faust.Record):
    word: str
    action: str

class MessageEvent(faust.Record):
    from_user: str  # from_user вместо зарезервированного слова from
    to_user: str
    text: str

#2 определение топиков
messages_topic = app.topic('messages', value_type=MessageEvent)
blocked_users_topic = app.topic('blocked_users', value_type=BlockEvent)
banned_words_topic = app.topic('banned_words', value_type=BannedWordEvent)
filtered_messages_topic = app.topic('filtered_messages', value_type=MessageEvent)

#3 персистентные таблицы хранилище состояний
# default=lambda: false и partitions=1 обязательно
blocked_table = app.Table('blocked_users', default=lambda: False, partitions=1)
banned_words_table = app.Table('banned_words', default=lambda: False, partitions=1)

#4 агенты потоковой обработки
#агент1 управление списком блокирово
@app.agent(blocked_users_topic)
async def handle_blocks(stream):
    async for event in stream:
        key = f"{event.user}:{event.blocked}"
        if event.action == 'block':
            blocked_table[key] = True
            print(f"[BLOCK] Пользователь {event.user} заблокировал {event.blocked}")
        else:
            blocked_table[key] = False
            print(f"[UNBLOCK] Пользователь {event.user} разблокировал {event.blocked}")

#агент2 динамическое обновление запрещенных слов
@app.agent(banned_words_topic)
async def handle_banned_words(stream):
    async for event in stream:
        word = event.word.lower()
        if event.action == 'add':
            banned_words_table[word] = True
            print(f"[BANNED] Добавлено запрещенное слово: '{word}'")
        else:
            banned_words_table[word] = False
            print(f"[BANNED] Удалено запрещенное слово: '{word}'")

#агент3: фильтрация и цензура сообщений
@app.agent(messages_topic)
async def handle_messages(stream):
    async for event in stream:
#ключ проверки: "получатель:отправитель"
        key = f"{event.to_user}:{event.from_user}"
#1 проверка блокировки: если заблокирован, прерываем обработку
        if blocked_table[key]:
            print(f"[FILTER] Сообщение от {event.from_user} к {event.to_user} ЗАБЛОКИРОВАНО и отброшено")
            continue  # сообщение не попадет в filtered_messages
#2 цензура маскировка запрещенных слов
        censored_text = event.text
        for word, is_banned in banned_words_table.items():
            if is_banned:
#регулярное выражение для замены целых слов
                pattern = r'\b' + re.escape(word) + r'\b'
                censored_text = re.sub(pattern, '***', censored_text, flags=re.IGNORECASE)
        if censored_text != event.text:
            print(f"[CENSOR] Текст изменен: '{event.text}' -> '{censored_text}'")
#3 отправка обработанного сообщения в итоговый топик
        await filtered_messages_topic.send(
            value=MessageEvent(from_user=event.from_user, to_user=event.to_user, text=censored_text)
        )
        print(f"[OK] Сообщение от {event.from_user} к {event.to_user} успешно обработано: '{censored_text}'")

if __name__ == '__main__':
    app.main()
