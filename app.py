
import os
import logging

import faust

from models import Message, BlockEvent, WordEvent
from filters import censor_text, normalize_word


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

logger = logging.getLogger("message-processor")

BROKERS = os.getenv(
    "KAFKA_BROKERS",
    "kafka://kafka-0:9092,kafka-1:9092,kafka-2:9092",
)

app = faust.App(
    "message-filter-service",
    broker=BROKERS,
    store="rocksdb://",
    datadir="/app/data",
    topic_partitions=3,
)

messages_topic = app.topic(
    "messages",
    key_type=str,
    value_type=Message,
)

blocked_topic = app.topic(
    "blocked_users",
    key_type=str,
    value_type=BlockEvent,
)

words_topic = app.topic(
    "banned_words",
    key_type=str,
    value_type=WordEvent,
)

filtered_topic = app.topic(
    "filtered_messages",
    key_type=str,
    value_type=Message,
)

blocked_table = app.Table(
    "blocked_users_state",
    default=list,
    partitions=3,
)

words_table = app.GlobalTable(
    "banned_words_state",
    default=bool,
    partitions=3,
)


@app.agent(blocked_topic)
async def process_blocks(stream):
    async for event in stream:
        user_id = event.user_id
        blocked_id = event.blocked_user_id

        blocked = set(blocked_table[user_id])

        if event.action == "block":
            blocked.add(blocked_id)

        elif event.action == "unblock":
            blocked.discard(blocked_id)

        else:
            logger.warning(
                "Unknown block action: %s",
                event.action,
            )
            continue

        blocked_table[user_id] = sorted(blocked)

        logger.info(
            "BLOCK UPDATE user=%s blocked=%s action=%s",
            user_id,
            blocked_id,
            event.action,
        )


@app.agent(words_topic)
async def process_words(stream):
    async for event in stream:
        word = normalize_word(event.word)

        if not word:
            continue

        if event.action == "add":
            words_table[word] = True

        elif event.action == "remove":
            words_table[word] = False

        else:
            logger.warning(
                "Unknown word action: %s",
                event.action,
            )
            continue

        logger.info(
            "WORD UPDATE word=%s action=%s",
            word,
            event.action,
        )


@app.agent(messages_topic)
async def process_messages(stream):
    async for message in stream:
        blocked = blocked_table[message.recipient_id]

        if message.sender_id in blocked:
            logger.info(
                "MESSAGE BLOCKED id=%s sender=%s recipient=%s",
                message.message_id,
                message.sender_id,
                message.recipient_id,
            )
            continue

        banned = {
            word
            for word, enabled in words_table.items()
            if enabled
        }

        clean_text = censor_text(message.text, banned)

        filtered_message = Message(
            message_id=message.message_id,
            sender_id=message.sender_id,
            recipient_id=message.recipient_id,
            text=clean_text,
        )

        await filtered_topic.send(
            key=message.recipient_id,
            value=filtered_message,
        )

        logger.info(
            "MESSAGE PROCESSED id=%s sender=%s recipient=%s text=%s",
            message.message_id,
            message.sender_id,
            message.recipient_id,
            clean_text,
        )


if __name__ == "__main__":
    app.main()

