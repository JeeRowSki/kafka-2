import faust


class Message(faust.Record, serializer="json"):
    message_id: str
    sender_id: str
    recipient_id: str
    text: str


class BlockEvent(faust.Record, serializer="json"):
    user_id: str
    blocked_user_id: str
    action: str


class WordEvent(faust.Record, serializer="json"):
    word: str
    action: str
