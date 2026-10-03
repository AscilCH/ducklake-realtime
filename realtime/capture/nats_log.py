"""EventLog on NATS JetStream."""


class NatsLog:
    def __init__(self, js):
        self._js = js

    async def publish(self, subject: str, payload: bytes, message_id: str) -> None:
        await self._js.publish(subject, payload, headers={"Nats-Msg-Id": message_id})
