import logging
import asyncio

from pydantic_settings import BaseSettings

from codes.consumers.base import BaseConsumer
from codes.models.message import Message


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ConsumerSettings(BaseSettings):
    url: str = "amqp://localhost"
    topic_simple_consumer: str = "simple_consumer"


class Consumer(BaseConsumer[Message]):
    def __init__(
        self, 
        settings: ConsumerSettings | None = None
    ) -> None:
        self.settings = settings or ConsumerSettings() 
        super().__init__(
            url=self.settings.url,
            topic=self.settings.topic_simple_consumer,
        )
        
    async def process(self, message: Message) -> None:
        logger.info("We are processing the message: %s", message)
        await asyncio.sleep(1)
        logger.info("Done")
    
    def parse(self, message: bytes) -> Message:
        return Message.model_validate_json(message)
    