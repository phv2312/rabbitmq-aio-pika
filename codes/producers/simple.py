import logging
from pydantic_settings import BaseSettings
from codes.consumers.base import BaseProducer
from codes.queues.rabbitmq.core import RabbitMQ
from codes.models.message import Message


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ProducerSettings(BaseSettings):
    url: str = "amqp://localhost"
    topic: str = "producer"


class Producer(BaseProducer[Message]):
    def __init__(
        self, 
        settings: ProducerSettings | None = None
    ) -> None:
        self.settings = settings or ProducerSettings() 
        self.rabbitmq = RabbitMQ(
            url=self.settings.url
        )
        
    async def produce(
        self,
        message: Message
    ) -> None:
        await self.rabbitmq.publish(
            topic=self.settings.topic,
            message=(
                message
                .model_dump_json()
                .encode()
            )
        )
    