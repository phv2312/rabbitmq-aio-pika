from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from pydantic import BaseModel
from aio_pika.abc import AbstractIncomingMessage

from codes.queues.rabbitmq.core import RabbitMQ, RabbitMQSettings


ConsumerT = TypeVar("ConsumerT", bound=BaseModel)
ProducerT = TypeVar("ProducerT", bound=BaseModel)


class BaseConsumer(ABC, Generic[ConsumerT]):
    def __init__(
        self,
        url: str,
        topic: str,
    ):
        self.queue = RabbitMQ(
            url, 
        )
        self.topic = topic
    
    @abstractmethod
    async def process(self, message: ConsumerT) -> None:
        ...
    
    @abstractmethod
    def parse(self, message: bytes) -> ConsumerT:
        ...
    
    async def consume(self, message: AbstractIncomingMessage) -> None:
        async with message.process():
            await self.process(self.parse(message.body))
    
    async def run(self) -> None:
        await self.queue.consume(
            self.topic, 
            self.consume
        )
    

class BaseProducer(ABC, Generic[ProducerT]):
    @abstractmethod
    async def produce(self, message: ProducerT) -> None:
        raise NotImplementedError