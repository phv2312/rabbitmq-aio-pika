import asyncio
import logging
from abc import ABC, abstractmethod
from typing import (
    Any, 
    Awaitable, 
    Callable, 
    Generic, 
    Literal, 
    TypeVar, 
    TypedDict, 
    cast
)

import aio_pika
from aio_pika.pool import Pool
from aio_pika.abc import (
    AbstractRobustConnection,
    AbstractIncomingMessage,
    AbstractExchange,
    AbstractChannel,
    AbstractQueue,
)
from aio_pika.exceptions import ChannelPreconditionFailed
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


logger = logging.getLogger(__name__)


ClassicQueueArgs = TypedDict(
    "ClassicQueueArgs",
    {
        "x-consumer-timeout": int,
        "x-queue-type": Literal["classic"],
        "x-dead-letter-exchange": str,
        "x-dead-letter-routing-key": str,
        "x-single-active-consumer": bool,
    },
)

QuorumQueueArgs = TypedDict(
    "QuorumQueueArgs",
    {
        "x-consumer-timeout": int,
        "x-queue-type": Literal["quorum"],
        "x-delivery-limit": int,
        "x-dead-letter-exchange": str,
        "x-dead-letter-routing-key": str,
        "x-single-active-consumer": bool,
    },
)

QueueArgs = ClassicQueueArgs | QuorumQueueArgs
QueueArgsT = TypeVar("QueueArgsT", ClassicQueueArgs, QuorumQueueArgs)


class BaseRabbitMQSettings(ABC, BaseSettings, Generic[QueueArgsT]):
    model_config = SettingsConfigDict(
        case_sensitive=False,
    )

    # See https://www.rabbitmq.com/docs/consumers#acknowledgement-timeout
    ack_timeout: int = 90 * 60 * 1000
    pool_size: int = 10
    prefetch_count: int = 5
    
    # See https://www.rabbitmq.com/docs/consumers#single-active-consumer
    single_active_consumer: bool = False

    # Default will be `classic`
    # Quorum (https://www.rabbitmq.com/quorum-queues.html) is faster than classic (but trade-off is higher latency)
    # Comparasion between `classic` and `quorum`, https://www.rabbitmq.com/blog/2022/05/16/rabbitmq-3.10-performance-improvements#scenario-1-one-queue-fast-publishers-and-consumers
    queue_type: Literal["quorum", "classic"] = "classic"
    dead_letter_exchange: str = "indexing.dlx"
    dead_letter_routing_key: str = "indexing.dlx"
    
    @property
    @abstractmethod
    def queue_args(self) -> QueueArgsT:
        ...


class ClassicRabbitMQSettings(BaseRabbitMQSettings[ClassicQueueArgs]):
    queue_type: Literal["classic"] = "classic"
    
    @property
    def queue_args(self) -> ClassicQueueArgs:
        return {
            "x-consumer-timeout": self.ack_timeout,
            "x-queue-type": self.queue_type,
            "x-dead-letter-exchange": self.dead_letter_exchange,
            "x-dead-letter-routing-key": self.dead_letter_routing_key,
            "x-single-active-consumer": self.single_active_consumer,
        }
    

class QuorumRabbitMQSettings(BaseRabbitMQSettings[QuorumQueueArgs]):
    queue_type: Literal["quorum"] = "quorum"

    # See https://www.rabbitmq.com/docs/quorum-queues#position-message-handling-configuring-limit
    # Only suported in Quorum queues
    delivery_limit: int = 1
    
    @property
    def queue_args(self) -> QuorumQueueArgs:
        return {
            "x-consumer-timeout": self.ack_timeout,
            "x-queue-type": self.queue_type,
            "x-delivery-limit": self.delivery_limit,
            "x-dead-letter-exchange": self.dead_letter_exchange,
            "x-dead-letter-routing-key": self.dead_letter_routing_key,
            "x-single-active-consumer": self.single_active_consumer,
        }


class RabbitMQSettings(BaseSettings):
    settings: QuorumRabbitMQSettings | ClassicRabbitMQSettings = Field(
        discriminator="queue_type",
        default_factory=ClassicRabbitMQSettings,
    ) 


class RabbitMQ:
    def __init__(
        self,
        url: str,
        settings: RabbitMQSettings | None = None,
    ) -> None:
        self.settings = (settings or RabbitMQSettings()).settings
        self.url = url
        self.connection_pool: Pool[AbstractRobustConnection] = Pool(
            self._get_connection, max_size=self.settings.pool_size
        )
        self.channel_pool: Pool[AbstractChannel] = Pool(
            self._get_channel, max_size=self.settings.pool_size
        )

    async def _get_exchange(
        self, channel: AbstractChannel, topic: str
    ) -> AbstractExchange:
        return await channel.declare_exchange(
            topic,
            type=aio_pika.ExchangeType.DIRECT,
            durable=True,
        )

    async def _get_channel(self) -> AbstractChannel:
        async with self.connection_pool.acquire() as connection:
            return await connection.channel()

    async def _get_connection(self) -> AbstractRobustConnection:
        return await aio_pika.connect_robust(self.url)

    async def _delete_queue(self, topic: str) -> None:
        # It seeems like queue_delete also close the channels
        async with self.channel_pool.acquire() as channel:
            await channel.queue_delete(topic)

    async def _declare_queue_recursive(
        self,
        channel: AbstractChannel,
        topic: str,
        queue_args: QueueArgs,
        delete_first: bool = False,
    ) -> AbstractQueue:
        # The queue declaration may fail if the existing queue has different arguments
        # than the inputs.
        # In this case delete and re-declare the queue.
        try:
            if delete_first:
                logger.info("Deleting then re-creating queue: %s", topic)
                await self._delete_queue(topic)
            return await channel.declare_queue(
                topic, 
                durable=True, 
                arguments=cast(dict[str, Any], queue_args)
            )
        except ChannelPreconditionFailed as exc:
            if not delete_first:
                return await self._declare_queue_recursive(
                    channel,
                    topic,
                    queue_args,
                    delete_first=True,
                )
            raise exc

    async def consume(
        self,
        topic: str,
        callback: Callable[[AbstractIncomingMessage], Awaitable[Any]],
    ) -> None:
        async with self.channel_pool.acquire() as channel:
            await channel.set_qos(prefetch_count=self.settings.prefetch_count)

            queue = await self._declare_queue_recursive(
                channel, topic, self.settings.queue_args
            )

            exchange = await self._get_exchange(channel, topic)
            await queue.bind(exchange, routing_key=topic)

            logger.info(
                "Started listening at queue: %s with prefetch-count: %d, optional arguments: %s",
                topic,
                self.settings.prefetch_count,
                self.settings.queue_args,
            )

            await queue.consume(callback)
            await asyncio.Future()

    async def publish(self, topic: str, message: bytes) -> None:
        async with self.channel_pool.acquire() as channel:
            exchange = await self._get_exchange(channel, topic)

            await exchange.publish(
                aio_pika.Message(
                    body=message,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key=topic,
            )
            logger.info("Published message to %s: %s", topic, message)
            