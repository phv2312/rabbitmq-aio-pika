import logging
import random
import string
from dotenv import load_dotenv
load_dotenv(".env")
from pydantic_settings import BaseSettings, SettingsConfigDict
from datetime import datetime

import asyncio

from codes.producers.simple import Producer as SimpleProducer
from codes.models.message import Message


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ProduceSettings(BaseSettings):
    model_config = SettingsConfigDict(case_sensitive=False)
    num_messages: int = 1


def generate_random_message() -> str:
    random_string = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return f"{timestamp} - {random_string}"


async def main():
    settings = ProduceSettings()
    producer = SimpleProducer()
    
    tasks: list[asyncio.Task[None]] = []
    for _ in range(settings.num_messages):
        message = Message.from_str(generate_random_message())
        tasks.append(
            asyncio.create_task(
                producer.produce(
                    message=message
                )
            )
        )
    
    logger.info("Produced %d messages", settings.num_messages)
    
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
    