from dotenv import load_dotenv
load_dotenv(".env")

import logging
import asyncio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    from codes.consumers.simple import Consumer

    consumer = Consumer()

    await asyncio.gather(
        *[
            consumer.run()
        ]
    )
    

if __name__ == "__main__":
    asyncio.run(main())
