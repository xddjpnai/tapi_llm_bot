import os
import asyncio
from dotenv import load_dotenv

from shared.messaging.kafka import create_consumer, create_producer
from shared.topics import QUOTES_REQUEST, QUOTES_UPDATED
from shared.integrations.tinkoff_client import TinkoffClient


load_dotenv()


async def worker():
    cons = await create_consumer(QUOTES_REQUEST, group_id="quotes-service")
    prod = await create_producer()
    try:
        async for msg in cons:
            data = msg.value or {}
            user_id = data.get("user_id")
            figis = data.get("figis") or []
            token_override = data.get("token")
            client = TinkoffClient(token_override)
            prices = {}
            try:
                prices = await client.get_last_prices(figis)
            except Exception:
                pass
            await prod.send_and_wait(QUOTES_UPDATED, {
                "user_id": user_id,
                "prices": prices,
            }, key=str(user_id).encode() if user_id else None)
    finally:
        await cons.stop()
        await prod.stop()


def main():
    asyncio.run(worker())


if __name__ == "__main__":
    main()


