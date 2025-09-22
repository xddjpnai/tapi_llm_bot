import os
import asyncio
import logging
from dotenv import load_dotenv

from shared.messaging.kafka import create_consumer, create_producer
from shared.topics import PORTFOLIO_REQUEST, PORTFOLIO_UPDATED
from shared.integrations.tinkoff_client import TinkoffClient


load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def worker():
    cons = await create_consumer(PORTFOLIO_REQUEST, group_id="portfolio-service")
    prod = await create_producer()
    try:
        async for msg in cons:
            data = msg.value or {}
            user_id = data.get("user_id")
            accounts = data.get("accounts") or []
            correlation_id = data.get("correlation_id")
            token_override = data.get("token")
            logger.info("portfolio.request: user_id=%s accounts=%s corr_id=%s", user_id, accounts, correlation_id)
            try:
                client = TinkoffClient(token_override)
                portfolio = await client.get_portfolio(accounts)
            except Exception:
                portfolio = []
            logger.info("portfolio.updated: user_id=%s positions=%s corr_id=%s", user_id, len(portfolio), correlation_id)
            await prod.send_and_wait(
                PORTFOLIO_UPDATED,
                {
                    "user_id": user_id,
                    "positions": portfolio,
                    "correlation_id": correlation_id,
                },
                key=str(user_id).encode() if user_id else None,
            )
    finally:
        await cons.stop()
        await prod.stop()


def main():
    asyncio.run(worker())


if __name__ == "__main__":
    main()


