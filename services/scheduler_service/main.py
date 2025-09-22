import os
import asyncio
from datetime import datetime

import aiohttp
from dotenv import load_dotenv

from shared.messaging.kafka import create_producer
from shared.topics import SUMMARY_REQUEST


load_dotenv()

STORAGE_URL = os.getenv("STORAGE_URL", "http://storage_service:8000")


async def tick_once():
    now_str = datetime.now().strftime("%H:%M")
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{STORAGE_URL}/notify", params={"time": now_str}) as resp:
            data = await resp.json()
        user_ids = data.get("user_ids", [])
        if not user_ids:
            return
        # Для каждого пользователя получим список тикеров
        prod = await create_producer()
        try:
            for uid in user_ids:
                async with session.get(f"{STORAGE_URL}/users/{uid}/portfolio") as pr:
                    pd = await pr.json()
                tickers = [p.get("ticker") for p in pd.get("positions", []) if p.get("ticker") and p.get("ticker") not in {"RUB","RUB000UTSTOM"}]
                if not tickers:
                    continue
                await prod.send_and_wait(SUMMARY_REQUEST, {"user_id": uid, "tickers": tickers[:20]})
        finally:
            await prod.stop()


async def run_forever():
    while True:
        try:
            await tick_once()
        except Exception:
            pass
        await asyncio.sleep(60)


def main():
    asyncio.run(run_forever())


if __name__ == "__main__":
    main()


