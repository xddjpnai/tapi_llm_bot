import asyncio
import logging
from contextlib import suppress
from typing import Iterable

from dotenv import load_dotenv

from shared.messaging.kafka import create_consumer, create_producer
from shared.schemas import SummaryRequest, NotificationOutbound, SummaryChunk
from shared.llm.prompts import PROMPT_TICKER_SUMMARY, PROMPT_TICKER_NEWS, PROMPT_DAILY
from shared.llm.perplexity import call_perplexity_with_citations
from shared.format import apply_citation_links_markdown_to_html, tg_bold, tg_code, chunk_text
from shared.topics import SUMMARY_REQUEST, NEWS_REQUEST, NOTIFICATIONS_OUTBOUND, DAILY_REQUEST


load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _filter_tickers(raw: Iterable[str]) -> list[str]:
    return [t for t in raw if t and t not in {"RUB", "RUB000UTSTOM"}]


def _is_llm_error(text: str | None) -> bool:
    if not text:
        return True
    return text.startswith("Perplexity API error") or text == "Perplexity API key not configured."


async def _send_notifications(prod, user_id: int, items: list[SummaryChunk]):
    if not user_id or not items:
        return
    payload = {"user_id": user_id, "items": [i.dict() for i in items]}
    await prod.send_and_wait(NOTIFICATIONS_OUTBOUND, payload, key=str(user_id).encode())


async def _handle_summary(cons, prod):
    async for msg in cons:
        data = msg.value or {}
        try:
            req = SummaryRequest.parse_obj(data)
        except Exception:
            logger.exception("summary.request parse failed: %s", data)
            continue
        tickers = _filter_tickers(req.tickers)
        if not tickers:
            logger.info("summary.request: user_id=%s skipped (no tickers)", req.user_id)
            continue
        logger.info("summary.request: user_id=%s tickers=%s", req.user_id, len(tickers))
        items: list[SummaryChunk] = []
        for tkr in tickers:
            prompt = PROMPT_TICKER_SUMMARY.format(name=tkr, ticker=tkr)
            text, cites = call_perplexity_with_citations(prompt)
            if _is_llm_error(text):
                logger.warning("summary.request: LLM error for %s: %s", tkr, text)
                continue
            html = apply_citation_links_markdown_to_html(text, cites)
            for ch in chunk_text(html, 3500):
                items.append(SummaryChunk(ticker=tkr, html=f"📊 {tg_bold(tkr)} ({tg_code(tkr)})\n{ch}"))
        if not items:
            logger.info("summary.request: user_id=%s produced no items", req.user_id)
            continue
        out = NotificationOutbound(user_id=req.user_id, items=items)
        await prod.send_and_wait(NOTIFICATIONS_OUTBOUND, out.dict(), key=str(req.user_id).encode())


async def _handle_news(cons, prod):
    async for msg in cons:
        data = msg.value or {}
        user_id = data.get("user_id")
        tickers = _filter_tickers(data.get("tickers") or [])
        if not user_id or not tickers:
            logger.info("news.request: skip (user=%s tickers=%s)", user_id, tickers)
            continue
        logger.info("news.request: user_id=%s tickers=%s", user_id, len(tickers))
        items: list[SummaryChunk] = []
        for tkr in tickers:
            prompt = PROMPT_TICKER_NEWS.format(name=tkr, ticker=tkr)
            text, cites = call_perplexity_with_citations(prompt)
            if _is_llm_error(text):
                g = f"https://news.google.com/search?q={tkr}"
                y = f"https://news.search.yahoo.com/search?p={tkr}"
                html = f"📰 {tg_bold(tkr)} ({tg_code(tkr)})\n• <a href=\"{g}\">Google News</a> • <a href=\"{y}\">Yahoo News</a>"
                items.append(SummaryChunk(ticker=tkr, html=html))
                continue
            html = apply_citation_links_markdown_to_html(text, cites)
            for ch in chunk_text(html, 3500):
                items.append(SummaryChunk(ticker=tkr, html=f"📰 {tg_bold(tkr)} ({tg_code(tkr)})\n{ch}"))
        await _send_notifications(prod, user_id, items)


async def _handle_daily(cons, prod):
    async for msg in cons:
        data = msg.value or {}
        user_id = data.get("user_id")
        tickers = _filter_tickers(data.get("tickers") or [])
        if not user_id:
            logger.info("daily.request: skip message without user_id")
            continue
        logger.info("daily.request: user_id=%s tickers=%s", user_id, len(tickers))
        prompt = PROMPT_DAILY.format(tickers=", ".join(tickers[:20]))
        text, cites = call_perplexity_with_citations(prompt)
        if _is_llm_error(text):
            html = "📊 Ежедневная сводка: нет данных от LLM. Попробуйте позже."
            items = [SummaryChunk(ticker="DAILY", html=html)]
        else:
            html = apply_citation_links_markdown_to_html(text, cites)
            items = [SummaryChunk(ticker="DAILY", html=chunk) for chunk in chunk_text(html, 3500)]
        await _send_notifications(prod, user_id, items)


async def worker():
    summary_cons = await create_consumer(SUMMARY_REQUEST, group_id="summary-service")
    news_cons = await create_consumer(NEWS_REQUEST, group_id="summary-service-news")
    daily_cons = await create_consumer(DAILY_REQUEST, group_id="summary-service-daily")
    prod = await create_producer()

    tasks = [
        asyncio.create_task(_handle_summary(summary_cons, prod), name="summary-consumer"),
        asyncio.create_task(_handle_news(news_cons, prod), name="news-consumer"),
        asyncio.create_task(_handle_daily(daily_cons, prod), name="daily-consumer"),
    ]

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("summary_service worker crashed")
        raise
    finally:
        for task in tasks:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        await summary_cons.stop()
        await news_cons.stop()
        await daily_cons.stop()
        await prod.stop()


def main():
    asyncio.run(worker())


if __name__ == "__main__":
    main()


