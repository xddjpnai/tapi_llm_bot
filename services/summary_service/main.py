import os
import asyncio
import logging
from dotenv import load_dotenv

from shared.messaging.kafka import create_consumer, create_producer
from shared.schemas import SummaryRequest, NotificationOutbound, SummaryChunk
from shared.llm.prompts import PROMPT_TICKER_SUMMARY, PROMPT_TICKER_NEWS
from shared.llm.perplexity import call_perplexity_with_citations
from shared.format import apply_citation_links_markdown_to_html, tg_bold, tg_code, chunk_text
from shared.topics import SUMMARY_REQUEST, NEWS_REQUEST, NOTIFICATIONS_OUTBOUND


load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def worker():
    cons = await create_consumer(SUMMARY_REQUEST, group_id="summary-service")
    news_cons = await create_consumer(NEWS_REQUEST, group_id="summary-service-news")
    prod = await create_producer()
    try:
        while True:
            # Обрабатываем обе очереди без starvation
            # summary
            try:
                msg = await asyncio.wait_for(cons.getone(), timeout=0.1)
                data = msg.value
            except Exception:
                data = None
            if data:
                try:
                    req = SummaryRequest.parse_obj(data)
                    logger.info("summary.request: user_id=%s tickers=%s", req.user_id, len(req.tickers))
                    items = []
                    for tkr in req.tickers:
                        if tkr in {"RUB", "RUB000UTSTOM"}:
                            continue
                        prompt = PROMPT_TICKER_SUMMARY.format(name=tkr, ticker=tkr)
                        text, cites = call_perplexity_with_citations(prompt)
                        html = apply_citation_links_markdown_to_html(text, cites)
                        for ch in chunk_text(html, 3500):
                            items.append(SummaryChunk(ticker=tkr, html=f"📊 {tg_bold(tkr)} ({tg_code(tkr)})\n{ch}"))
                    out = NotificationOutbound(user_id=req.user_id, items=items)
                    await prod.send_and_wait(NOTIFICATIONS_OUTBOUND, out.dict(), key=str(req.user_id).encode())
                except Exception:
                    logger.exception("summary processing failed")
            # новости
            # news
            try:
                nmsg = await asyncio.wait_for(news_cons.getone(), timeout=0.1)
            except Exception:
                nmsg = None
            if nmsg:
                nd = nmsg.value
                user_id = nd.get("user_id")
                tickers = nd.get("tickers") or []
                logger.info("news.request: user_id=%s tickers=%s", user_id, len(tickers))
                items = []
                for tkr in tickers:
                    if tkr in {"RUB", "RUB000UTSTOM"}:
                        continue
                    prompt = PROMPT_TICKER_NEWS.format(name=tkr, ticker=tkr)
                    text, cites = call_perplexity_with_citations(prompt)
                    if not text or text.startswith("Perplexity API error") or text == "Perplexity API key not configured.":
                        # Fallback: даем кликабельные ссылки на поисковики новостей
                        g = f"https://news.google.com/search?q={tkr}"
                        y = f"https://news.search.yahoo.com/search?p={tkr}"
                        html = f"📰 {tg_bold(tkr)} ({tg_code(tkr)})\n• <a href=\"{g}\">Google News</a> • <a href=\"{y}\">Yahoo News</a>"
                        items.append(SummaryChunk(ticker=tkr, html=html))
                    else:
                        html = apply_citation_links_markdown_to_html(text, cites)
                        for ch in chunk_text(html, 3500):
                            items.append(SummaryChunk(ticker=tkr, html=f"📰 {tg_bold(tkr)} ({tg_code(tkr)})\n{ch}"))
                if items:
                    await prod.send_and_wait(NOTIFICATIONS_OUTBOUND, {"user_id": user_id, "items": [i.dict() for i in items]}, key=str(user_id).encode())
    finally:
        await cons.stop()
        await news_cons.stop()
        await prod.stop()


def main():
    asyncio.run(worker())


if __name__ == "__main__":
    main()


