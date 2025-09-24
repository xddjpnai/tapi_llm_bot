import os
import asyncio
import logging
from typing import List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, String, Float, PrimaryKeyConstraint
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

from shared.messaging.kafka import create_consumer, create_producer
from shared.topics import PORTFOLIO_UPDATED, NOTIFICATIONS_OUTBOUND
from typing import Optional


load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://postgres:postgres@postgres:5432/postgres")

app = FastAPI()

engine = create_engine(DATABASE_URL)
metadata = MetaData()
# removed candles cache (moved to chart_service)

users = Table(
    "users", metadata,
    Column("user_id", Integer, primary_key=True),
)

# removed legacy 'accounts' table (not used)

selected_accounts = Table(
    "selected_accounts", metadata,
    Column("user_id", Integer),
    Column("account_name", String),
    PrimaryKeyConstraint("user_id", "account_name"),
)

portfolio = Table(
    "portfolio", metadata,
    Column("user_id", Integer),
    Column("ticker", String),
    Column("figi", String),
    Column("quantity", Float),
    Column("expected_yield", Float, nullable=True),
    Column("expected_yield_percent", Float, nullable=True),
    Column("market", String, nullable=True),
    Column("currency", String, nullable=True),
    Column("name", String, nullable=True),
    PrimaryKeyConstraint("user_id", "ticker"),
)

settings = Table(
    "settings", metadata,
    Column("user_id", Integer, primary_key=True),
    Column("daily_summary_time", String, default="09:00"),
    Column("daily_summary_enabled", Integer, default=0),
    Column("alert_threshold_percent", Float, default=3.0),
    Column("notify_quotes", Integer, default=1),
    Column("notify_news", Integer, default=1),
)

# per-user Tinkoff token storage
tokens = Table(
    "tinkoff_tokens", metadata,
    Column("user_id", Integer, primary_key=True),
    Column("token", String),
)

metadata.create_all(engine)


def _safe_migrate_add_portfolio_columns():
    try:
        with engine.begin() as conn:
            if engine.dialect.name == "postgresql":
                conn.execute("ALTER TABLE portfolio ADD COLUMN IF NOT EXISTS expected_yield double precision")
                conn.execute("ALTER TABLE portfolio ADD COLUMN IF NOT EXISTS expected_yield_percent double precision")
                conn.execute("ALTER TABLE portfolio ADD COLUMN IF NOT EXISTS market varchar")
                conn.execute("ALTER TABLE portfolio ADD COLUMN IF NOT EXISTS currency varchar")
                conn.execute("ALTER TABLE portfolio ADD COLUMN IF NOT EXISTS name varchar")
            elif engine.dialect.name == "sqlite":
                # SQLite doesn't support IF NOT EXISTS for ADD COLUMN in older versions; try and ignore errors
                try:
                    conn.execute("ALTER TABLE portfolio ADD COLUMN expected_yield REAL")
                except Exception:
                    pass
                try:
                    conn.execute("ALTER TABLE portfolio ADD COLUMN expected_yield_percent REAL")
                except Exception:
                    pass
                for column in ("market", "currency", "name"):
                    try:
                        conn.execute(f"ALTER TABLE portfolio ADD COLUMN {column} TEXT")
                    except Exception:
                        pass
    except Exception:
        # Best-effort migration; if it fails, subsequent queries may still work if columns exist
        pass


_safe_migrate_add_portfolio_columns()


class AccountsPayload(BaseModel):
    accounts: List[str]


class SettingsPayload(BaseModel):
    daily_summary_time: str | None = None
    daily_summary_enabled: bool | None = None
    alert_threshold_percent: float | None = None
    notify_quotes: bool | None = None
    notify_news: bool | None = None

class TokenPayload(BaseModel):
    token: str


@app.post("/users/{user_id}/selected_accounts")
def set_selected_accounts(user_id: int, body: AccountsPayload):
    try:
        with engine.begin() as conn:
            # ensure user row exists
            if engine.dialect.name == "postgresql":
                conn.execute(
                    pg_insert(users).values(user_id=user_id).on_conflict_do_nothing(index_elements=[users.c.user_id])
                )
            else:
                conn.execute(users.insert().prefix_with("OR IGNORE"), {"user_id": user_id})
            conn.execute(selected_accounts.delete().where(selected_accounts.c.user_id == user_id))
            if body.accounts:
                conn.execute(selected_accounts.insert(), [
                    {"user_id": user_id, "account_name": a} for a in body.accounts
                ])
        return {"ok": True}
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/users/{user_id}/selected_accounts")
def get_selected_accounts(user_id: int):
    with engine.begin() as conn:
        rows = conn.execute(selected_accounts.select().where(selected_accounts.c.user_id == user_id)).fetchall()
    return {"accounts": [r.account_name for r in rows]}


@app.get("/users/{user_id}/portfolio")
def get_portfolio(user_id: int):
    with engine.begin() as conn:
        rows = conn.execute(portfolio.select().where(portfolio.c.user_id == user_id)).fetchall()
    return {"positions": [{
        "ticker": r.ticker,
        "figi": r.figi,
        "quantity": r.quantity,
        "expected_yield": getattr(r, "expected_yield", None),
        "expected_yield_percent": getattr(r, "expected_yield_percent", None),
        "market": getattr(r, "market", None),
        "currency": getattr(r, "currency", None),
        "name": getattr(r, "name", None),
    } for r in rows]}


@app.get("/users/{user_id}/settings")
def get_settings(user_id: int):
    try:
        with engine.begin() as conn:
            row = conn.execute(settings.select().where(settings.c.user_id == user_id)).fetchone()
            if row:
                return {
                    "daily_summary_time": row.daily_summary_time,
                    "daily_summary_enabled": bool(row.daily_summary_enabled),
                    "alert_threshold_percent": row.alert_threshold_percent,
                    "notify_quotes": bool(row.notify_quotes),
                    "notify_news": bool(row.notify_news),
                }
    except Exception:
        # fallthrough to defaults
        pass
    # Defaults if no settings yet or on error
    return {
        "daily_summary_time": "09:00",
        "daily_summary_enabled": False,
        "alert_threshold_percent": 3.0,
        "notify_quotes": True,
        "notify_news": True,
    }


@app.put("/users/{user_id}/settings")
def update_settings(user_id: int, body: SettingsPayload):
    values = {}
    if body.daily_summary_time is not None:
        values["daily_summary_time"] = body.daily_summary_time
    if body.daily_summary_enabled is not None:
        values["daily_summary_enabled"] = 1 if body.daily_summary_enabled else 0
    if body.alert_threshold_percent is not None:
        values["alert_threshold_percent"] = float(body.alert_threshold_percent)
    if body.notify_quotes is not None:
        values["notify_quotes"] = 1 if body.notify_quotes else 0
    if body.notify_news is not None:
        values["notify_news"] = 1 if body.notify_news else 0
    if not values:
        return {"ok": True}
    try:
        with engine.begin() as conn:
            if engine.dialect.name == "postgresql":
                conn.execute(
                    pg_insert(users)
                    .values(user_id=user_id)
                    .on_conflict_do_nothing(index_elements=[users.c.user_id])
                )
                conn.execute(
                    pg_insert(settings)
                    .values(user_id=user_id)
                    .on_conflict_do_nothing(index_elements=[settings.c.user_id])
                )
            else:
                conn.execute(users.insert().prefix_with("OR IGNORE"), {"user_id": user_id})
                conn.execute(settings.insert().prefix_with("OR IGNORE"), {"user_id": user_id})
            conn.execute(settings.update().where(settings.c.user_id == user_id).values(**values))
        return {"ok": True}
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/users/{user_id}/token")
def get_token(user_id: int):
    with engine.begin() as conn:
        row = conn.execute(tokens.select().where(tokens.c.user_id == user_id)).fetchone()
    return {"token": row.token if row else None}


@app.put("/users/{user_id}/token")
def put_token(user_id: int, body: TokenPayload):
    if not body.token:
        raise HTTPException(status_code=400, detail="token is required")
    try:
        with engine.begin() as conn:
            # upsert
            if engine.dialect.name == "postgresql":
                conn.execute(
                    pg_insert(tokens).values(user_id=user_id, token=body.token).on_conflict_do_update(
                        index_elements=[tokens.c.user_id], set_={"token": body.token}
                    )
                )
            else:
                conn.execute(tokens.delete().where(tokens.c.user_id == user_id))
                conn.execute(tokens.insert().values(user_id=user_id, token=body.token))
        return {"ok": True}
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


async def consume_portfolio_updates():
    cons = await create_consumer(PORTFOLIO_UPDATED, group_id="storage-service")
    prod = await create_producer()
    try:
        async for msg in cons:
            data = msg.value or {}
            user_id = data.get("user_id")
            positions = data.get("positions") or []
            correlation_id = data.get("correlation_id")
            if not user_id:
                continue
            logger.info("storage consume portfolio.updated: user_id=%s received_positions=%s corr_id=%s", user_id, len(positions), correlation_id)
            def write_rows():
                with engine.begin() as conn:
                    # очистка и вставка агрегированных позиций
                    conn.execute(portfolio.delete().where(portfolio.c.user_id == user_id))
                    agg = {}
                    for p in positions:
                        t = p.get("ticker")
                        if not t:
                            continue
                        qty = float(p.get("quantity") or 0)
                        fg = p.get("figi")
                        ey = p.get("expected_yield")
                        eyp = p.get("expected_yield_percent")
                        if t in agg:
                            agg[t]["quantity"] += qty
                            # складываем только абсолютную доходность, процент хранить как последний известный
                            try:
                                if ey is not None:
                                    agg[t]["expected_yield"] = (agg[t].get("expected_yield") or 0.0) + float(ey)
                            except Exception:
                                pass
                            if eyp is not None:
                                agg[t]["expected_yield_percent"] = float(eyp)
                        else:
                    agg[t] = {
                        "quantity": qty,
                        "figi": fg,
                        "expected_yield": float(ey) if ey is not None else None,
                        "expected_yield_percent": float(eyp) if eyp is not None else None,
                        "market": p.get("market"),
                        "currency": p.get("currency"),
                        "name": p.get("name"),
                    }
                    rows_local = [
                        {
                            "user_id": user_id,
                            "ticker": t,
                            "figi": v.get("figi"),
                            "quantity": v.get("quantity"),
                            "expected_yield": v.get("expected_yield"),
                            "expected_yield_percent": v.get("expected_yield_percent"),
                            "market": v.get("market"),
                            "currency": v.get("currency"),
                            "name": v.get("name"),
                        }
                        for t, v in agg.items()
                    ]
                    if rows_local:
                        conn.execute(portfolio.insert(), rows_local)
                    return len(rows_local)

            try:
                inserted = write_rows()
            except ProgrammingError:
                # колонок может не быть после обновления — выполним миграцию и попробуем еще раз
                _safe_migrate_add_portfolio_columns()
                inserted = write_rows()
            logger.info("storage wrote portfolio: user_id=%s inserted=%s corr_id=%s", user_id, inserted, correlation_id)
            # уведомление пользователю через Kafka
            try:
                html_msg = f"Портфель обновлен: {inserted} позиций"
                await prod.send_and_wait(
                    NOTIFICATIONS_OUTBOUND,
                    {"user_id": user_id, "items": [{"ticker": "PORTFOLIO", "html": html_msg}]},
                    key=str(user_id).encode(),
                )
            except Exception:
                logger.warning("failed to send notification for user_id=%s corr_id=%s", user_id, correlation_id)
    finally:
        await cons.stop()
        await prod.stop()


@app.on_event("startup")
async def on_startup():
    asyncio.create_task(consume_portfolio_updates())


@app.get("/notify")
def list_users_to_notify(time: str):
    with engine.begin() as conn:
        rows = conn.execute(settings.select().where(settings.c.daily_summary_enabled == 1).where(settings.c.daily_summary_time == time)).fetchall()
    return {"user_ids": [r.user_id for r in rows]}


