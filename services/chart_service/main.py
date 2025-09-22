import os
from datetime import timedelta
import io

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from tinkoff.invest import AsyncClient, CandleInterval
from tinkoff.invest.schemas import CandleSource
from tinkoff.invest.utils import now

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import mplfinance as mpf


app = FastAPI()


class ChartRequest(BaseModel):
    token: str
    ident: str  # ticker | FIGI | UID
    tf: str  # M5 | H1 | D1


async def resolve_instrument_id(token: str, ident: str) -> str:
    if len(ident) > 5:
        return ident
    async with AsyncClient(token) as client:
        resp = await client.instruments.find_instrument(query=ident)
        if resp and resp.instruments:
            chosen = None
            for ins in resp.instruments:
                if getattr(ins, "ticker", "").upper() == ident.upper():
                    chosen = ins
                    break
            chosen = chosen or resp.instruments[0]
            uid = getattr(chosen, "uid", None)
            figi = getattr(chosen, "figi", None)
            return uid or figi or ident
    return ident


def _interval_and_window(tf: str):
    tf = tf.upper()
    if tf == "M5":
        return CandleInterval.CANDLE_INTERVAL_5_MIN, 7, '5min'
    if tf == "H1":
        return CandleInterval.CANDLE_INTERVAL_HOUR, 365, 'H'
    return CandleInterval.CANDLE_INTERVAL_DAY, 365, 'D'


async def _build_dataframe(token: str, ident: str, tf: str):
    interval, days, freq = _interval_and_window(tf)
    instrument_id = await resolve_instrument_id(token, ident)
    rows = []
    times = []
    async with AsyncClient(token) as client:
        async for c in client.get_all_candles(
            instrument_id=instrument_id,
            from_=now() - timedelta(days=days),
            interval=interval,
            candle_source_type=CandleSource.CANDLE_SOURCE_EXCHANGE,
        ):
            try:
                o = c.open.units + c.open.nano / 1e9
                h = c.high.units + c.high.nano / 1e9
                l = c.low.units + c.low.nano / 1e9
                cl = c.close.units + c.close.nano / 1e9
                rows.append((o,h,l,cl))
                times.append(pd.to_datetime(getattr(c, 'time', None)))
            except Exception:
                continue
    if not rows:
        raise HTTPException(status_code=404, detail="no candles")
    df = pd.DataFrame(rows, columns=['Open','High','Low','Close']).tail(100)
    if times and len(times) >= len(df):
        times = times[-len(df):]
        try:
            df.index = pd.DatetimeIndex(times)
        except Exception:
            df.index = pd.date_range(end=pd.Timestamp.utcnow(), periods=len(df), freq=freq)
    else:
        df.index = pd.date_range(end=pd.Timestamp.utcnow(), periods=len(df), freq=freq)
    last_price = float(df['Close'].iloc[-1])
    return df, last_price, freq


@app.post("/chart")
async def chart(req: ChartRequest):
    df, last_price, _ = await _build_dataframe(req.token, req.ident, req.tf)
    fig, axlist = mpf.plot(
        df,
        type='candle', style='charles', figsize=(6,3), title='', ylabel='',
        hlines=dict(hlines=[last_price], colors='tab:red', linestyle='--', linewidths=1.0),
        tight_layout=True, returnfig=True,
    )
    try:
        ax = axlist[0]
        ax.annotate(
            f"{last_price:.2f}",
            xy=(0.0, last_price), xycoords=('axes fraction','data'),
            xytext=(6, 0), textcoords='offset points', ha='left', va='center', color='tab:red',
            bbox=dict(boxstyle='round,pad=0.2', fc='white', ec='tab:red', lw=0.5)
        )
    except Exception:
        pass

    buf = io.BytesIO()
    fig.savefig(buf, format='png', bbox_inches='tight')
    plt.close(fig)
    buf.seek(0)
    return StreamingResponse(buf, media_type='image/png')


@app.post("/chart_stats")
async def chart_stats(req: ChartRequest):
    df, last_price, _ = await _build_dataframe(req.token, req.ident, req.tf)
    min_price = float(df['Low'].min())
    max_price = float(df['High'].max())
    prev_close = None
    try:
        if len(df['Close']) >= 2:
            prev_close = float(df['Close'].iloc[-2])
    except Exception:
        prev_close = None
    return {"min": min_price, "max": max_price, "last": last_price, "prev_close": prev_close}


