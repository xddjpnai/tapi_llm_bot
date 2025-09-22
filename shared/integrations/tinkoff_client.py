from datetime import timedelta
from typing import Dict, List
import logging

from tinkoff.invest import AsyncClient, CandleInterval
from tinkoff.invest.schemas import CandleSource
from tinkoff.invest.utils import now


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TinkoffClient:
    def __init__(self, token: str | None = None):
        if not token:
            raise ValueError("Tinkoff token is required")
        self.token = token

    @staticmethod
    def _to_float(value) -> float | None:
        try:
            if value is None:
                return None
            units = getattr(value, "units", None)
            nano = getattr(value, "nano", None)
            if units is not None and nano is not None:
                return float(units) + float(nano) / 1e9
        except Exception:
            pass
        try:
            return float(value)
        except Exception:
            return None

    async def get_accounts(self):
        async with AsyncClient(self.token) as client:
            try:
                response = await client.users.get_accounts()
                accounts = []
                for acc in response.accounts:
                    name = getattr(acc, "name", None) or getattr(acc, "id", None) or "Account"
                    accounts.append({
                        "id": getattr(acc, "id", None) or getattr(acc, "account_id", None) or name,
                        "name": name,
                    })
                return accounts
            except Exception as e:
                logger.warning("Не удалось получить аккаунты: %s", e)
                return []

    async def _resolve_account_ids_by_names(self, target_names: list) -> list:
        if not target_names:
            return []
        all_accounts = await self.get_accounts()
        wanted = set(target_names)
        return [a["id"] for a in all_accounts if a.get("name") in wanted]

    async def get_portfolio(self, accounts):
        if not accounts:
            return []
        if isinstance(accounts, str):
            accounts_list = [accounts]
        else:
            accounts_list = list(accounts)
        account_ids: list = []
        try:
            if any(not a.isalnum() for a in accounts_list):
                account_ids = await self._resolve_account_ids_by_names(accounts_list)
            else:
                resolved = await self._resolve_account_ids_by_names(accounts_list)
                account_ids = resolved if resolved else accounts_list
        except Exception:
            account_ids = accounts_list

        result = []
        async with AsyncClient(self.token) as client:
            for account_id in account_ids:
                try:
                    portfolio = await client.operations.get_portfolio(account_id=account_id)
                    for p in portfolio.positions:
                        qty = 0.0
                        try:
                            units = getattr(p.quantity, "units", 0)
                            nano = getattr(p.quantity, "nano", 0)
                            qty = float(units) + float(nano) / 1e9
                        except Exception:
                            qty = float(getattr(p, "quantity", 0) or 0)
                        ticker_like = getattr(p, "ticker", None) or getattr(p, "instrument", None)
                        figi = getattr(p, "figi", None)
                        ticker_value = ticker_like or figi or "UNKNOWN"
                        # expected yield (absolute and percent) if available
                        exp_y = getattr(p, "expected_yield", None)
                        exp_y_val = self._to_float(exp_y)
                        exp_pct = getattr(p, "expected_yield_percent", None)
                        exp_pct_val = self._to_float(exp_pct)
                        result.append({
                            "ticker": ticker_value,
                            "quantity": qty,
                            "figi": figi,
                            "expected_yield": exp_y_val,
                            "expected_yield_percent": exp_pct_val,
                        })
                except Exception as e:
                    logger.warning("Не удалось получить портфель для %s: %s", account_id, e)
                    continue
        return result

    async def get_last_prices(self, figi_list: list):
        if not figi_list:
            return {}
        async with AsyncClient(self.token) as client:
            prices = await client.market_data.get_last_prices(figi=figi_list)
            return {p.figi: float(p.price.units + p.price.nano / 1e9) for p in prices.last_prices}

    # Removed unused helpers: get_instruments_short, resolve_figi_by_tickers, get_candles


