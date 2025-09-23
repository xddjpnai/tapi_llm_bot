import os
import asyncio
import aiohttp
import uuid
from aiogram import Bot, Dispatcher, types
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from dotenv import load_dotenv

from shared.messaging.kafka import create_producer, create_consumer
from shared.schemas import SummaryRequest, NotificationOutbound
from shared.topics import SUMMARY_REQUEST, NOTIFICATIONS_OUTBOUND, QUOTES_REQUEST, QUOTES_UPDATED, PORTFOLIO_REQUEST, NEWS_REQUEST, PORTFOLIO_UPDATED
from shared.format import tg_bold, tg_code


load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is required")

bot = Bot(TELEGRAM_BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(bot)

# Кеш имен счетов на время сессии редактирования
accounts_cache: dict[int, list[str]] = {}
# Ожидания обновления портфеля по correlation_id
pending_portfolio_updates: dict[str, asyncio.Future] = {}


main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📁 Портфель"), KeyboardButton(text="📊 Сводка")],
        [KeyboardButton(text="💹 Котировки"), KeyboardButton(text="📰 Новости")],
        [KeyboardButton(text="⚙ Счета"), KeyboardButton(text="🛠 Настройки"), KeyboardButton(text="🔑 Токен")],
    ],
    resize_keyboard=True,
)


STORAGE_URL = os.getenv("STORAGE_URL", "http://localhost:8000")
CHART_URL = os.getenv("CHART_URL", "http://localhost:8002")


@dp.message_handler(commands=["start"])
async def start_cmd(message: types.Message):
    first = (message.from_user.first_name or "").strip()
    hello = f"Привет, {first}!" if first else "Привет!"
    # check token
    user_id = message.from_user.id
    user_token: str | None = None
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{STORAGE_URL}/users/{user_id}/token") as resp:
                if resp.status < 300:
                    j = await resp.json()
                    user_token = (j or {}).get("token")
        except Exception:
            pass
    if not user_token:
        link = "https://www.tbank.ru/invest/settings/api/"
        await message.answer(
            f"{hello} Чтобы подключить Тинькофф, нажмите ‘🔑 Токен’ и пришлите ваш токен.\nПосле сохранения токена нажмите ‘⚙ Счета’ и выберите счета — затем портфель загрузится автоматически.\n\nГде взять токен: <a href=\"{link}\">страница API Т‑банка</a>",
            reply_markup=main_kb,
        )
    else:
        await message.answer(
            f"{hello} Сначала выберите счета в меню ‘⚙ Счета’, затем портфель загрузится автоматически.",
            reply_markup=main_kb,
        )


@dp.message_handler(lambda m: m.text == "📊 Сводка")
async def summary_request(message: types.Message):
    user_id = message.from_user.id
    # Запросим портфель у storage API
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{STORAGE_URL}/users/{user_id}/portfolio") as resp:
            data = await resp.json()
    tickers = [p.get("ticker") for p in data.get("positions", []) if p.get("ticker") and p.get("ticker") not in {"RUB","RUB000UTSTOM"}]
    if not tickers:
        await message.answer("Портфель пуст. Добавьте счета и загрузите позиции.")
        return
    prod = await create_producer()
    try:
        req = SummaryRequest(user_id=user_id, tickers=tickers[:20]).dict()
        await prod.send_and_wait(SUMMARY_REQUEST, req)
        await message.answer("Запросил сводку…")
    finally:
        await prod.stop()


@dp.message_handler(lambda m: m.text == "📰 Новости")
async def show_news(message: types.Message):
    user_id = message.from_user.id
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{STORAGE_URL}/users/{user_id}/portfolio") as resp:
                data = await resp.json()
        except Exception:
            data = {"positions": []}
    tickers = [p.get("ticker") for p in data.get("positions", []) if p.get("ticker") and p.get("ticker") not in {"RUB","RUB000UTSTOM"}]
    if not tickers:
        await message.answer("Нет бумаг для новостей.")
        return
    # Быстрый фолбек-сообщение со ссылками, чтобы пользователь сразу получил ответ
    links = []
    for t in tickers[:5]:
        g = f"https://news.google.com/search?q={t}"
        y = f"https://news.search.yahoo.com/search?p={t}"
        links.append(f"📰 {tg_bold(t)} — <a href=\"{g}\">Google</a> • <a href=\"{y}\">Yahoo</a>")
    await message.answer("\n".join(links) + "\n\nГотовлю подробную выжимку…")
    # Отправим асинхронно задачу на LLM
    prod = await create_producer()
    try:
        corr_id = str(uuid.uuid4())
        await prod.send_and_wait(NEWS_REQUEST, {"user_id": user_id, "tickers": tickers[:20], "correlation_id": corr_id}, key=str(user_id).encode())
    finally:
        await prod.stop()


def build_accounts_kb(all_accounts: list[str], selected: set[str]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    for name in all_accounts:
        mark = "✅ " if name in selected else ""
        kb.add(InlineKeyboardButton(text=f"{mark}{name}", callback_data=f"acc_toggle:{name}"))
    kb.add(InlineKeyboardButton(text="✅ Готово", callback_data="acc_done"))
    return kb


# Команда /token больше не используется — оставим на случай старых пользователей, покажем подсказку про кнопку
@dp.message_handler(commands=["token"])
async def set_token(message: types.Message):
    await message.reply("Сейчас обновление токена делается через кнопку ‘🔑 Токен’. Нажмите её и пришлите токен ответом.")


@dp.message_handler(lambda m: m.text == "🔑 Токен")
async def update_token_request(message: types.Message):
    from aiogram.types import ForceReply
    await message.answer("Пришлите новый TINKOFF_API_TOKEN ответом на это сообщение.", reply_markup=ForceReply(selective=True))


@dp.message_handler(lambda m: m.reply_to_message and ("TINKOFF_API_TOKEN" in (m.reply_to_message.text or "") or "Токен" in (m.reply_to_message.text or "")))
async def update_token_receive(message: types.Message):
    user_id = message.from_user.id
    token = (message.text or "").strip()
    if not token or " " in token:
        await message.reply("Некорректный токен. Пришлите только строку токена без пробелов.")
        return
    async with aiohttp.ClientSession() as session:
        async with session.put(f"{STORAGE_URL}/users/{user_id}/token", json={"token": token}) as resp:
            if resp.status >= 300:
                txt = await resp.text()
                await message.reply(f"Не удалось сохранить токен: {txt}")
                return
    await message.reply("Токен обновлен ✅\nТеперь нажмите ‘⚙ Счета’ и выберите счета, чтобы видеть бумаги и графики.", reply_markup=main_kb)


@dp.message_handler(lambda m: m.text == "⚙ Счета")
async def edit_accounts(message: types.Message):
    user_id = message.from_user.id
    # Получим пользовательский токен из storage (если есть)
    user_token: str | None = None
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{STORAGE_URL}/users/{user_id}/token") as resp:
            if resp.status < 300:
                j = await resp.json()
                user_token = (j or {}).get("token")
    # Список доступных счетов получаем из Tinkoff (через shared интеграцию)
    from shared.integrations.tinkoff_client import TinkoffClient
    if not user_token:
        await message.answer("Сначала сохраните токен: нажмите ‘🔑 Токен’")
        return
    client = TinkoffClient(user_token)
    accounts = await client.get_accounts()
    names = [a.get("name") for a in accounts if a.get("name")]
    accounts_cache[user_id] = names[:]
    if not names:
        await message.answer("Не удалось получить список счетов. Проверьте токен и права доступа в Тинькофф.")
        return
    # Текущий выбор из storage
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{STORAGE_URL}/users/{user_id}/selected_accounts") as resp:
            sel = await resp.json()
    selected = set(sel.get("accounts", []))
    await message.answer("Выберите счета (нажмите '✅ Готово' для сохранения)", reply_markup=build_accounts_kb(names, selected))


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("acc_toggle:"))
async def accounts_toggle(call: types.CallbackQuery):
    user_id = call.from_user.id
    name = call.data.split(":", 1)[1]
    # Имя кнопки уже в разметке — не дергаем API, берем из текущей клавиатуры
    names = accounts_cache.get(user_id) or []
    if not names:
        kb_rows = call.message.reply_markup.inline_keyboard if call.message.reply_markup else []
        for row in kb_rows:
            for btn in row:
                text = (btn.text or "").strip()
                if getattr(btn, "callback_data", "") == "acc_done":
                    continue
                if text.startswith("✅ "):
                    text = text.replace("✅ ", "", 1)
                if text:
                    names.append(text)
    if not names:
        await call.answer("Список счетов пуст", show_alert=True)
        return
    # Считаем текущее состояние выбора из разметки клавиатуры, а не из storage
    btns = call.message.reply_markup.inline_keyboard if call.message.reply_markup else []
    selected: set[str] = set()
    for row in btns:
        for btn in row:
            text = btn.text or ""
            if text.startswith("✅ "):
                selected.add(text.replace("✅ ", "", 1))
    # Переключим выбранность
    if name in selected:
        selected.remove(name)
    else:
        selected.add(name)
    # Перерисуем клавиатуру
    try:
        await call.message.edit_reply_markup(reply_markup=build_accounts_kb(names, selected))
    except Exception:
        pass
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "acc_done")
async def accounts_done(call: types.CallbackQuery):
    user_id = call.from_user.id
    # Считаем текущую клавиатуру, чтобы собрать выбранные
    btns = call.message.reply_markup.inline_keyboard if call.message.reply_markup else []
    chosen: list[str] = []
    for row in btns:
        for btn in row:
            # игнорируем саму кнопку завершения
            if getattr(btn, "callback_data", "") == "acc_done":
                continue
            text = btn.text or ""
            if text.startswith("✅ "):
                chosen.append(text.replace("✅ ", "", 1))
    # Если выбранных нет, но есть кеш, возьмем пересечение кеша с отмеченными в разметке (на случай скрытых строк)
    async with aiohttp.ClientSession() as session:
        async with session.post(f"{STORAGE_URL}/users/{user_id}/selected_accounts", json={"accounts": chosen}) as resp:
            _ = await resp.text()
        # Получим токен
        async with session.get(f"{STORAGE_URL}/users/{user_id}/token") as resp2:
            tok_json = await resp2.json()
        # Получим актуальный список после сохранения (на случай гонора клавиатуры)
        async with session.get(f"{STORAGE_URL}/users/{user_id}/selected_accounts") as resp3:
            saved = await resp3.json()
    accounts = saved.get("accounts", [])
    user_token = (tok_json or {}).get("token")
    if not accounts:
        await call.message.answer("Счета сохранены, но список пуст. Выберите счета повторно.")
    else:
        if not user_token:
            await call.message.answer("Счета сохранены. Для загрузки портфеля сохраните токен: нажмите ‘🔑 Токен’.")
        else:
            prod = await create_producer()
            try:
                corr_id = str(uuid.uuid4())
                payload = {"user_id": user_id, "accounts": accounts, "correlation_id": corr_id, "token": user_token}
                print(f"[portfolio.request] user={user_id} accounts={accounts} corr={corr_id}")
                # Prepare future before sending
                fut = asyncio.get_event_loop().create_future()
                pending_portfolio_updates[corr_id] = fut
                await prod.send_and_wait(PORTFOLIO_REQUEST, payload, key=str(user_id).encode())
            finally:
                await prod.stop()
            await call.message.answer("Счета сохранены. Загружаю портфель…")
            # Wait for correlated portfolio.updated or timeout, then render current portfolio
            try:
                try:
                    await asyncio.wait_for(pending_portfolio_updates.get(corr_id), timeout=15.0)
                except Exception:
                    pass
                await render_and_send_portfolio(call.message.chat.id, user_id)
            except Exception:
                pass
            await call.answer()


async def render_and_send_portfolio(chat_id: int, user_id: int):
    try:
        # Получим свежий портфель (после PORTFOLIO_UPDATED он уже записан в storage)
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{STORAGE_URL}/users/{user_id}/portfolio") as resp_pf:
                data_pf = await resp_pf.json()
        positions: list = data_pf.get("positions", [])

        # Подготовим агрегаты
        total_all_time_abs = 0.0
        total_all_time_abs_any = False
        sec_positions = []
        rub_lines = []
        for p in positions:
            t = p.get("ticker") or "?"
            fg = p.get("figi") or ""
            qty = p.get("quantity") or 0
            ey = p.get("expected_yield")
            if t in {"RUB","RUB000UTSTOM"}:
                rub_lines.append((t, fg, qty))
                continue
            try:
                if ey is not None:
                    total_all_time_abs += float(ey)
                    total_all_time_abs_any = True
            except Exception:
                pass
            sec_positions.append((t, fg, qty))

        # Получим токен и рассчитаем цены и дневную динамику
        last_by_figi: dict[str, float] = {}
        daily_abs = None
        daily_pct = None
        current_value = None
        user_token: str | None = None
        async with aiohttp.ClientSession() as s_tok:
            async with s_tok.get(f"{STORAGE_URL}/users/{user_id}/token") as rtk:
                if rtk.status < 300:
                    tok_json = await rtk.json()
                    user_token = (tok_json or {}).get("token")
        if user_token and sec_positions:
            prev_sum = 0.0
            last_sum = 0.0
            async with aiohttp.ClientSession() as s_stats:
                for t, fg, qty in sec_positions:
                    if not fg:
                        continue
                    try:
                        async with s_stats.post(f"{CHART_URL}/chart_stats", json={"token": user_token, "ident": fg, "tf": "D1"}) as rs2:
                            if rs2.status == 200:
                                st2 = await rs2.json()
                                lv = st2.get("last")
                                pv = st2.get("prev_close")
                                if lv is not None:
                                    last_by_figi[fg] = float(lv)
                                    last_sum += float(lv) * float(qty)
                                if pv is not None:
                                    prev_sum += float(pv) * float(qty)
                    except Exception:
                        continue
            if last_sum > 0 and prev_sum > 0:
                daily_abs = round(last_sum - prev_sum, 2)
                daily_pct = round((daily_abs / prev_sum) * 100.0, 2)
                current_value = last_sum
            elif last_sum > 0:
                current_value = last_sum

        # Итоги и отправка
        all_time_abs = round(total_all_time_abs, 2) if total_all_time_abs_any else None
        all_time_pct = None
        if all_time_abs is not None and current_value and (current_value - all_time_abs) > 0:
            all_time_pct = round((all_time_abs / (current_value - all_time_abs)) * 100.0, 2)
        lines = []
        for t, fg, qty in sec_positions:
            price_txt = tg_code(last_by_figi.get(fg)) if fg in last_by_figi else "н/д"
            lines.append(f"📁 {tg_bold(t)}\n• Количество: {tg_code(qty)}\n• Цена: {price_txt}")
        for t, fg, qty in rub_lines:
            lines.append(f"💰 {tg_bold('Рубли (RUB)')}\n• Остаток: {tg_code(qty)}")
        if lines:
            header = "Портфель:"
            change_lines = ""
            if daily_abs is not None:
                change_lines += f"\n• Суточное изм.: {tg_code(daily_abs)}" + (f" ({tg_code(daily_pct)}%)" if daily_pct is not None else "")
            if all_time_abs is not None:
                change_lines += f"\n• Изм. за всё время: {tg_code(all_time_abs)}" + (f" ({tg_code(all_time_pct)}%)" if all_time_pct is not None else "")
            kb = InlineKeyboardMarkup().add(InlineKeyboardButton(text="✖ Закрыть", callback_data="delete_msg"))
            await bot.send_message(chat_id, f"{header}{change_lines}\n\n" + "\n\n".join(lines), reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass


@dp.message_handler(lambda m: m.text == "📁 Портфель")
async def show_portfolio(message: types.Message):
    user_id = message.from_user.id
    try:
        # Всегда пытаемся обновить портфель перед выводом
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{STORAGE_URL}/users/{user_id}/selected_accounts") as resp_sel:
                sel = await resp_sel.json()
            async with session.get(f"{STORAGE_URL}/users/{user_id}/token") as resp_tok:
                tok_json = await resp_tok.json()
        accounts = (sel or {}).get("accounts", [])
        user_token = (tok_json or {}).get("token")
        if accounts and user_token:
            prod = await create_producer()
            corr_id = str(uuid.uuid4())
            try:
                fut = asyncio.get_event_loop().create_future()
                pending_portfolio_updates[corr_id] = fut
                await prod.send_and_wait(PORTFOLIO_REQUEST, {"user_id": user_id, "accounts": accounts, "correlation_id": corr_id, "token": user_token}, key=str(user_id).encode())
            finally:
                await prod.stop()
            await message.answer("Обновляю портфель…")
            try:
                try:
                    await asyncio.wait_for(pending_portfolio_updates.get(corr_id), timeout=15.0)
                except Exception:
                    pass
                await render_and_send_portfolio(message.chat.id, user_id)
                return
            except Exception:
                pass
        # Fallback: если нет токена/счетов — просто покажем текущее состояние
        await render_and_send_portfolio(message.chat.id, user_id)
    except Exception:
        await render_and_send_portfolio(message.chat.id, user_id)


def build_settings_kb(s: dict) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=3)
    daily_on = bool(s.get("daily_summary_enabled"))
    notify_quotes = bool(s.get("notify_quotes"))
    notify_news = bool(s.get("notify_news"))
    time_val = (s.get("daily_summary_time") or "09:00").strip()
    kb.add(InlineKeyboardButton(text=f"Ежедневная сводка: {'Вкл' if daily_on else 'Выкл'}", callback_data="settings_toggle:daily_summary_enabled"))
    for_times = ["09:00", "12:00", "18:00"]
    kb.row(*(InlineKeyboardButton(text=(("✅ " if t == time_val else "") + t), callback_data=f"settings_time:{t}") for t in for_times))
    kb.row(
        InlineKeyboardButton(text=f"Котировки: {'Вкл' if notify_quotes else 'Выкл'}", callback_data="settings_toggle:notify_quotes"),
        InlineKeyboardButton(text=f"Новости: {'Вкл' if notify_news else 'Выкл'}", callback_data="settings_toggle:notify_news"),
    )
    return kb


@dp.message_handler(lambda m: m.text == "🛠 Настройки")
async def settings_menu(message: types.Message):
    user_id = message.from_user.id
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{STORAGE_URL}/users/{user_id}/settings") as resp:
                if resp.status >= 300:
                    txt = await resp.text()
                    await message.answer(f"Ошибка получения настроек: {txt}")
                    return
                s = await resp.json()
        kb = build_settings_kb(s)
        await message.answer("Настройки ежедневной сводки и уведомлений:", reply_markup=kb)
    except Exception:
        await message.answer("Не удалось получить настройки. Попробуйте позже.")


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("settings_toggle:"))
async def settings_toggle(call: types.CallbackQuery):
    user_id = call.from_user.id
    field = call.data.split(":", 1)[1]
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{STORAGE_URL}/users/{user_id}/settings") as resp:
            s = await resp.json()
        new_val = not bool(s.get(field))
        await session.put(f"{STORAGE_URL}/users/{user_id}/settings", json={field: new_val})
        async with session.get(f"{STORAGE_URL}/users/{user_id}/settings") as resp2:
            s2 = await resp2.json()
    try:
        await call.message.edit_reply_markup(reply_markup=build_settings_kb(s2))
    except Exception:
        pass
    await call.answer()


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("settings_time:"))
async def settings_time(call: types.CallbackQuery):
    user_id = call.from_user.id
    time_val = call.data.split(":", 1)[1]
    async with aiohttp.ClientSession() as session:
        await session.put(f"{STORAGE_URL}/users/{user_id}/settings", json={"daily_summary_time": time_val})
        async with session.get(f"{STORAGE_URL}/users/{user_id}/settings") as resp2:
            s2 = await resp2.json()
    try:
        await call.message.edit_reply_markup(reply_markup=build_settings_kb(s2))
    except Exception:
        pass
    await call.answer()


# Удалены элементы управления порогом изменения; коллбек не используется


@dp.message_handler(lambda m: m.text == "💹 Котировки")
async def show_quotes(message: types.Message):
    user_id = message.from_user.id
    # Получим фигишки из портфеля
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{STORAGE_URL}/users/{user_id}/portfolio") as resp:
            data = await resp.json()
        async with session.get(f"{STORAGE_URL}/users/{user_id}/token") as resp2:
            tok_json = await resp2.json()
    positions = data.get("positions", [])
    user_token = (tok_json or {}).get("token")
    figis = [p.get("figi") for p in positions if p.get("figi") and p.get("ticker") not in {"RUB","RUB000UTSTOM"}]
    if not figis:
        await message.answer("Нет бумаг для котировок.")
        return
    prod = await create_producer()
    try:
        payload = {"user_id": user_id, "figis": figis, "token": user_token}
        await prod.send_and_wait(QUOTES_REQUEST, payload, key=str(user_id).encode())
        await message.answer("Запросил котировки…")
    finally:
        await prod.stop()


async def notifications_consumer():
    cons = await create_consumer(NOTIFICATIONS_OUTBOUND, group_id="bot-gateway")
    try:
        async for msg in cons:
            data = msg.value
            try:
                payload = NotificationOutbound.parse_obj(data)
            except Exception:
                continue
            # простая трассировка
            try:
                print(f"[notif] to={payload.user_id} items={len(payload.items)}")
            except Exception:
                pass
            for item in payload.items:
                kb = InlineKeyboardMarkup().add(InlineKeyboardButton(text="✖ Закрыть", callback_data="delete_msg"))
                await bot.send_message(payload.user_id, item.html, reply_markup=kb)
    finally:
        await cons.stop()


async def quotes_consumer():
    cons = await create_consumer(QUOTES_UPDATED, group_id="bot-gateway")
    try:
        async for msg in cons:
            data = msg.value or {}
            user_id = data.get("user_id")
            prices = data.get("prices", {})
            if not user_id or not prices:
                continue
            # Для отображения подтянем портфель (чтобы знать тикеры и кол-ва)
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{STORAGE_URL}/users/{user_id}/portfolio") as resp:
                    pf = await resp.json()
            pos = pf.get("positions", [])
            # Получим токен пользователя для построения графиков
            user_token: str | None = None
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{STORAGE_URL}/users/{user_id}/token") as resp:
                    if resp.status < 300:
                        tok_json = await resp.json()
                        user_token = (tok_json or {}).get("token")
            # По одной бумаге — один блок: текст + график (D1 по умолчанию)
            for p in pos:
                fg = p.get("figi")
                t = p.get("ticker")
                if not fg or t in {"RUB","RUB000UTSTOM"}:
                    continue
                qty = p.get("quantity") or 0
                price = prices.get(fg)
                # Получим статистику диапазона для выбранного TF по умолчанию (D1)
                minmax_txt = ""
                try:
                    if user_token:
                        async with aiohttp.ClientSession() as s2:
                            async with s2.post(f"{CHART_URL}/chart_stats", json={"token": user_token, "ident": fg, "tf": "D1"}) as r3:
                                if r3.status == 200:
                                    st = await r3.json()
                                    minmax_txt = f"\n• Мин/Макс (D1): {tg_code(st.get('min'))} / {tg_code(st.get('max'))}"
                except Exception:
                    pass
                price_txt = "н/д" if price is None else tg_code(price)
                caption = f"💹 {tg_bold(t)}\n• Кол-во: {tg_code(qty)}\n• Цена: {price_txt}{minmax_txt}"
                kb = InlineKeyboardMarkup().row(
                    InlineKeyboardButton(text="M5", callback_data=f"chart:{fg}:M5"),
                    InlineKeyboardButton(text="H1", callback_data=f"chart:{fg}:H1"),
                    InlineKeyboardButton(text="D1", callback_data=f"chart:{fg}:D1"),
                ).add(InlineKeyboardButton(text="✖ Закрыть", callback_data="delete_msg"))
                if user_token:
                    try:
                        async with aiohttp.ClientSession() as s2:
                            async with s2.post(f"{CHART_URL}/chart", json={"token": user_token, "ident": fg, "tf": "D1"}) as r2:
                                if r2.status == 200:
                                    img = await r2.read()
                                    await bot.send_photo(user_id, img, caption=caption, reply_markup=kb, parse_mode="HTML")
                                else:
                                    await bot.send_message(user_id, caption, reply_markup=kb, parse_mode="HTML")
                    except Exception:
                        await bot.send_message(user_id, caption, reply_markup=kb, parse_mode="HTML")
                else:
                    await bot.send_message(user_id, caption, reply_markup=kb, parse_mode="HTML")
    finally:
        await cons.stop()


async def portfolio_updated_consumer():
    cons = await create_consumer(PORTFOLIO_UPDATED, group_id="bot-gateway")
    try:
        async for msg in cons:
            data = msg.value or {}
            corr_id = data.get("correlation_id")
            if not corr_id:
                continue
            fut = pending_portfolio_updates.pop(corr_id, None)
            if fut and not fut.done():
                try:
                    fut.set_result(True)
                except Exception:
                    pass
    finally:
        await cons.stop()


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("chart:"))
async def chart_handler(call: types.CallbackQuery):
    try:
        _, ident, tf = call.data.split(":", 2)
    except ValueError:
        await call.answer()
        return
    user_id = call.from_user.id
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{STORAGE_URL}/users/{user_id}/token") as resp:
            tok_json = await resp.json()
    user_token = (tok_json or {}).get("token")
    if not user_token:
        await call.answer("Нет токена. Обновите через кнопку ‘🔑 Токен’", show_alert=True)
        return
    try:
        async with aiohttp.ClientSession() as s2:
            async with s2.post(f"{CHART_URL}/chart", json={"token": user_token, "ident": ident, "tf": tf}) as r2:
                if r2.status != 200:
                    await call.answer("Не удалось построить график", show_alert=True)
                    return
                content = await r2.read()
        kb = InlineKeyboardMarkup().row(
            InlineKeyboardButton(text="M5", callback_data=f"chart:{ident}:M5"),
            InlineKeyboardButton(text="H1", callback_data=f"chart:{ident}:H1"),
            InlineKeyboardButton(text="D1", callback_data=f"chart:{ident}:D1"),
        ).add(InlineKeyboardButton(text="✖ Закрыть", callback_data="delete_msg"))
        # Соберем подпись: Тикер, Кол-во, Цена (last для выбранного TF), Мин/Макс (TF)
        caption = ""
        try:
            t = "?"
            qty = 0
            # тянем портфель, чтобы найти тикер и количество по FIGI
            async with aiohttp.ClientSession() as s_info:
                async with s_info.get(f"{STORAGE_URL}/users/{user_id}/portfolio") as rpf:
                    pf = await rpf.json()
            for p in pf.get("positions", []):
                if p.get("figi") == ident:
                    t = p.get("ticker") or t
                    qty = p.get("quantity") or 0
                    break
            last_val = None
            tf_min = None
            tf_max = None
            async with aiohttp.ClientSession() as s3:
                # Берем last/min/max для выбранного TF
                try:
                    async with s3.post(f"{CHART_URL}/chart_stats", json={"token": user_token, "ident": ident, "tf": tf}) as r_stats:
                        if r_stats.status == 200:
                            st = await r_stats.json()
                            last_val = st.get("last")
                            tf_min = st.get("min")
                            tf_max = st.get("max")
                except Exception:
                    pass
            price_txt = "н/д" if last_val is None else tg_code(last_val)
            minmax_part = "" if tf_min is None or tf_max is None else f"\n• Мин/Макс ({tf}): {tg_code(tf_min)} / {tg_code(tf_max)}"
            caption = f"💹 {tg_bold(t)}\n• Кол-во: {tg_code(qty)}\n• Цена: {price_txt}{minmax_part}"
        except Exception:
            caption = ""
        await bot.send_photo(call.message.chat.id, content, caption=caption, reply_markup=kb, parse_mode="HTML")
    except Exception:
        await call.message.answer("Не удалось построить график")
    finally:
        await call.answer()


@dp.callback_query_handler(lambda c: c.data == "delete_msg")
async def delete_message(call: types.CallbackQuery):
    try:
        await call.message.delete()
    except Exception:
        try:
            await call.answer("Не удалось удалить сообщение", show_alert=True)
        except Exception:
            pass
    else:
        try:
            await call.answer()
        except Exception:
            pass


def main():
    from aiogram import executor

    async def on_startup(_dp):
        loop = asyncio.get_event_loop()
        loop.create_task(notifications_consumer())
        loop.create_task(quotes_consumer())
        loop.create_task(portfolio_updated_consumer())

    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)


if __name__ == "__main__":
    main()


