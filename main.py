import asyncio
import json
import logging
import os
import sqlite3
import uuid
from typing import Tuple

from aiogram import Bot, Dispatcher, F, types
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    WebAppInfo,
    LabeledPrice,
)

# =========================
# CONFIG
# =========================
# Лучше хранить токены в переменных окружения:
# export BOT_TOKEN="..."
# export PROVIDER_TOKEN="..."
BOT_TOKEN = os.getenv("BOT_TOKEN", "8137546517:AAGno-CJPZ9C8-bbC7KccoGhPHaGiQZCMdw")
PROVIDER_TOKEN = os.getenv("PROVIDER_TOKEN", "381764678:TEST:161391")

# URL мини-приложения (сайт)
WEBAPP_URL_BASE = "https://www.boostt.ru/"
# Параметр, который сайт читает как баланс
WEBAPP_BALANCE_PARAM = "tgBalance"

# =========================
# LOGGING
# =========================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =========================
# BOT
# =========================
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# =========================
# SIMPLE DB (SQLite)
# =========================
DB_PATH = os.getenv("DB_PATH", "/opt/tgbot/scalingo_bot_boost/data/bot_data.db")
_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
_conn.execute(
    """
    CREATE TABLE IF NOT EXISTS balances (
      user_id INTEGER PRIMARY KEY,
      balance_kopecks INTEGER NOT NULL DEFAULT 0
    )
    """
)
_conn.commit()


def _get_balance_kopecks(user_id: int) -> int:
    row = _conn.execute(
        "SELECT balance_kopecks FROM balances WHERE user_id = ?",
        (user_id,),
    ).fetchone()
    return int(row[0]) if row else 0


def _set_balance_kopecks(user_id: int, value: int) -> None:
    value = max(0, int(value))
    _conn.execute(
        "INSERT INTO balances (user_id, balance_kopecks) VALUES (?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET balance_kopecks=excluded.balance_kopecks",
        (user_id, value),
    )
    _conn.commit()


def _add_balance_kopecks(user_id: int, delta: int) -> int:
    cur = _get_balance_kopecks(user_id)
    new_val = cur + int(delta)
    _set_balance_kopecks(user_id, new_val)
    return new_val


def _try_debit_balance_kopecks(user_id: int, amount: int) -> Tuple[bool, int, int]:
    """
    Возвращает:
      ok, balance_before, balance_after
    """
    amount = int(amount)
    before = _get_balance_kopecks(user_id)
    if before < amount:
        return False, before, before
    after = before - amount
    _set_balance_kopecks(user_id, after)
    return True, before, after


def _format_rub_from_kopecks(v: int) -> str:
    return f"{v // 100} ₽" if v % 100 == 0 else f"{v / 100:.2f} ₽"


def _webapp_url_for_user(user_id: int) -> str:
    bal_k = _get_balance_kopecks(user_id)
    bal_rub = bal_k / 100.0
    return f"{WEBAPP_URL_BASE}?{WEBAPP_BALANCE_PARAM}={bal_rub:.2f}"


# =========================
# TEMP ORDER STORAGE
# =========================
# В проде лучше БД, но оставляем как у вас.
user_orders = {}
awaiting_custom_topup = set()  # user_id, которые ввели "другая сумма"


# =========================
# UI BUILDERS
# =========================
def main_menu_kb(user_id: int) -> InlineKeyboardMarkup:
    """
    Главное меню без кнопок "Аккаунты" и "Рассылка".
    """
    webapp_url = _webapp_url_for_user(user_id)
    keyboard = [
        [
            InlineKeyboardButton(
                text="📈 Накрутка",
                web_app=WebAppInfo(url=webapp_url),
            ),
        ],
        [
            InlineKeyboardButton(text="💳 Баланс", callback_data="balance_menu"),
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def topup_amounts_kb(need_rub: int = 0) -> InlineKeyboardMarkup:
    need = int(need_rub or 0)
    keyboard = [
        [
            InlineKeyboardButton(text="100 ₽", callback_data="topup_amount_100"),
            InlineKeyboardButton(text="300 ₽", callback_data="topup_amount_300"),
            InlineKeyboardButton(text="500 ₽", callback_data="topup_amount_500"),
        ],
        [
            InlineKeyboardButton(text="1000 ₽", callback_data="topup_amount_1000"),
            InlineKeyboardButton(text="Другая сумма", callback_data="topup_amount_custom"),
        ],
        [
            InlineKeyboardButton(text="🪙 Крипта", callback_data=f"topup_crypto_{need}"),
        ],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_main")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def open_webapp_kb(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🚀 Открыть приложение",
                    web_app=WebAppInfo(url=_webapp_url_for_user(user_id)),
                )
            ]
        ]
    )


# =========================
# TOPUP UI (SINGLE ENTRY)
# =========================
async def show_topup_amounts(chat_id: int, user_id: int, need_rub: int = 0) -> None:
    """
    Всегда показываем сразу выбор суммы.
    """
    bal = _get_balance_kopecks(user_id)
    need_line = f"\n\nНе хватает: <b>{int(need_rub)} ₽</b>" if need_rub and need_rub > 0 else ""
    text = (
        "💳 <b>Пополнить баланс</b>\n\n"
        f"Текущий баланс: <b>{_format_rub_from_kopecks(bal)}</b>"
        f"{need_line}\n\n"
        "Выберите сумму пополнения:"
    )
    await bot.send_message(
        chat_id=chat_id,
        text=text,
        reply_markup=topup_amounts_kb(need_rub=need_rub),
        parse_mode=ParseMode.HTML,
    )


# =========================
# COMMANDS
# =========================
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    """
    Поддержка deep-link:
      /start topup
      /start topup_need_123
    """
    user_id = message.from_user.id
    payload = ""
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            payload = parts[1].strip()

    if payload.startswith("topup"):
        need_rub = 0
        if payload.startswith("topup_need_"):
            try:
                need_rub = int(payload.replace("topup_need_", "").strip())
            except Exception:
                need_rub = 0
        await show_topup_amounts(message.chat.id, user_id, need_rub=need_rub)
        return

    welcome_text = """🚀 Приветствую!

Готовы к росту? 🎯

💳 Ваш баланс: <b>{balance}</b>
""".format(balance=_format_rub_from_kopecks(_get_balance_kopecks(user_id)))

    logger.info("Отправка сообщения с кнопками.")

    try:
        await message.answer_photo(
            types.FSInputFile("leeee.png"),
            caption=welcome_text,
            reply_markup=main_menu_kb(user_id),
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        logger.error(f"Ошибка отправки фото: {e}")
        await message.answer(
            welcome_text,
            reply_markup=main_menu_kb(user_id),
            parse_mode=ParseMode.HTML,
        )


@dp.message(Command("balance"))
async def cmd_balance(message: types.Message):
    user_id = message.from_user.id
    bal = _get_balance_kopecks(user_id)
    await message.answer(
        f"💳 Ваш баланс: <b>{_format_rub_from_kopecks(bal)}</b>",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="➕ Пополнить", callback_data="balance_topup")],
                [InlineKeyboardButton(text="🚀 Открыть приложение", web_app=WebAppInfo(url=_webapp_url_for_user(user_id)))],
            ]
        ),
        parse_mode=ParseMode.HTML,
    )


# =========================
# MAIN MENU CALLBACKS
# =========================
@dp.callback_query(lambda c: c.data == "balance_menu")
async def balance_menu_callback(callback: types.CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    bal = _get_balance_kopecks(user_id)
    text = (
        f"💳 <b>Баланс</b>\n\n"
        f"Текущий баланс: <b>{_format_rub_from_kopecks(bal)}</b>\n\n"
        f"Выберите действие:"
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="➕ Пополнить баланс", callback_data="balance_topup")],
            [InlineKeyboardButton(text="🚀 Открыть приложение", web_app=WebAppInfo(url=_webapp_url_for_user(user_id)))],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="back_main")],
        ]
    )
    await callback.message.answer(text, reply_markup=kb, parse_mode=ParseMode.HTML)


@dp.callback_query(lambda c: c.data == "balance_topup")
async def balance_topup_callback(callback: types.CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    await show_topup_amounts(callback.message.chat.id, user_id, need_rub=0)


# =========================
# TOPUP ROUTER (AMOUNTS + CRYPTO)
# =========================
@dp.callback_query(lambda c: c.data.startswith("topup_"))
async def topup_router(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    data = callback.data

    await callback.answer()

    if data.startswith("topup_crypto_"):
        need = int(data.replace("topup_crypto_", "") or 0)
        extra = f"\n\nРекомендуемая сумма: <b>{need} ₽</b>" if need > 0 else ""
        await callback.message.answer(
            "🪙 <b>Пополнение криптой</b>\n\n"
            "1) Отправьте USDT (TRC20) на адрес:\n"
            "<code>TXYZxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx</code>\n\n"
            "2) После оплаты напишите в поддержку и пришлите TXID.\n"
            "📞 Поддержка: @walter_belyi"
            + extra,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="◀️ Назад", callback_data="back_main")],
                    [InlineKeyboardButton(text="🚀 Открыть приложение", web_app=WebAppInfo(url=_webapp_url_for_user(user_id)))],
                ]
            ),
        )
        return

    if data.startswith("topup_amount_"):
        amount = data.replace("topup_amount_", "")

        if amount == "custom":
            awaiting_custom_topup.add(user_id)
            await callback.message.answer(
                "Введите сумму пополнения в рублях (например: <b>250</b>).",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="back_main")]]
                ),
            )
            return

        try:
            amount_rub = int(amount)
        except Exception:
            amount_rub = 0

        if amount_rub <= 0:
            await callback.message.answer("❌ Некорректная сумма.")
            return

        await send_topup_invoice(chat_id=user_id, user_id=user_id, amount_rub=amount_rub, reason="Пополнение баланса")
        return


@dp.message(F.text)
async def custom_topup_amount_handler(message: types.Message):
    """
    Принимаем сумму после "Другая сумма".
    """
    user_id = message.from_user.id
    if user_id not in awaiting_custom_topup:
        return

    raw = (message.text or "").strip().replace(",", ".")
    try:
        amount = int(float(raw))
    except Exception:
        amount = 0

    if amount <= 0:
        await message.answer("❌ Введите число больше 0 (например: 250).")
        return

    awaiting_custom_topup.discard(user_id)
    await send_topup_invoice(chat_id=message.chat.id, user_id=user_id, amount_rub=amount, reason="Пополнение баланса")


# =========================
# TOPUP INVOICE
# =========================
async def send_topup_invoice(chat_id: int, user_id: int, amount_rub: int, reason: str = "Пополнение") -> None:
    """
    ВАЖНО: если получите PAYMENT_PROVIDER_INVALID — значит PROVIDER_TOKEN не привязан к этому боту
    в @BotFather -> Bot Settings -> Payments.
    """
    if not PROVIDER_TOKEN:
        raise RuntimeError(
            "PROVIDER_TOKEN is not set. Configure payments in @BotFather -> Payments and set PROVIDER_TOKEN env var."
        )

    amount_kopecks = int(amount_rub) * 100
    order_id = str(uuid.uuid4())

    user_orders[order_id] = {
        "type": "topup",
        "user_id": user_id,
        "amount": amount_kopecks,
        "reason": reason,
    }

    prices = [LabeledPrice(label=f"💳 {reason}", amount=amount_kopecks)]

    await bot.send_invoice(
        chat_id=chat_id,
        title="💳 Пополнение баланса",
        description=f"{reason}\nПосле оплаты баланс обновится автоматически.",
        payload=order_id,
        provider_token=PROVIDER_TOKEN,
        currency="RUB",
        prices=prices,
        start_parameter="topup_balance",
    )


# =========================
# PRE-CHECKOUT + SUCCESS PAYMENT
# =========================
@dp.pre_checkout_query()
async def pre_checkout_handler(pre_checkout_query: types.PreCheckoutQuery):
    order_id = pre_checkout_query.invoice_payload
    if order_id in user_orders:
        await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)
    else:
        await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=False, error_message="Заказ не найден")


@dp.message(lambda message: message.successful_payment is not None)
async def successful_payment_handler(message: types.Message):
    payment = message.successful_payment
    order_id = payment.invoice_payload
    order = user_orders.get(order_id)

    if not order:
        await message.answer("❌ Ошибка обработки оплаты. Напишите в поддержку: @walter_belyi")
        return

    if order.get("type") == "topup":
        user_id = int(order["user_id"])
        amount = int(order["amount"])
        new_bal = _add_balance_kopecks(user_id, amount)

        text = (
            "✅ <b>Баланс пополнен!</b>\n\n"
            f"Сумма: <b>{_format_rub_from_kopecks(amount)}</b>\n"
            f"Текущий баланс: <b>{_format_rub_from_kopecks(new_bal)}</b>\n\n"
            "Откройте приложение — баланс отобразится автоматически."
        )
        await message.answer(text, reply_markup=open_webapp_kb(user_id), parse_mode=ParseMode.HTML)

        user_orders.pop(order_id, None)
        return

    await message.answer("✅ Оплата прошла успешно.", parse_mode=ParseMode.HTML)
    user_orders.pop(order_id, None)


# =========================
# WEBAPP DATA (ORDER FROM SITE)
# =========================
@dp.message(F.web_app_data)
async def webapp_data_handler(message: types.Message):
    """
    Сайт отправляет tg.sendData(JSON.stringify(payload))
    Мы обрабатываем:
      - open_topup (показать выбор сумм сразу)
      - pay_with_balance (списать с внутреннего баланса)
    """
    user_id = message.from_user.id
    try:
        data = json.loads(message.web_app_data.data)
    except Exception:
        await message.answer("❌ Не удалось прочитать данные заказа.")
        return

    action = data.get("action")

    # Открыть пополнение (кнопка "+" или "Пополнить" на сайте)
    if action == "open_topup":
        try:
            need_rub = int(float(data.get("need_rub", 0) or 0))
        except Exception:
            need_rub = 0
        await show_topup_amounts(message.chat.id, user_id, need_rub=need_rub)
        return

    # Оплата заказов внутренним балансом
    if action != "pay_with_balance":
        await message.answer("ℹ️ Команда получена.")
        return

    # сумма заказа в рублях (строка вида '40.00')
    try:
        total_rub = float(str(data.get("total_price", "0")).replace(",", "."))
    except Exception:
        total_rub = 0.0

    amount_kopecks = int(round(total_rub * 100))
    ok, before, after = _try_debit_balance_kopecks(user_id, amount_kopecks)

    order_id = data.get("order_id", "—")
    title = data.get("title", "Заказ")
    qty = data.get("quantity", data.get("qty", "—"))
    platform = data.get("platform", "—")
    service = data.get("service", "—")
    category_name = data.get("category_name") or data.get("categoryName") or data.get("category") or "—"

    if not ok:
        need = max(0, amount_kopecks - before)
        need_rub = int((need + 99) // 100)

        text = (
            "❌ <b>Недостаточно средств на балансе</b>\n\n"
            f"Баланс: <b>{_format_rub_from_kopecks(before)}</b>\n"
            f"Нужно: <b>{_format_rub_from_kopecks(amount_kopecks)}</b>\n"
            f"Не хватает: <b>{_format_rub_from_kopecks(need)}</b>\n\n"
            "Выберите сумму пополнения:"
        )
        await message.answer(text, parse_mode=ParseMode.HTML)
        await show_topup_amounts(message.chat.id, user_id, need_rub=need_rub)
        return

    # успех списания
    text = (
        "✅ <b>Оплата списана с баланса</b>\n\n"
        f"Заказ: <b>{title}</b>\n"
        f"ID: <code>{order_id}</code>\n"
        f"Платформа: <b>{platform}</b>\n"
        f"Услуга: <b>{service}</b>\n"
        f"Категория: <b>{category_name}</b>\n"
        f"Кол-во: <b>{qty}</b>\n"
        f"Сумма: <b>{_format_rub_from_kopecks(amount_kopecks)}</b>\n\n"
        f"Баланс: <b>{_format_rub_from_kopecks(after)}</b>\n\n"
        "Для выполнения заказа свяжитесь с менеджером: @walter_belyi"
    )
    await message.answer(text, reply_markup=open_webapp_kb(user_id), parse_mode=ParseMode.HTML)


# =========================
# BACK TO MAIN
# =========================
@dp.callback_query(lambda c: c.data == "back_main")
async def back_main_callback(callback: types.CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    welcome_text = """🚀 Приветствую!

Готовы к росту? 🎯

💳 Ваш баланс: <b>{balance}</b>
""".format(balance=_format_rub_from_kopecks(_get_balance_kopecks(user_id)))

    await callback.message.answer(
        welcome_text,
        reply_markup=main_menu_kb(user_id),
        parse_mode=ParseMode.HTML,
    )


# =========================
# RUN
# =========================
async def main():
    logger.info("Бот запускается...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())


