import asyncio
import logging
import uuid
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo, LabeledPrice
from aiogram.enums import ParseMode

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = "8137546517:AAGno-CJPZ9C8-bbC7KccoGhPHaGiQZCMdw"
PROVIDER_TOKEN = "381764678:TEST:80597"  # Тестовый токен ЮKassa

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Хранилище заказов (в продакшене используйте БД)
user_orders = {}


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    welcome_text = """🚀 Приветствую!

Это лучший бот в РФ для накрутки
Готовы к росту? 🎯"""

    # Включаем отладочную информацию о мини-приложении
    logger.info("Отправка сообщения с кнопками для Накрутки и других сервисов.")

    keyboard = [
        [
            InlineKeyboardButton(
                text="📈 Накрутка",
                web_app=WebAppInfo(url="https://www.boostt.ru/")
            ),
            InlineKeyboardButton(text="🔐 Аккаунты", callback_data="accounts")
        ],
        [InlineKeyboardButton(text="🤖 Рассылка", callback_data="mailing_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(inline_keyboard=keyboard)

    try:
        # Если фотография не загружается, выводим только текстовое сообщение
        await message.answer_photo(
            types.FSInputFile('leeee.png'),
            caption=welcome_text,
            reply_markup=reply_markup
        )
    except Exception as e:
        logger.error(f"Ошибка отправки фото: {e}")
        await message.answer(welcome_text, reply_markup=reply_markup)


@dp.callback_query(lambda c: c.data == "accounts")
async def accounts_callback(callback: types.CallbackQuery):
    await callback.message.answer("🔐 Аккаунты в разработке")


@dp.callback_query(lambda c: c.data == "mailing_menu")
async def mailing_menu_callback(callback: types.CallbackQuery):
    text = """🤖 <b>Услуги рассылки:</b>

1. <b>Бот для рассылки</b>
   💰 Стоимость: 300 рублей
   ⚡ Автоматическая рассылка ваших сообщений

2. <b>Запуск рассылки вашего сообщения</b>
   💰 Стоимость: 100 рублей
   ⏰ Длительность: 1 день
   👥 Охват: до 1000 пользователей

💳 <b>Автоматическая оплата через ЮKassa:</b>"""

    keyboard = [
        [InlineKeyboardButton(text="🤖 Купить бота (300₽)", callback_data="buy_bot")],
        [InlineKeyboardButton(text="📢 Купить рассылку (100₽)", callback_data="buy_mailing")],
        [InlineKeyboardButton(text="📞 Поддержка @walter_belyi", url="https://t.me/walter_belyi")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_main")]
    ]
    reply_markup = InlineKeyboardMarkup(inline_keyboard=keyboard)

    await callback.message.answer(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)


@dp.callback_query(lambda c: c.data == "buy_bot")
async def buy_bot_callback(callback: types.CallbackQuery):
    # Создаем инвойс для бота
    prices = [LabeledPrice(label="🤖 Бот для рассылки", amount=30000)]  # 300 рублей в копейках

    # Генерируем уникальный ID заказа
    order_id = str(uuid.uuid4())
    user_orders[order_id] = {
        'user_id': callback.from_user.id,
        'service': 'bot',
        'amount': 30000
    }

    await bot.send_invoice(
        chat_id=callback.from_user.id,
        title="🤖 Бот для рассылки",
        description="Автоматическая рассылка ваших сообщений\nПосле оплаты свяжитесь с @walter_belyi для настройки",
        payload=order_id,
        provider_token=PROVIDER_TOKEN,
        currency="RUB",
        prices=prices,
        start_parameter="bot_subscription",
        need_name=False,
        need_email=False,
        need_phone_number=False,
        need_shipping_address=False
    )


@dp.callback_query(lambda c: c.data == "buy_mailing")
async def buy_mailing_callback(callback: types.CallbackQuery):
    # Создаем инвойс для рассылки
    prices = [LabeledPrice(label="📢 Рассылка на 1 день", amount=10000)]  # 100 рублей в копейках

    # Генерируем уникальный ID заказа
    order_id = str(uuid.uuid4())
    user_orders[order_id] = {
        'user_id': callback.from_user.id,
        'service': 'mailing',
        'amount': 10000
    }

    await bot.send_invoice(
        chat_id=callback.from_user.id,
        title="📢 Рассылка сообщения",
        description="Рассылка вашего сообщения на 1000 пользователей за 1 день\nПосле оплаты свяжитесь с @walter_belyi",
        payload=order_id,
        provider_token=PROVIDER_TOKEN,
        currency="RUB",
        prices=prices,
        start_parameter="mailing_service",
        need_name=False,
        need_email=False,
        need_phone_number=False,
        need_shipping_address=False
    )


@dp.pre_checkout_query()
async def pre_checkout_handler(pre_checkout_query: types.PreCheckoutQuery):
    # Проверяем существование заказа
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

    if order:
        service_name = "🤖 Бот для рассылки" if order['service'] == 'bot' else "📢 Рассылка сообщения"

        success_text = f"""✅ <b>Оплата прошла успешно!</b>

💼 Услуга: {service_name}
💰 Сумма: {payment.total_amount // 100} ₽
📦 Номер заказа: {order_id[:8]}

⚡ <b>Что дальше?</b>

Для активации услуги напишите нашему менеджеру:
📞 @walter_belyi

Укажите в сообщении:
• Номер заказа: {order_id[:8]}
• Ваш Telegram ID: {message.from_user.id}

Мы свяжемся с вами в течение 15 минут! 🚀"""

        await message.answer(success_text, parse_mode=ParseMode.HTML)

        # Удаляем заказ из временного хранилища
        del user_orders[order_id]
    else:
        await message.answer("❌ Ошибка обработки заказа. Свяжитесь с поддержкой: @walter_belyi")


@dp.callback_query(lambda c: c.data == "back_main")
async def back_main_callback(callback: types.CallbackQuery):
    welcome_text = """🚀 Приветствую!

Это лучший бот в РФ для накрутки
Готовы к росту? 🎯"""

    keyboard = [
        [
            InlineKeyboardButton(
                text="📈 Накрутка",
                web_app=WebAppInfo(url="https://amvera-daniel54-run-telergramapp.amvera.io")
            ),
            InlineKeyboardButton(text="🔐 Аккаунты", callback_data="accounts")
        ],
        [InlineKeyboardButton(text="🤖 Рассылка", callback_data="mailing_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(inline_keyboard=keyboard)

    await callback.message.answer(welcome_text, reply_markup=reply_markup)


async def main():
    logger.info("Бот запускается...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())


