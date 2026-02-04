import asyncio
import base64
import json
import logging
import os
import sqlite3
import threading
import uuid
import time
import hashlib
import hmac
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any
from urllib.parse import urlencode, parse_qsl

from dotenv import load_dotenv

load_dotenv()

import aiohttp
from aiohttp import web
import ssl
import certifi

from aiogram import Bot, Dispatcher, F, types
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    WebAppInfo,
    LabeledPrice,
    ReplyKeyboardMarkup,
    KeyboardButton,
)

# =========================
# CONFIG (ENV ONLY)
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN")
PROVIDER_TOKEN = os.getenv("PROVIDER_TOKEN")  # Telegram Payments (YooKassa). Configure in @BotFather -> Payments.

CRYPTO_PAY_TOKEN = os.getenv("CRYPTO_PAY_TOKEN")  # Crypto Pay API token from @CryptoBot -> Crypto Pay -> Create App
CRYPTO_PAY_API_BASE = os.getenv("CRYPTO_PAY_API_BASE", "https://pay.crypt.bot/api")

WEBAPP_URL_BASE = os.getenv("WEBAPP_URL_BASE", "https://www.boostt.ru/")
WEBAPP_BALANCE_PARAM = os.getenv("WEBAPP_BALANCE_PARAM", "tgBalance")
WEBAPP_SUCCESS_PARAM = os.getenv("WEBAPP_SUCCESS_PARAM", "tgSuccess")


# =========================
# API (WebApp backend)
# =========================
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8080"))
API_BASE_PATH = os.getenv("API_BASE_PATH", "/api").rstrip("/")


WELCOME_PHOTO = os.getenv("WELCOME_PHOTO", "leeee.png")

# Manager notifications
MANAGER_USERNAME = os.getenv("MANAGER_USERNAME", "DM_belyi").lstrip("@").strip() or "DM_belyi"
MANAGER_CHAT_ID = os.getenv("MANAGER_CHAT_ID")  # if set, preferred over username
try:
    MANAGER_CHAT_ID_INT = int(MANAGER_CHAT_ID) if MANAGER_CHAT_ID else None
except Exception:
    MANAGER_CHAT_ID_INT = None
BOT_USERNAME_ENV = (os.getenv("BOT_USERNAME") or "").strip().lstrip("@")

REF_BONUS_RATE = Decimal("0.40")
_BOT_USERNAME_CACHE: Optional[str] = None

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set (set env BOT_TOKEN=...)")

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
# DB (MySQL or SQLite)
# =========================
# Prefer MySQL if DB_BACKEND=mysql or MYSQL_HOST is set.
DB_BACKEND = (os.getenv("DB_BACKEND") or "").strip().lower()
MYSQL_HOST = (os.getenv("MYSQL_HOST") or "").strip()
_DB_KIND = "mysql" if (DB_BACKEND == "mysql" or MYSQL_HOST) else "sqlite"

_db_lock = threading.RLock()  # важен RLock: есть вложенные вызовы

if _DB_KIND == "mysql":
    try:
        import pymysql  # type: ignore
    except Exception as e:
        raise RuntimeError("MySQL backend selected but PyMySQL is not installed. Install: pip install PyMySQL") from e

    MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
    MYSQL_DB = (os.getenv("MYSQL_DB") or os.getenv("MYSQL_DATABASE") or "").strip()
    MYSQL_USER = (os.getenv("MYSQL_USER") or "").strip()
    MYSQL_PASSWORD = (os.getenv("MYSQL_PASSWORD") or "").strip()
    MYSQL_SSL_CA = (os.getenv("MYSQL_SSL_CA") or "").strip()
    MYSQL_CONNECT_TIMEOUT = int(os.getenv("MYSQL_CONNECT_TIMEOUT", "10"))
    MYSQL_READ_TIMEOUT = int(os.getenv("MYSQL_READ_TIMEOUT", "10"))
    MYSQL_WRITE_TIMEOUT = int(os.getenv("MYSQL_WRITE_TIMEOUT", "10"))

    if not MYSQL_HOST or not MYSQL_DB or not MYSQL_USER or not MYSQL_PASSWORD:
        raise RuntimeError(
            "MySQL backend requires MYSQL_HOST, MYSQL_DB, MYSQL_USER, MYSQL_PASSWORD in environment"
        )

    # IMPORTANT: VERIFY_IDENTITY usually requires connecting via hostname that matches the certificate (not raw IP).
    ssl_params = None
    if MYSQL_SSL_CA:
        ca_path = Path(MYSQL_SSL_CA).expanduser()
        if ca_path.exists():
            ssl_params = {"ca": str(ca_path), "check_hostname": True}
        else:
            raise RuntimeError(f"MYSQL_SSL_CA file not found: {ca_path}")

    _conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        charset="utf8mb4",
        autocommit=False,
        ssl=ssl_params,
        cursorclass=pymysql.cursors.Cursor,
        connect_timeout=MYSQL_CONNECT_TIMEOUT,
        read_timeout=MYSQL_READ_TIMEOUT,
        write_timeout=MYSQL_WRITE_TIMEOUT,
    )

    def _db_ping() -> None:
        global _conn
        try:
            _conn.ping(reconnect=True)
        except Exception:
            _conn = pymysql.connect(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                database=MYSQL_DB,
                charset="utf8mb4",
                autocommit=False,
                ssl=ssl_params,
                cursorclass=pymysql.cursors.Cursor,
                connect_timeout=MYSQL_CONNECT_TIMEOUT,
                read_timeout=MYSQL_READ_TIMEOUT,
                write_timeout=MYSQL_WRITE_TIMEOUT,
            )

    def _db_fetchone(q: str, params: tuple) -> Optional[tuple]:
        with _db_lock:
            _db_ping()
            with _conn.cursor() as cur:
                cur.execute(q, params)
                return cur.fetchone()

    def _db_fetchall(q: str, params: tuple) -> List[tuple]:
        with _db_lock:
            _db_ping()
            with _conn.cursor() as cur:
                cur.execute(q, params)
                return list(cur.fetchall())

    def _db_exec(q: str, params: tuple) -> int:
        with _db_lock:
            _db_ping()
            with _conn.cursor() as cur:
                cur.execute(q, params)
                affected = cur.rowcount
            return int(affected or 0)

    def _db_commit() -> None:
        with _db_lock:
            _conn.commit()

    def _db_rollback() -> None:
        with _db_lock:
            _conn.rollback()

    # Schema (InnoDB)
    with _db_lock:
        _db_ping()
        with _conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS balances (
                  user_id BIGINT PRIMARY KEY,
                  balance_kopecks BIGINT NOT NULL DEFAULT 0,
                  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS tg_processed_payments (
                  telegram_payment_charge_id VARCHAR(255) PRIMARY KEY,
                  provider_payment_charge_id VARCHAR(255),
                  user_id BIGINT NOT NULL,
                  amount_kopecks BIGINT NOT NULL,
                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS crypto_invoices (
                  invoice_id BIGINT PRIMARY KEY,
                  user_id BIGINT NOT NULL,
                  amount_kopecks BIGINT NOT NULL,
                  amount_rub VARCHAR(64) NOT NULL,
                  pay_url TEXT NOT NULL,
                  status VARCHAR(16) NOT NULL DEFAULT 'active',
                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  KEY idx_crypto_status_created (status, created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS pending_orders (
                  user_id BIGINT PRIMARY KEY,
                  order_id VARCHAR(64) NOT NULL,
                  amount_kopecks BIGINT NOT NULL,
                  order_json LONGTEXT NOT NULL,
                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS processed_orders (
                  order_id VARCHAR(64) PRIMARY KEY,
                  user_id BIGINT NOT NULL,
                  amount_kopecks BIGINT NOT NULL,
                  order_json LONGTEXT NOT NULL,
                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS orders (
                  order_id VARCHAR(64) PRIMARY KEY,
                  user_id BIGINT NOT NULL,
                  amount_kopecks BIGINT NOT NULL,
                  order_json LONGTEXT NOT NULL,
                  category_name VARCHAR(255),
                  status VARCHAR(16) NOT NULL DEFAULT 'new',
                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  KEY idx_orders_user_created (user_id, created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS settings (
                  `key` VARCHAR(191) PRIMARY KEY,
                  value TEXT NOT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS referrals (
                  user_id BIGINT PRIMARY KEY,
                  referrer_id BIGINT NOT NULL,
                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  KEY idx_referrals_referrer (referrer_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS referral_earnings (
                  order_id VARCHAR(64) PRIMARY KEY,
                  referrer_id BIGINT NOT NULL,
                  referred_id BIGINT NOT NULL,
                  amount_kopecks BIGINT NOT NULL,
                  reward_kopecks BIGINT NOT NULL,
                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  KEY idx_ref_earn_referrer (referrer_id),
                  KEY idx_ref_earn_referred (referred_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS referral_balances (
                  user_id BIGINT PRIMARY KEY,
                  balance_kopecks BIGINT NOT NULL DEFAULT 0,
                  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """
            )

        _conn.commit()

else:
    # =========================
    # SQLite fallback (local)
    # =========================
    DB_PATH = os.getenv("DB_PATH", str(Path(__file__).resolve().parent / "data" / "bot_data.db"))
    db_file = Path(DB_PATH)
    db_file.parent.mkdir(parents=True, exist_ok=True)

    _conn = sqlite3.connect(str(db_file), check_same_thread=False)
    _conn.execute("PRAGMA journal_mode=WAL;")
    _conn.execute("PRAGMA synchronous=NORMAL;")

    with _db_lock:
        _conn.execute(
            """
            CREATE TABLE IF NOT EXISTS balances (
              user_id INTEGER PRIMARY KEY,
              balance_kopecks INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        _conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tg_processed_payments (
              telegram_payment_charge_id TEXT PRIMARY KEY,
              provider_payment_charge_id TEXT,
              user_id INTEGER NOT NULL,
              amount_kopecks INTEGER NOT NULL,
              created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )
        _conn.execute(
            """
            CREATE TABLE IF NOT EXISTS crypto_invoices (
              invoice_id INTEGER PRIMARY KEY,
              user_id INTEGER NOT NULL,
              amount_kopecks INTEGER NOT NULL,
              amount_rub TEXT NOT NULL,
              pay_url TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT 'active',
              created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )

        # Pending purchase that should be auto-finalized after a top-up
        _conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pending_orders (
              user_id INTEGER PRIMARY KEY,
              order_id TEXT NOT NULL,
              amount_kopecks INTEGER NOT NULL,
              order_json TEXT NOT NULL,
              created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )
        # Idempotency / audit for purchases
        _conn.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_orders (
              order_id TEXT PRIMARY KEY,
              user_id INTEGER NOT NULL,
              amount_kopecks INTEGER NOT NULL,
              order_json TEXT NOT NULL,
              created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )

        _conn.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
              order_id TEXT PRIMARY KEY,
              user_id INTEGER NOT NULL,
              amount_kopecks INTEGER NOT NULL,
              order_json TEXT NOT NULL,
              category_name TEXT,
              status TEXT NOT NULL DEFAULT 'new',
              created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )

        _conn.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            )
            """
        )
        _conn.execute(
            """
            CREATE TABLE IF NOT EXISTS referrals (
              user_id INTEGER PRIMARY KEY,
              referrer_id INTEGER NOT NULL,
              created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )
        _conn.execute(
            """
            CREATE TABLE IF NOT EXISTS referral_earnings (
              order_id TEXT PRIMARY KEY,
              referrer_id INTEGER NOT NULL,
              referred_id INTEGER NOT NULL,
              amount_kopecks INTEGER NOT NULL,
              reward_kopecks INTEGER NOT NULL,
              created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )
        _conn.execute(
            """
            CREATE TABLE IF NOT EXISTS referral_balances (
              user_id INTEGER PRIMARY KEY,
              balance_kopecks INTEGER NOT NULL DEFAULT 0,
              updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )

        _conn.commit()


    def _ensure_orders_schema() -> None:
        """Best-effort DB migration for orders table."""
        with _db_lock:
            cols = [r[1] for r in _conn.execute("PRAGMA table_info(orders)").fetchall()]
            if "category_name" not in cols:
                try:
                    _conn.execute("ALTER TABLE orders ADD COLUMN category_name TEXT")
                except Exception:
                    pass
            if "status" not in cols:
                try:
                    _conn.execute("ALTER TABLE orders ADD COLUMN status TEXT NOT NULL DEFAULT 'new'")
                except Exception:
                    pass
            _conn.commit()


    _ensure_orders_schema()


# =========================
# SETTINGS (runtime config)
# =========================
def _get_setting(key: str) -> Optional[str]:
    key = str(key or '').strip()
    if not key:
        return None
    if _DB_KIND == "mysql":
        row = _db_fetchone("SELECT value FROM settings WHERE `key`=%s", (key,))
    else:
        with _db_lock:
            row = _conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    if not row:
        return None
    v = str(row[0])
    return v if v != '' else None


def _set_setting(key: str, value: str) -> None:
    key = str(key or '').strip()
    if not key:
        return
    v = str(value or '').strip()
    if _DB_KIND == "mysql":
        _db_exec(
            "INSERT INTO settings (`key`, value) VALUES (%s, %s) ON DUPLICATE KEY UPDATE value=VALUES(value)",
            (key, v),
        )
        _db_commit()
    else:
        with _db_lock:
            _conn.execute(
                "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, v),
            )
            _conn.commit()


def _get_balance_kopecks(user_id: int) -> int:
    if _DB_KIND == "mysql":
        row = _db_fetchone("SELECT balance_kopecks FROM balances WHERE user_id=%s", (int(user_id),))
        return int(row[0]) if row else 0
    with _db_lock:
        row = _conn.execute(
            "SELECT balance_kopecks FROM balances WHERE user_id = ?",
            (int(user_id),),
        ).fetchone()
    return int(row[0]) if row else 0


def _set_balance_kopecks(user_id: int, value: int) -> None:
    value = max(0, int(value))
    if _DB_KIND == "mysql":
        _db_exec(
            "INSERT INTO balances (user_id, balance_kopecks) VALUES (%s, %s) "
            "ON DUPLICATE KEY UPDATE balance_kopecks=VALUES(balance_kopecks)",
            (int(user_id), value),
        )
        _db_commit()
        return
    with _db_lock:
        _conn.execute(
            "INSERT INTO balances (user_id, balance_kopecks) VALUES (?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET balance_kopecks=excluded.balance_kopecks",
            (int(user_id), value),
        )
        _conn.commit()


def _calc_referral_reward(amount_kopecks: int) -> int:
    amount_kopecks = int(amount_kopecks)
    if amount_kopecks <= 0:
        return 0
    reward = (Decimal(amount_kopecks) * REF_BONUS_RATE).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return int(reward)


def _get_referrer(user_id: int) -> Optional[int]:
    if _DB_KIND == "mysql":
        row = _db_fetchone("SELECT referrer_id FROM referrals WHERE user_id=%s", (int(user_id),))
        return int(row[0]) if row else None
    with _db_lock:
        row = _conn.execute("SELECT referrer_id FROM referrals WHERE user_id=?", (int(user_id),)).fetchone()
    return int(row[0]) if row else None


def _set_referrer(user_id: int, referrer_id: int) -> bool:
    user_id = int(user_id)
    referrer_id = int(referrer_id)
    if user_id <= 0 or referrer_id <= 0 or user_id == referrer_id:
        return False
    if _DB_KIND == "mysql":
        _db_exec(
            "INSERT IGNORE INTO referrals (user_id, referrer_id) VALUES (%s, %s)",
            (user_id, referrer_id),
        )
        _db_commit()
        row = _db_fetchone("SELECT referrer_id FROM referrals WHERE user_id=%s", (user_id,))
        return bool(row and int(row[0]) == referrer_id)
    with _db_lock:
        cur = _conn.execute(
            "INSERT OR IGNORE INTO referrals (user_id, referrer_id) VALUES (?, ?)",
            (user_id, referrer_id),
        )
        _conn.commit()
        return cur.rowcount > 0


def _record_referral_reward(
    order_id: str,
    referrer_id: int,
    referred_id: int,
    amount_kopecks: int,
    reward_kopecks: int,
) -> bool:
    order_id = str(order_id or "").strip()
    if not order_id:
        return False
    if _DB_KIND == "mysql":
        _db_exec(
            "INSERT IGNORE INTO referral_earnings "
            "(order_id, referrer_id, referred_id, amount_kopecks, reward_kopecks) "
            "VALUES (%s, %s, %s, %s, %s)",
            (order_id, int(referrer_id), int(referred_id), int(amount_kopecks), int(reward_kopecks)),
        )
        _db_commit()
        row = _db_fetchone("SELECT 1 FROM referral_earnings WHERE order_id=%s", (order_id,))
        return bool(row)
    with _db_lock:
        cur = _conn.execute(
            "INSERT OR IGNORE INTO referral_earnings "
            "(order_id, referrer_id, referred_id, amount_kopecks, reward_kopecks) "
            "VALUES (?, ?, ?, ?, ?)",
            (order_id, int(referrer_id), int(referred_id), int(amount_kopecks), int(reward_kopecks)),
        )
        _conn.commit()
        return cur.rowcount > 0


def _add_balance_kopecks(user_id: int, delta: int) -> int:
    if _DB_KIND == "mysql":
        # atomic-ish update with lock; sufficient for single-process bot
        with _db_lock:
            _db_ping()
            try:
                with _conn.cursor() as cur:
                    cur.execute("SELECT balance_kopecks FROM balances WHERE user_id=%s FOR UPDATE", (int(user_id),))
                    row = cur.fetchone()
                    cur_bal = int(row[0]) if row else 0
                    new_val = cur_bal + int(delta)
                    if new_val < 0:
                        new_val = 0
                    cur.execute(
                        "INSERT INTO balances (user_id, balance_kopecks) VALUES (%s, %s) "
                        "ON DUPLICATE KEY UPDATE balance_kopecks=VALUES(balance_kopecks)",
                        (int(user_id), int(new_val)),
                    )
                _conn.commit()
                return int(new_val)
            except Exception:
                _conn.rollback()
                raise

    with _db_lock:
        cur = _get_balance_kopecks(user_id)
        new_val = cur + int(delta)
        if new_val < 0:
            new_val = 0
        _set_balance_kopecks(user_id, new_val)
    return new_val


def _try_debit_balance_kopecks(user_id: int, amount: int) -> Tuple[bool, int, int]:
    """
    Возвращает: ok, balance_before, balance_after

    ВАЖНО: amount должен быть строго > 0 (иначе это путь к накрутке).
    Для MySQL списание выполняется в транзакции с SELECT ... FOR UPDATE.
    """
    amount = int(amount)
    before = _get_balance_kopecks(user_id)

    if amount <= 0:
        return False, before, before

    if _DB_KIND == "mysql":
        with _db_lock:
            _db_ping()
            try:
                with _conn.cursor() as cur:
                    cur.execute("SELECT balance_kopecks FROM balances WHERE user_id=%s FOR UPDATE", (int(user_id),))
                    row = cur.fetchone()
                    cur_bal = int(row[0]) if row else 0
                    if cur_bal < amount:
                        _conn.rollback()
                        return False, cur_bal, cur_bal
                    new_val = cur_bal - amount
                    cur.execute(
                        "INSERT INTO balances (user_id, balance_kopecks) VALUES (%s, %s) "
                        "ON DUPLICATE KEY UPDATE balance_kopecks=VALUES(balance_kopecks)",
                        (int(user_id), int(new_val)),
                    )
                _conn.commit()
                return True, cur_bal, int(new_val)
            except Exception:
                _conn.rollback()
                raise

    # SQLite
    with _db_lock:
        cur_bal = _get_balance_kopecks(user_id)
        if cur_bal < amount:
            return False, cur_bal, cur_bal
        new_val = cur_bal - amount
        _set_balance_kopecks(user_id, new_val)
        return True, cur_bal, int(new_val)


def _format_rub_from_kopecks(v: int) -> str:
    v = int(v)
    return f"{v // 100} ₽" if v % 100 == 0 else f"{v / 100:.2f} ₽"


def _b64url_encode_json(obj: Dict[str, Any]) -> str:
    raw = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _webapp_url_for_user(user_id: int, extra_params: Optional[Dict[str, str]] = None) -> str:
    """
    Генерирует URL WebApp. Баланс передаем только для UI (не для логики).
    """
    bal_k = _get_balance_kopecks(user_id)
    bal_rub = bal_k / 100.0

    params: Dict[str, str] = {WEBAPP_BALANCE_PARAM: f"{bal_rub:.2f}"}
    if extra_params:
        params.update({k: str(v) for k, v in extra_params.items() if v is not None})

    joiner = "&" if "?" in WEBAPP_URL_BASE else "?"
    return f"{WEBAPP_URL_BASE}{joiner}{urlencode(params)}"


# =========================
# PAYMENTS / INVOICES / ORDERS
# =========================
def _is_tg_payment_processed(telegram_charge_id: str) -> bool:
    telegram_charge_id = str(telegram_charge_id or "").strip()
    if not telegram_charge_id:
        return False
    if _DB_KIND == "mysql":
        row = _db_fetchone(
            "SELECT 1 FROM tg_processed_payments WHERE telegram_payment_charge_id=%s",
            (telegram_charge_id,),
        )
        return bool(row)
    with _db_lock:
        row = _conn.execute(
            "SELECT 1 FROM tg_processed_payments WHERE telegram_payment_charge_id=?",
            (telegram_charge_id,),
        ).fetchone()
    return bool(row)


def _mark_tg_payment_processed(telegram_charge_id: str, provider_charge_id: str, user_id: int, amount_kopecks: int) -> None:
    telegram_charge_id = str(telegram_charge_id or "").strip()
    if not telegram_charge_id:
        return
    if _DB_KIND == "mysql":
        _db_exec(
            "INSERT IGNORE INTO tg_processed_payments (telegram_payment_charge_id, provider_payment_charge_id, user_id, amount_kopecks) "
            "VALUES (%s, %s, %s, %s)",
            (telegram_charge_id, str(provider_charge_id or ""), int(user_id), int(amount_kopecks)),
        )
        _db_commit()
        return
    with _db_lock:
        _conn.execute(
            "INSERT INTO tg_processed_payments (telegram_payment_charge_id, provider_payment_charge_id, user_id, amount_kopecks) "
            "VALUES (?, ?, ?, ?)",
            (telegram_charge_id, provider_charge_id, user_id, int(amount_kopecks)),
        )
        _conn.commit()


# =========================
# PENDING / PROCESSED ORDERS
# =========================
def _is_order_processed(order_id: str) -> bool:
    if not order_id:
        return False
    if _DB_KIND == "mysql":
        row = _db_fetchone("SELECT 1 FROM processed_orders WHERE order_id=%s", (str(order_id),))
        return bool(row)
    with _db_lock:
        row = _conn.execute("SELECT 1 FROM processed_orders WHERE order_id=?", (str(order_id),)).fetchone()
    return bool(row)


def _mark_order_processed(order_id: str, user_id: int, amount_kopecks: int, order_json: str) -> None:
    if not order_id:
        return
    if _DB_KIND == "mysql":
        _db_exec(
            "INSERT IGNORE INTO processed_orders (order_id, user_id, amount_kopecks, order_json) VALUES (%s, %s, %s, %s)",
            (str(order_id), int(user_id), int(amount_kopecks), str(order_json)),
        )
        _db_commit()
        return
    with _db_lock:
        _conn.execute(
            "INSERT OR IGNORE INTO processed_orders (order_id, user_id, amount_kopecks, order_json) VALUES (?, ?, ?, ?)",
            (str(order_id), int(user_id), int(amount_kopecks), str(order_json)),
        )
        _conn.commit()


def _set_pending_order(user_id: int, order_id: str, amount_kopecks: int, order_json: str) -> None:
    if _DB_KIND == "mysql":
        _db_exec(
            "INSERT INTO pending_orders (user_id, order_id, amount_kopecks, order_json) VALUES (%s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE order_id=VALUES(order_id), amount_kopecks=VALUES(amount_kopecks), order_json=VALUES(order_json)",
            (int(user_id), str(order_id), int(amount_kopecks), str(order_json)),
        )
        _db_commit()
        return
    with _db_lock:
        _conn.execute(
            "INSERT OR REPLACE INTO pending_orders (user_id, order_id, amount_kopecks, order_json) VALUES (?, ?, ?, ?)",
            (int(user_id), str(order_id), int(amount_kopecks), str(order_json)),
        )
        _conn.commit()


def _get_pending_order(user_id: int) -> Optional[Dict[str, Any]]:
    if _DB_KIND == "mysql":
        row = _db_fetchone(
            "SELECT user_id, order_id, amount_kopecks, order_json FROM pending_orders WHERE user_id=%s",
            (int(user_id),),
        )
    else:
        with _db_lock:
            row = _conn.execute(
                "SELECT user_id, order_id, amount_kopecks, order_json FROM pending_orders WHERE user_id=?",
                (int(user_id),),
            ).fetchone()
    if not row:
        return None
    try:
        payload = json.loads(str(row[3]))
    except Exception:
        payload = {}
    return {
        "user_id": int(row[0]),
        "order_id": str(row[1]),
        "amount_kopecks": int(row[2]),
        "order": payload,
        "order_json": str(row[3]),
    }


def _clear_pending_order(user_id: int) -> None:
    if _DB_KIND == "mysql":
        _db_exec("DELETE FROM pending_orders WHERE user_id=%s", (int(user_id),))
        _db_commit()
        return
    with _db_lock:
        _conn.execute("DELETE FROM pending_orders WHERE user_id=?", (int(user_id),))
        _conn.commit()


# =========================
# ORDERS (WebApp history)
# =========================
def _create_order(user_id: int, order: Dict[str, Any], amount_kopecks: int) -> str:
    """Creates/updates an order in DB.

    - Preserves existing status (e.g. done) if the order_id already exists.
    - Stores category_name separately for fast listing.
    """
    order_id = str(order.get("order_id") or order.get("orderId") or order.get("id") or uuid.uuid4().hex)
    category_name = str(
        order.get("category_name")
        or order.get("categoryName")
        or order.get("category")
        or "—"
    )
    try:
        order_json = json.dumps(order, ensure_ascii=False)
    except Exception:
        order_json = "{}"

    if _DB_KIND == "mysql":
        # Preserve current status by not updating it in ON DUPLICATE KEY UPDATE
        _db_exec(
            "INSERT INTO orders (order_id, user_id, amount_kopecks, order_json, category_name, status) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE "
            "user_id=VALUES(user_id), amount_kopecks=VALUES(amount_kopecks), order_json=VALUES(order_json), category_name=VALUES(category_name)",
            (order_id, int(user_id), int(amount_kopecks), order_json, category_name, "new"),
        )
        _db_commit()
        return order_id

    with _db_lock:
        # Preserve current status (if any)
        try:
            row = _conn.execute("SELECT status FROM orders WHERE order_id=?", (order_id,)).fetchone()
            cur_status = str(row[0]) if row and row[0] else "new"
        except Exception:
            cur_status = "new"

        try:
            _conn.execute(
                "INSERT INTO orders (order_id, user_id, amount_kopecks, order_json, category_name, status) "
                "VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(order_id) DO UPDATE SET "
                "user_id=excluded.user_id, "
                "amount_kopecks=excluded.amount_kopecks, "
                "order_json=excluded.order_json, "
                "category_name=excluded.category_name",
                (order_id, int(user_id), int(amount_kopecks), order_json, category_name, cur_status),
            )
        except Exception:
            # Fallback for very old SQLite builds
            _conn.execute(
                "INSERT OR REPLACE INTO orders (order_id, user_id, amount_kopecks, order_json, category_name, status) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (order_id, int(user_id), int(amount_kopecks), order_json, category_name, cur_status),
            )
        _conn.commit()
    return order_id


def _set_order_status(order_id: str, status: str) -> None:
    status = str(status or "").strip() or "new"
    if _DB_KIND == "mysql":
        _db_exec("UPDATE orders SET status=%s WHERE order_id=%s", (status, str(order_id)))
        _db_commit()
        return
    with _db_lock:
        _conn.execute("UPDATE orders SET status=? WHERE order_id=?", (status, str(order_id)))
        _conn.commit()


def _list_orders(user_id: int, limit: int = 50) -> List[Dict[str, Any]]:
    if _DB_KIND == "mysql":
        rows = _db_fetchall(
            "SELECT order_id, amount_kopecks, order_json, created_at, category_name, status "
            "FROM orders WHERE user_id=%s ORDER BY created_at DESC LIMIT %s",
            (int(user_id), int(limit)),
        )
    else:
        with _db_lock:
            rows = _conn.execute(
                "SELECT order_id, amount_kopecks, order_json, created_at, category_name, status "
                "FROM orders WHERE user_id=? ORDER BY datetime(created_at) DESC LIMIT ?",
                (int(user_id), int(limit)),
            ).fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        try:
            payload = json.loads(str(r[2]))
        except Exception:
            payload = {}
        category_name = str(r[4] or payload.get("category_name") or payload.get("categoryName") or payload.get("category") or "—")
        status = str(r[5] or "new")
        out.append(
            {
                "order_id": str(r[0]),
                "amount_kopecks": int(r[1]),
                "created_at": str(r[3]),
                "category_name": category_name,
                "status": status,
                "order": payload,
            }
        )
    return out


def _list_all_orders(limit: int = 50) -> List[Dict[str, Any]]:
    if _DB_KIND == "mysql":
        rows = _db_fetchall(
            "SELECT order_id, user_id, amount_kopecks, order_json, created_at, category_name, status "
            "FROM orders ORDER BY created_at DESC LIMIT %s",
            (int(limit),),
        )
    else:
        with _db_lock:
            rows = _conn.execute(
                "SELECT order_id, user_id, amount_kopecks, order_json, created_at, category_name, status "
                "FROM orders ORDER BY datetime(created_at) DESC LIMIT ?",
                (int(limit),),
            ).fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        try:
            payload = json.loads(str(r[3]))
        except Exception:
            payload = {}
        category_name = str(r[5] or payload.get("category_name") or payload.get("categoryName") or payload.get("category") or "—")
        status = str(r[6] or "new")
        out.append(
            {
                "order_id": str(r[0]),
                "user_id": int(r[1]),
                "amount_kopecks": int(r[2]),
                "created_at": str(r[4]),
                "category_name": category_name,
                "status": status,
                "order": payload,
            }
        )
    return out


def _get_order(user_id: int, order_id: str) -> Optional[Dict[str, Any]]:
    if _DB_KIND == "mysql":
        row = _db_fetchone(
            "SELECT order_id, amount_kopecks, order_json, created_at, category_name, status "
            "FROM orders WHERE user_id=%s AND order_id=%s",
            (int(user_id), str(order_id)),
        )
    else:
        with _db_lock:
            row = _conn.execute(
                "SELECT order_id, amount_kopecks, order_json, created_at, category_name, status "
                "FROM orders WHERE user_id=? AND order_id=?",
                (int(user_id), str(order_id)),
            ).fetchone()
    if not row:
        return None
    try:
        payload = json.loads(str(row[2]))
    except Exception:
        payload = {}
    category_name = str(row[4] or payload.get("category_name") or payload.get("categoryName") or payload.get("category") or "—")
    status = str(row[5] or "new")
    return {
        "order_id": str(row[0]),
        "amount_kopecks": int(row[1]),
        "created_at": str(row[3]),
        "category_name": category_name,
        "status": status,
        "order": payload,
    }


def _get_order_by_id(order_id: str) -> Optional[Dict[str, Any]]:
    if not order_id:
        return None
    if _DB_KIND == "mysql":
        row = _db_fetchone(
            "SELECT order_id, user_id, amount_kopecks, order_json, created_at, category_name, status "
            "FROM orders WHERE order_id=%s",
            (str(order_id),),
        )
    else:
        with _db_lock:
            row = _conn.execute(
                "SELECT order_id, user_id, amount_kopecks, order_json, created_at, category_name, status "
                "FROM orders WHERE order_id=?",
                (str(order_id),),
            ).fetchone()
    if not row:
        return None
    try:
        payload = json.loads(str(row[3]))
    except Exception:
        payload = {}
    category_name = str(row[5] or payload.get("category_name") or payload.get("categoryName") or payload.get("category") or "—")
    status = str(row[6] or "new")
    return {
        "order_id": str(row[0]),
        "user_id": int(row[1] or 0),
        "amount_kopecks": int(row[2] or 0),
        "created_at": str(row[4]),
        "category_name": category_name,
        "status": status,
        "order": payload,
    }

# =========================
# STATE
# =========================
awaiting_custom_topup_card = set()  # user_id awaiting_custom_topup_card
awaiting_custom_topup_crypto = set()  # user_id


# =========================
# CRYPTO PAY (HTTP) — certifi fix
# =========================
_crypto_session: Optional[aiohttp.ClientSession] = None


def _get_ssl_context():
    # certifi (фикс SSL CERTIFICATE_VERIFY_FAILED на macOS/venv)
    return ssl.create_default_context(cafile=certifi.where())


async def _crypto_call(method: str, params: Dict[str, Any]) -> Any:
    """Crypto Pay API call."""
    if not CRYPTO_PAY_TOKEN:
        raise RuntimeError("CRYPTO_PAY_TOKEN is not set (set env CRYPTO_PAY_TOKEN=...)")

    global _crypto_session
    if _crypto_session is None or _crypto_session.closed:
        ssl_ctx = _get_ssl_context()
        _crypto_session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=20),
            connector=aiohttp.TCPConnector(ssl=ssl_ctx),
        )

    url = f"{CRYPTO_PAY_API_BASE.rstrip('/')}/{method}"
    headers = {"Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN}

    async with _crypto_session.post(url, json=params, headers=headers) as resp:
        data = await resp.json(content_type=None)

    if not isinstance(data, dict) or not data.get("ok"):
        err = data.get("error") if isinstance(data, dict) else f"Bad response: {data}"
        raise RuntimeError(f"Crypto Pay API error: {err}")

    return data.get("result")


def _crypto_invoice_url(inv: Dict[str, Any]) -> str:
    return (
        inv.get("bot_invoice_url")
        or inv.get("mini_app_invoice_url")
        or inv.get("web_app_invoice_url")
        or inv.get("pay_url")
        or ""
    )


async def _create_crypto_invoice(user_id: int, amount_rub: Decimal) -> Dict[str, Any]:
    amount_rub = amount_rub.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    if amount_rub <= 0:
        raise ValueError("Amount must be > 0")

    inv = await _crypto_call(
        "createInvoice",
        {
            "currency_type": "fiat",
            "fiat": "RUB",
            "amount": f"{amount_rub:.2f}",
            "accepted_assets": "USDT,TON,BTC,ETH,USDC",
            "description": "Пополнение баланса BoostShop",
            "payload": f"topup:{user_id}:{uuid.uuid4().hex}",
            "expires_in": 3600,
        },
    )
    return inv


def _store_crypto_invoice(invoice_id: int, user_id: int, amount_kopecks: int, amount_rub_str: str, pay_url: str) -> None:
    if _DB_KIND == "mysql":
        # Preserve existing status by not updating it on duplicate
        _db_exec(
            "INSERT INTO crypto_invoices (invoice_id, user_id, amount_kopecks, amount_rub, pay_url, status) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE "
            "user_id=VALUES(user_id), amount_kopecks=VALUES(amount_kopecks), amount_rub=VALUES(amount_rub), pay_url=VALUES(pay_url)",
            (int(invoice_id), int(user_id), int(amount_kopecks), str(amount_rub_str), str(pay_url), "active"),
        )
        _db_commit()
        return
    with _db_lock:
        _conn.execute(
            """
            INSERT OR REPLACE INTO crypto_invoices (invoice_id, user_id, amount_kopecks, amount_rub, pay_url, status)
            VALUES (?, ?, ?, ?, ?, COALESCE((SELECT status FROM crypto_invoices WHERE invoice_id=?), 'active'))
            """,
            (invoice_id, user_id, int(amount_kopecks), amount_rub_str, pay_url, invoice_id),
        )
        _conn.commit()

def _get_active_crypto_invoice_ids(limit: int = 200) -> List[int]:
    if _DB_KIND == "mysql":
        rows = _db_fetchall(
            "SELECT invoice_id FROM crypto_invoices WHERE status='active' ORDER BY created_at DESC LIMIT %s",
            (int(limit),),
        )
        return [int(r[0]) for r in rows]
    with _db_lock:
        rows = _conn.execute(
            "SELECT invoice_id FROM crypto_invoices WHERE status='active' ORDER BY created_at DESC LIMIT ?",
            (int(limit),),
        ).fetchall()
    return [int(r[0]) for r in rows]

def _mark_crypto_paid_if_first(invoice_id: int) -> bool:
    if _DB_KIND == "mysql":
        # Atomic: update only if not already paid
        with _db_lock:
            _db_ping()
            try:
                with _conn.cursor() as cur:
                    cur.execute(
                        "UPDATE crypto_invoices SET status='paid' WHERE invoice_id=%s AND status<>'paid'",
                        (int(invoice_id),),
                    )
                    changed = int(cur.rowcount or 0)
                _conn.commit()
                return changed > 0
            except Exception:
                _conn.rollback()
                raise

    with _db_lock:
        row = _conn.execute("SELECT status FROM crypto_invoices WHERE invoice_id=?", (int(invoice_id),)).fetchone()
        if not row:
            return False
        if str(row[0]) == "paid":
            return False
        _conn.execute("UPDATE crypto_invoices SET status='paid' WHERE invoice_id=?", (int(invoice_id),))
        _conn.commit()
    return True


def _get_ref_balance_kopecks(user_id: int) -> int:
    if _DB_KIND == "mysql":
        row = _db_fetchone("SELECT balance_kopecks FROM referral_balances WHERE user_id=%s", (int(user_id),))
    else:
        row = _conn.execute(
            "SELECT balance_kopecks FROM referral_balances WHERE user_id=?",
            (int(user_id),),
        ).fetchone()
    if not row:
        return 0
    try:
        return int(row[0] or 0)
    except Exception:
        return 0


def _add_ref_balance(user_id: int, amount_kopecks: int) -> None:
    if amount_kopecks <= 0:
        return
    if _DB_KIND == "mysql":
        _db_exec(
            "INSERT INTO referral_balances (user_id, balance_kopecks) VALUES (%s, %s) "
            "ON DUPLICATE KEY UPDATE balance_kopecks=balance_kopecks+VALUES(balance_kopecks)",
            (int(user_id), int(amount_kopecks)),
        )
    else:
        with _db_lock:
            _conn.execute(
                "INSERT INTO referral_balances (user_id, balance_kopecks) VALUES (?, ?) "
                "ON CONFLICT(user_id) DO UPDATE SET balance_kopecks=balance_kopecks+excluded.balance_kopecks, "
                "updated_at=datetime('now')",
                (int(user_id), int(amount_kopecks)),
            )
            _conn.commit()

def _get_crypto_invoice_meta(invoice_id: int) -> Optional[Dict[str, Any]]:
    if _DB_KIND == "mysql":
        row = _db_fetchone(
            "SELECT invoice_id, user_id, amount_kopecks, amount_rub, pay_url, status FROM crypto_invoices WHERE invoice_id=%s",
            (int(invoice_id),),
        )
    else:
        with _db_lock:
            row = _conn.execute(
                "SELECT invoice_id, user_id, amount_kopecks, amount_rub, pay_url, status FROM crypto_invoices WHERE invoice_id=?",
                (int(invoice_id),),
            ).fetchone()
    if not row:
        return None
    return {
        "invoice_id": int(row[0]),
        "user_id": int(row[1]),
        "amount_kopecks": int(row[2]),
        "amount_rub": str(row[3]),
        "pay_url": str(row[4]),
        "status": str(row[5]),
    }

def _resolve_manager_target() -> Optional[Any]:
    """
    Returns chat_id suitable for Bot.send_message().
    Priority:
      1) MANAGER_CHAT_ID env
      2) DB setting: settings.manager_chat_id (can be set via /manager_set)
      3) MANAGER_USERNAME env (as @username)
    """
    raw = (MANAGER_CHAT_ID or _get_setting('manager_chat_id') or '').strip()
    if raw:
        try:
            return int(raw)
        except Exception:
            return raw
    uname = (MANAGER_USERNAME or '').lstrip('@').strip()
    if uname:
        return f"@{uname}"
    return None


async def _notify_manager(text: str, reply_markup: Optional[InlineKeyboardMarkup] = None) -> None:
    target = _resolve_manager_target()
    if not target:
        logger.warning("Manager target is not configured (set MANAGER_CHAT_ID or use /manager_set).")
        return

    try:
        await bot.send_message(
            target,
            text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=reply_markup,
        )
    except Exception as e:
        logger.error(f"Failed to notify manager ({target}): {e}")


def _notify_manager_bg(text: str, reply_markup: Optional[InlineKeyboardMarkup] = None) -> None:
    """Fire-and-forget manager notification using asyncio."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    loop.create_task(_notify_manager(text, reply_markup=reply_markup))


def _user_link(user_id: int, display: str = "пользователь") -> str:
    return f"<a href=\"tg://user?id={int(user_id)}\">{display}</a>"


def _order_text_block(order: Dict[str, Any]) -> str:
    # Унифицируем поля
    order_id = order.get("order_id") or order.get("orderId") or order.get("id") or "—"
    title = order.get("title") or "Заказ"
    platform = order.get("platform") or "—"
    service = order.get("service") or "—"
    category_name = order.get("category_name") or order.get("categoryName") or order.get("category") or "—"
    qty = order.get("quantity") or order.get("qty") or "—"
    total = order.get("total_price") or order.get("total") or "0"
    link = order.get("link") or order.get("target") or order.get("url") or ""
    link_line = f"\nСсылка: <code>{link}</code>" if link else ""

    return (
        f"Заказ: <b>{title}</b>\n"
        f"ID: <code>{order_id}</code>\n"
        f"Платформа: <b>{platform}</b>\n"
        f"Услуга: <b>{service}</b>\n"
        f"Категория: <b>{category_name}</b>\n"
        f"Кол-во: <b>{qty}</b>\n"
        f"Сумма: <b>{total} ₽</b>"
        f"{link_line}"
    )


def _order_success_param(order: Dict[str, Any]) -> Dict[str, str]:
    # То, что webapp покажет на экране успеха
    payload = {
        "order_id": order.get("order_id") or order.get("orderId") or order.get("id"),
        "title": order.get("title"),
        "platform": order.get("platform"),
        "service": order.get("service"),
        "category_name": order.get("category_name") or order.get("categoryName") or order.get("category"),
        "quantity": order.get("quantity") or order.get("qty"),
        "total_price": order.get("total_price") or order.get("total"),
    }
    return {WEBAPP_SUCCESS_PARAM: _b64url_encode_json(payload)}


async def _finalize_pending_order_if_possible(user_id: int, source: str) -> bool:
    pending = _get_pending_order(user_id)
    if not pending:
        return False

    order = pending["order"] if isinstance(pending.get("order"), dict) else {}
    order_id = str(pending.get("order_id") or order.get("order_id") or order.get("orderId") or "")
    amount_need = int(pending.get("amount_kopecks") or 0)

    # Если вдруг уже обработан — очищаем pending
    if order_id and _is_order_processed(order_id):
        _clear_pending_order(user_id)
        return True

    ok, before, after = _try_debit_balance_kopecks(user_id, amount_need)
    if not ok:
        need_more = max(0, amount_need - before)
        need_rub = int((need_more + 99) // 100)
        txt = (
            "ℹ️ <b>Баланс пополнен</b>, но для покупки всё ещё не хватает средств.\n\n"
            f"Не хватает: <b>{need_rub} ₽</b>\n"
            f"Текущий баланс: <b>{_format_rub_from_kopecks(before)}</b>\n\n"
            "Пополните ещё раз — заказ будет оформлен автоматически."
        )
        try:
            await bot.send_message(user_id, txt, parse_mode=ParseMode.HTML, reply_markup=topup_amounts_kb(need_rub))
        except Exception:
            pass
        return False

    # Успешно списали -> фиксируем
    _clear_pending_order(user_id)
    try:
        order_json = json.dumps(order, ensure_ascii=False)
    except Exception:
        order_json = "{}"
    final_order_id = order_id or f"auto-{uuid.uuid4().hex}"
    _mark_order_processed(final_order_id, user_id, amount_need, order_json)

    # История заказов (WebApp «Профиль»)
    try:
        order["order_id"] = final_order_id
        _create_order(user_id, order, amount_need)
    except Exception as e:
        logger.warning(f"Failed to store order in history (auto-finalize): {e}")

    try:
        await _apply_referral_reward(user_id, final_order_id, amount_need)
    except Exception:
        pass

    # Пользователь
    text_user = (
        "✅ <b>Покупка оплачена!</b>\n\n"
        f"{_order_text_block(order)}\n\n"
        f"Списано: <b>{_format_rub_from_kopecks(amount_need)}</b>\n"
        f"Баланс: <b>{_format_rub_from_kopecks(after)}</b>\n\n"
        f"Источник: <b>{source}</b>\n"
        "Откройте приложение — увидите экран «успешная покупка»."
    )
    try:
        await bot.send_message(
            user_id,
            text_user,
            parse_mode=ParseMode.HTML,
            reply_markup=open_webapp_kb(user_id, success_order=order),
        )
    except Exception:
        pass

    # Менеджер
    text_mgr = (
        "🧾 <b>Новый оплаченный заказ</b>\n\n"
        f"Покупатель: {_user_link(user_id, 'профиль')} (ID <code>{user_id}</code>)\n"
        f"Оплата: <b>{source}</b>\n\n"
        f"{_order_text_block(order)}"
    )
    _notify_manager_bg(text_mgr, reply_markup=_mgr_confirm_kb(final_order_id))
    return True


async def _process_paid_crypto_invoice(invoice_id: int) -> None:
    meta = _get_crypto_invoice_meta(invoice_id)
    if not meta:
        return

    if not _mark_crypto_paid_if_first(invoice_id):
        return

    user_id = meta["user_id"]
    amount_kopecks = meta["amount_kopecks"]
    new_bal = _add_balance_kopecks(user_id, amount_kopecks)

    # Попытаемся автоматически завершить ожидающую покупку
    await _finalize_pending_order_if_possible(user_id, source="Крипто")

    text = (
        "✅ <b>Крипто-оплата получена!</b>\n\n"
        f"Сумма: <b>{_format_rub_from_kopecks(amount_kopecks)}</b>\n"
        f"Текущий баланс: <b>{_format_rub_from_kopecks(new_bal)}</b>\n\n"
        "Откройте приложение — баланс обновится."
    )
    try:
        await bot.send_message(user_id, text, reply_markup=open_webapp_kb(user_id), parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Failed to notify user about crypto payment: {e}")

    # Нотификация менеджеру (о факте оплаты)
    _notify_manager_bg(
        "🪙 <b>Пополнение криптой</b>\n\n"
        f"Пользователь: {_user_link(user_id, 'профиль')} (ID <code>{user_id}</code>)\n"
        f"Сумма: <b>{_format_rub_from_kopecks(amount_kopecks)}</b>"
    )


async def crypto_invoices_watcher() -> None:
    while True:
        await asyncio.sleep(20)
        try:
            ids = _get_active_crypto_invoice_ids(limit=200)
            if not ids:
                continue

            result = await _crypto_call(
                "getInvoices",
                {"invoice_ids": ",".join(str(i) for i in ids), "status": "paid", "count": 1000},
            )
            if not isinstance(result, list):
                continue

            for inv in result:
                try:
                    if str(inv.get("status")) == "paid":
                        await _process_paid_crypto_invoice(int(inv.get("invoice_id")))
                except Exception as e:
                    logger.error(f"Error processing paid invoice: {e}")

        except Exception as e:
            logger.error(f"Crypto watcher error: {e}")


# =========================
# UI BUILDERS
# =========================
async def _get_bot_username() -> Optional[str]:
    global _BOT_USERNAME_CACHE
    if _BOT_USERNAME_CACHE:
        return _BOT_USERNAME_CACHE
    if BOT_USERNAME_ENV:
        _BOT_USERNAME_CACHE = BOT_USERNAME_ENV
        return _BOT_USERNAME_CACHE
    try:
        me = await bot.get_me()
        _BOT_USERNAME_CACHE = (me.username or "").strip().lstrip("@") or None
    except Exception:
        _BOT_USERNAME_CACHE = None
    return _BOT_USERNAME_CACHE


async def _build_ref_link(user_id: int) -> Optional[str]:
    username = await _get_bot_username()
    if not username:
        return None
    return f"https://t.me/{username}?start=ref_{int(user_id)}"


async def _apply_referral_reward(user_id: int, order_id: str, amount_kopecks: int) -> None:
    referrer_id = _get_referrer(user_id)
    if not referrer_id or int(referrer_id) == int(user_id):
        return
    reward = _calc_referral_reward(amount_kopecks)
    if reward <= 0:
        return
    created = _record_referral_reward(order_id, referrer_id, user_id, amount_kopecks, reward)
    if not created:
        return
    _add_ref_balance(referrer_id, reward)

    ref_text = (
        "🎉 <b>Новый доход по партнёрке</b>\n\n"
        f"Клиент: {_user_link(user_id, 'профиль')} (ID <code>{user_id}</code>)\n"
        f"Сумма покупки: <b>{_format_rub_from_kopecks(amount_kopecks)}</b>\n"
        f"Ваш доход (40%): <b>{_format_rub_from_kopecks(reward)}</b>\n"
        f"Заказ: <code>{order_id}</code>"
    )
    try:
        await bot.send_message(int(referrer_id), ref_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except Exception:
        pass

    _notify_manager_bg(
        "🤝 <b>Партнёрская программа</b>\n\n"
        f"Реферер: {_user_link(referrer_id, 'профиль')} (ID <code>{referrer_id}</code>)\n"
        f"Клиент: {_user_link(user_id, 'профиль')} (ID <code>{user_id}</code>)\n"
        f"Сумма покупки: <b>{_format_rub_from_kopecks(amount_kopecks)}</b>\n"
        f"Начисление 40%: <b>{_format_rub_from_kopecks(reward)}</b>\n"
        f"Заказ: <code>{order_id}</code>"
    )


def main_reply_kb(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="🚀 Накрутка и баланс"),
                KeyboardButton(text="➕ Пополнить баланс 💳"),
            ],
            [
                KeyboardButton(text="🤝 Партнерская программа"),
                KeyboardButton(text="🆘 Поддержка"),
                KeyboardButton(text="📜 Правила"),
            ],
        ],
        resize_keyboard=True,
    )

def main_menu_kb(user_id: int) -> InlineKeyboardMarkup:
    webapp_url = _webapp_url_for_user(user_id)
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📈 Накрутка", web_app=WebAppInfo(url=webapp_url))],
            [InlineKeyboardButton(text="➕ Пополнить баланс 💳", callback_data="balance_topup")],
        ]
    )


def open_webapp_kb(user_id: int, success_order: Optional[Dict[str, Any]] = None) -> InlineKeyboardMarkup:
    extra = None
    if success_order:
        extra = _order_success_param(success_order)
    url = _webapp_url_for_user(user_id, extra_params=extra)
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🚀 Открыть приложение", web_app=WebAppInfo(url=url))]]
    )


def topup_amounts_kb(need_rub: int = 0) -> InlineKeyboardMarkup:
    need = int(need_rub or 0)

    rows = []
    if need > 0:
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"✅ Пополнить ровно {need} ₽",
                    callback_data=f"topup_recommend_{need}",
                )
            ]
        )

    rows += [
        [
            InlineKeyboardButton(text="100 ₽", callback_data="topup_amount_100"),
            InlineKeyboardButton(text="300 ₽", callback_data="topup_amount_300"),
            InlineKeyboardButton(text="500 ₽", callback_data="topup_amount_500"),
        ],
        [
            InlineKeyboardButton(text="1000 ₽", callback_data="topup_amount_1000"),
            InlineKeyboardButton(text="Другая сумма", callback_data="topup_amount_custom"),
        ],
        [InlineKeyboardButton(text="🪙 Оплатить криптой", callback_data=f"topup_crypto_menu_{need}")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_main")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def crypto_amounts_kb(need_rub: int = 0) -> InlineKeyboardMarkup:
    need = int(need_rub or 0)
    row0 = []
    if need > 0:
        row0 = [InlineKeyboardButton(text=f"Рекомендовано: {need} ₽", callback_data=f"crypto_amount_{need}")]

    rows = []
    if row0:
        rows.append(row0)

    rows += [
        [
            InlineKeyboardButton(text="100 ₽", callback_data="crypto_amount_100"),
            InlineKeyboardButton(text="300 ₽", callback_data="crypto_amount_300"),
            InlineKeyboardButton(text="500 ₽", callback_data="crypto_amount_500"),
        ],
        [
            InlineKeyboardButton(text="1000 ₽", callback_data="crypto_amount_1000"),
            InlineKeyboardButton(text="Другая сумма", callback_data="crypto_amount_custom"),
        ],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=f"back_topup_{need}")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def show_topup_amounts(chat_id: int, user_id: int, need_rub: int = 0) -> None:
    bal = _get_balance_kopecks(user_id)
    need = int(need_rub or 0)

    header = "💳 <b>Пополнить баланс</b>"
    if need > 0:
        header = "❌ <b>Недостаточно средств</b>\n\n💳 <b>Пополнить баланс</b>"

    need_line = f"\n\nНе хватает: <b>{need} ₽</b>" if need > 0 else ""
    text = (
        f"{header}\n\n"
        f"Текущий баланс: <b>{_format_rub_from_kopecks(bal)}</b>"
        f"{need_line}\n\n"
        "Выберите сумму пополнения:"
    )
    await bot.send_message(chat_id, text, reply_markup=topup_amounts_kb(need_rub=need), parse_mode=ParseMode.HTML)


async def send_welcome(chat_id: int, user_id: int, include_greeting: bool = True) -> None:
    bal = _format_rub_from_kopecks(_get_balance_kopecks(user_id))
    greeting = "🚀 <b>Приветствую!</b>\n\n" if include_greeting else ""
    welcome_text = (
        f"{greeting}"
        "Готовы к росту?\n\n"
        f"💳 Ваш баланс: <b>{bal}</b>\n\n"
        "Чтобы продолжить, нажмите кнопку ниже:\n"
        "• 🚀 Накрутка и баланс\n"
    )

    try:
        await bot.send_photo(
            chat_id=chat_id,
            photo=types.FSInputFile(WELCOME_PHOTO),
            caption=welcome_text,
            reply_markup=main_reply_kb(user_id),
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        logger.error(f"Welcome photo error: {e}")
        await bot.send_message(chat_id, welcome_text, reply_markup=main_reply_kb(user_id), parse_mode=ParseMode.HTML)


async def send_quick_menu(chat_id: int, user_id: int) -> None:
    bal = _format_rub_from_kopecks(_get_balance_kopecks(user_id))
    text = (
        "Готовы к росту?\n\n"
        f"💳 Ваш баланс: <b>{bal}</b>\n\n"
        "Доступно:\n"
        "• 📈 Оформить заказ в приложении\n"
        "• ➕ Пополнить баланс (карта или крипта)\n"
        "• Если средств не хватает — бот сам откроет пополнение"
    )
    await bot.send_message(chat_id, text, parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(user_id))


async def _notify_new_referral(referrer_id: int, referred_id: int) -> None:
    text_ref = (
        "✅ <b>Новый реферал</b>\n\n"
        f"Клиент: {_user_link(referred_id, 'профиль')} (ID <code>{referred_id}</code>)\n"
        "Источник: реф‑ссылка"
    )
    try:
        await bot.send_message(int(referrer_id), text_ref, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except Exception:
        pass

    _notify_manager_bg(
        "👥 <b>Новый реферал</b>\n\n"
        f"Реферер: {_user_link(referrer_id, 'профиль')} (ID <code>{referrer_id}</code>)\n"
        f"Клиент: {_user_link(referred_id, 'профиль')} (ID <code>{referred_id}</code>)\n"
        "Источник: реф‑ссылка"
    )


# =========================
# COMMANDS
# =========================
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
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

    if payload.startswith("ref_"):
        ref_raw = payload.replace("ref_", "").strip()
        try:
            ref_id = int(ref_raw)
        except Exception:
            ref_id = 0
        if ref_id > 0 and ref_id != user_id:
            created = _set_referrer(user_id, ref_id)
            if created:
                await _notify_new_referral(ref_id, user_id)

    await send_welcome(message.chat.id, user_id)


@dp.message(Command("balance"))
async def cmd_balance(message: types.Message):
    user_id = message.from_user.id
    bal = _get_balance_kopecks(user_id)
    await message.answer(
        f"💳 Ваш баланс: <b>{_format_rub_from_kopecks(bal)}</b>",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="➕ Пополнить", callback_data="balance_topup")],
                [
                    InlineKeyboardButton(
                        text="🚀 Открыть приложение",
                        web_app=WebAppInfo(url=_webapp_url_for_user(user_id)),
                    )
                ],
            ]
        ),
        parse_mode=ParseMode.HTML,
    )



@dp.message(Command("myid"))
async def cmd_myid(message: types.Message):
    """Показывает chat_id и user_id (удобно для настройки MANAGER_CHAT_ID)."""
    await message.answer(
        "🆔 <b>Идентификаторы</b>\n\n"
        f"chat_id: <code>{message.chat.id}</code>\n"
        f"user_id: <code>{message.from_user.id}</code>\n"
        f"username: @{(message.from_user.username or '-')}"
        ,
        parse_mode=ParseMode.HTML,
    )


@dp.message(Command("manager_get"))
async def cmd_manager_get(message: types.Message):
    target = _resolve_manager_target()
    db_val = _get_setting('manager_chat_id')
    await message.answer(
        "👤 <b>Менеджер</b>\n\n"
        f"MANAGER_CHAT_ID(env): <code>{MANAGER_CHAT_ID or '-'}</code>\n"
        f"manager_chat_id(db): <code>{db_val or '-'}</code>\n"
        f"MANAGER_USERNAME(env): <code>@{MANAGER_USERNAME or '-'}</code>\n\n"
        f"➡️ Текущий target: <code>{target or '-'}</code>"
        ,
        parse_mode=ParseMode.HTML,
    )


@dp.message(Command("manager_set"))
async def cmd_manager_set(message: types.Message):
    """
    Сохраняет chat_id менеджера в БД (settings.manager_chat_id).
    Использование:
      /manager_set            -> сохранить текущий chat_id
      /manager_set -100123... -> сохранить указанный chat_id
    Доступ: только если username отправителя совпадает с MANAGER_USERNAME.
    """
    sender_uname = (message.from_user.username or '').lstrip('@').strip().lower()
    allowed_uname = (MANAGER_USERNAME or '').lstrip('@').strip().lower()
    if allowed_uname and sender_uname != allowed_uname:
        await message.answer("❌ Недостаточно прав для этой команды.")
        return

    raw = ''
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            raw = parts[1].strip()
    if not raw:
        raw = str(message.chat.id)

    # validate: must be int-like or @username-like
    candidate = raw
    if candidate.startswith('@'):
        candidate = candidate[1:]
        candidate = '@' + candidate
    else:
        try:
            int(candidate)
        except Exception:
            await message.answer("❌ Укажите корректный chat_id (число) или @username.")
            return

    _set_setting('manager_chat_id', raw)
    await message.answer(
        "✅ Сохранено.\n\n"
        f"manager_chat_id(db) = <code>{raw}</code>\n"
        "Теперь уведомления о заказах будут уходить на этот чат."
        ,
        parse_mode=ParseMode.HTML,
    )

# =========================
# CALLBACKS
# =========================
@dp.callback_query(lambda c: c.data == "balance_topup")
async def balance_topup_callback(callback: types.CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    awaiting_custom_topup_card.discard(user_id)
    awaiting_custom_topup_crypto.discard(user_id)
    await show_topup_amounts(callback.message.chat.id, user_id, need_rub=0)


@dp.callback_query(lambda c: c.data.startswith("topup_recommend_"))
async def topup_recommend_callback(callback: types.CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    try:
        need = int(callback.data.replace("topup_recommend_", "") or 0)
    except Exception:
        need = 0

    if need <= 0:
        await show_topup_amounts(callback.message.chat.id, user_id, need_rub=0)
        return

    text = (
        "💳 <b>Пополнить баланс</b>\n\n"
        f"Вы выбрали сумму: <b>{need} ₽</b>\n\n"
        "Выберите способ оплаты:"
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"💳 Картой / ЮKassa — {need} ₽", callback_data=f"topup_amount_{need}")],
            [InlineKeyboardButton(text=f"🪙 Криптой — {need} ₽", callback_data=f"crypto_amount_{need}")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"back_topup_{need}")],
        ]
    )
    await callback.message.answer(text, reply_markup=kb, parse_mode=ParseMode.HTML)


@dp.callback_query(lambda c: c.data == "back_main")
async def back_main_callback(callback: types.CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    awaiting_custom_topup_card.discard(user_id)
    awaiting_custom_topup_crypto.discard(user_id)
    await send_welcome(callback.message.chat.id, user_id)


@dp.callback_query(lambda c: c.data.startswith("back_topup_"))
async def back_topup_callback(callback: types.CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    awaiting_custom_topup_crypto.discard(user_id)
    try:
        need = int(callback.data.replace("back_topup_", "") or 0)
    except Exception:
        need = 0
    await show_topup_amounts(callback.message.chat.id, user_id, need_rub=need)


# =========================
# TOPUP: CARD
# =========================
@dp.callback_query(lambda c: c.data.startswith("topup_amount_"))
async def topup_amount_callback(callback: types.CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    amount = callback.data.replace("topup_amount_", "")

    if amount == "custom":
        awaiting_custom_topup_card.add(user_id)
        awaiting_custom_topup_crypto.discard(user_id)
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

    try:
        await send_topup_invoice(
            chat_id=callback.message.chat.id,
            user_id=user_id,
            amount_rub=amount_rub,
            reason="Пополнение баланса",
        )
    except Exception as e:
        await callback.message.answer(f"❌ Не удалось создать инвойс (карта): {e}")


async def send_partner_program(chat_id: int, user_id: int) -> None:
    ref_link = await _build_ref_link(user_id)
    link_line = f"<code>{ref_link}</code>" if ref_link else "❌ Не удалось получить ссылку. Укажите BOT_USERNAME."
    ref_balance = _format_rub_from_kopecks(_get_ref_balance_kopecks(user_id))
    text = (
        "💎 <b>Партнёрская программа</b>\n\n"
        "Приглашайте друзей и зарабатывайте на этом!\n\n"
        "Вы будете получать: <b>40%</b> от каждой покупки вашего реферала.\n"
        "Размещайте свою реферальную ссылку в чатах/каналах и рассказывайте друзьям — "
        "получайте пассивный доход с каждой покупки.\n\n"
        "При каждом новом реферале бот уведомит вас и покажет, сколько вы заработали.\n\n"
        f"💰 Ваш реф‑баланс: <b>{ref_balance}</b>\n\n"
        f"Ваша реферальная ссылка:\n{link_line}"
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📋 Скопировать ссылку", callback_data="ref_copy")],
            [InlineKeyboardButton(text="💸 Вывести", callback_data="ref_withdraw")],
        ]
    )
    await bot.send_message(chat_id, text, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)


@dp.message(F.text == "💳 Баланс")
async def menu_balance(message: types.Message):
    await send_welcome(message.chat.id, message.from_user.id, include_greeting=False)


@dp.message(F.text == "➕ Пополнить баланс 💳")
async def menu_balance_topup(message: types.Message):
    await show_topup_amounts(message.chat.id, message.from_user.id, need_rub=0)


@dp.message(F.text == "🚀 Накрутка и баланс")
async def menu_boost_balance(message: types.Message):
    await send_quick_menu(message.chat.id, message.from_user.id)


@dp.message(F.text == "🤝 Партнерская программа")
async def menu_partner_program(message: types.Message):
    await send_partner_program(message.chat.id, message.from_user.id)


@dp.message(F.text == "🆘 Поддержка")
async def menu_support(message: types.Message):
    text = (
        "🆘 <b>Поддержка</b>\n\n"
        "Напишите менеджеру, он поможет с любыми вопросами:\n"
        f"@{MANAGER_USERNAME}"
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💬 Написать в поддержку", url=f"https://t.me/{MANAGER_USERNAME}")]
        ]
    )
    await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=kb)


@dp.message(F.text == "📜 Правила")
async def menu_rules(message: types.Message):
    text = (
        "📜 <b>Правила и соглашения</b>\n\n"
        "Уважаемый пользователь! Ознакомьтесь с нашими правилами и соглашениями магазина.\n\n"
        "✅ <b>Boost Shop гарантирует:</b>\n"
        "• Работоспособность аккаунтов в течение 48 часов при АКТИВНОЙ гарантии.\n"
        "• Продажу аккаунтов только в «одни руки».\n"
        "• Замену аккаунтов при их невалидности по вине поставщика при АКТИВНОЙ гарантии.\n"
        "• Возврат средств, если аккаунт заменить невозможно. Возврат осуществляется только на платежные системы:\n"
        "◦ CryptoBot\n\n"
        "⏳ Время выдачи товара в нашем магазине — до 24 часов.\n"
        "⚠️ Обмен или возврат товара, если он вам не подошел или не устроил, невозможен.\n\n"
        "📋 <b>Общие правила:</b>\n"
        "• Мы не раздаем товары бесплатно.\n"
        "• Администрация сервиса оставляет за собой право отказать в обслуживании и поддержке клиенту без объяснения причин.\n"
        "• Сервис не несет ответственности за ваши действия.\n"
        "• Совершая покупку, вы автоматически соглашаетесь со всеми правилами сервиса.\n\n"
        "<b>Обратите внимание:</b>\n"
        "Наши прокси не гарантируют доступ к ресурсам: Банки, Госуслуги, Авито."
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔐 Политика использования", url="https://telegra.ph/Politika-ispolzovaniya-01-31")]
        ]
    )
    await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)


@dp.message(F.text == "🔐 Политика")
async def menu_policy(message: types.Message):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔐 Открыть политику", url="https://telegra.ph/Politika-ispolzovaniya-01-31")]
        ]
    )
    await message.answer("🔐 Политика использования:", reply_markup=kb)


@dp.callback_query(lambda c: c.data == "ref_copy")
async def ref_copy(callback: types.CallbackQuery):
    await callback.answer()
    ref_link = await _build_ref_link(callback.from_user.id)
    if not ref_link:
        await callback.message.answer("❌ Не удалось получить ссылку. Укажите BOT_USERNAME.")
        return
    await callback.message.answer(f"📋 Ваша реферальная ссылка:\n<code>{ref_link}</code>", parse_mode=ParseMode.HTML)


@dp.callback_query(lambda c: c.data == "ref_withdraw")
async def ref_withdraw(callback: types.CallbackQuery):
    await callback.answer()
    bal = _format_rub_from_kopecks(_get_ref_balance_kopecks(callback.from_user.id))
    text = (
        "💸 <b>Вывод средств</b>\n\n"
        f"Ваш реф‑баланс: <b>{bal}</b>\n\n"
        f"Напишите менеджеру @{MANAGER_USERNAME} — он подсчитает сумму и пришлёт выплату.\n"
        "Можно отправить скрин/детали для ускорения."
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="ref_back")]]
    )
    await callback.message.answer(text, parse_mode=ParseMode.HTML, reply_markup=kb)


@dp.callback_query(lambda c: c.data == "ref_back")
async def ref_back(callback: types.CallbackQuery):
    await callback.answer()
    await send_partner_program(callback.message.chat.id, callback.from_user.id)


@dp.message(F.text)
async def custom_amount_handler(message: types.Message):
    user_id = message.from_user.id
    raw = (message.text or "").strip().replace(",", ".")
    menu_texts = {
        "💳 Баланс",
        "🚀 Накрутка и баланс",
        "🤝 Партнерская программа",
        "🆘 Поддержка",
        "📜 Правила",
    }
    if user_id not in awaiting_custom_topup_card and user_id not in awaiting_custom_topup_crypto:
        if (message.text or "").strip() in menu_texts:
            return

    if user_id in awaiting_custom_topup_card:
        try:
            rub = Decimal(raw)
        except InvalidOperation:
            rub = Decimal("0")

        if rub <= 0:
            await message.answer("❌ Введите число больше 0 (например: 250).")
            return

        awaiting_custom_topup_card.discard(user_id)
        amount_rub = int(rub.quantize(Decimal("1"), rounding=ROUND_HALF_UP))
        try:
            await send_topup_invoice(chat_id=message.chat.id, user_id=user_id, amount_rub=amount_rub, reason="Пополнение баланса")
        except Exception as e:
            await message.answer(f"❌ Не удалось создать инвойс (карта): {e}")
        return

    if user_id in awaiting_custom_topup_crypto:
        try:
            rub = Decimal(raw)
        except InvalidOperation:
            rub = Decimal("0")

        if rub <= 0:
            await message.answer("❌ Введите число больше 0 (например: 250).")
            return

        awaiting_custom_topup_crypto.discard(user_id)
        await start_crypto_topup(message.chat.id, user_id, rub)
        return


# =========================
# TOPUP: CRYPTO
# =========================
@dp.callback_query(lambda c: c.data.startswith("topup_crypto_menu_"))
async def topup_crypto_menu_callback(callback: types.CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    awaiting_custom_topup_crypto.discard(user_id)

    try:
        need = int(callback.data.replace("topup_crypto_menu_", "") or 0)
    except Exception:
        need = 0

    if not CRYPTO_PAY_TOKEN:
        await callback.message.answer(
            "🪙 <b>Крипто-оплата</b>\n\n"
            "❌ Сейчас не настроена на стороне бота.\n"
            "Нужно подключить Crypto Pay API (@CryptoBot → Crypto Pay → Create App) и установить CRYPTO_PAY_TOKEN.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="back_main")]]
            ),
        )
        return

    kb = crypto_amounts_kb(need_rub=need)
    await callback.message.answer(
        "🪙 <b>Пополнение криптой</b>\n\nВыберите сумму (в рублях). Оплатить можно USDT/TON/BTC/ETH/USDC.",
        parse_mode=ParseMode.HTML,
        reply_markup=kb,
    )


@dp.callback_query(lambda c: c.data.startswith("crypto_amount_"))
async def crypto_amount_callback(callback: types.CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    amount = callback.data.replace("crypto_amount_", "")

    if amount == "custom":
        awaiting_custom_topup_crypto.add(user_id)
        awaiting_custom_topup_card.discard(user_id)
        await callback.message.answer(
            "Введите сумму пополнения в рублях для крипто-оплаты (например: <b>250</b>).",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="back_main")]]
            ),
        )
        return

    try:
        amount_rub = Decimal(str(int(amount)))
    except Exception:
        amount_rub = Decimal("0")

    if amount_rub <= 0:
        await callback.message.answer("❌ Некорректная сумма.")
        return

    await start_crypto_topup(callback.message.chat.id, user_id, amount_rub)


async def start_crypto_topup(chat_id: int, user_id: int, amount_rub: Decimal) -> None:
    try:
        inv = await _create_crypto_invoice(user_id, amount_rub)
    except Exception as e:
        await bot.send_message(chat_id, f"❌ Не удалось создать крипто-инвойс: {e}")
        return

    invoice_id = int(inv.get("invoice_id"))
    url = _crypto_invoice_url(inv)
    if not url:
        await bot.send_message(chat_id, "❌ Crypto Pay не вернул ссылку на оплату. Проверьте настройки.")
        return

    amount_kopecks = int((amount_rub.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP) * 100).to_integral_value())
    _store_crypto_invoice(invoice_id, user_id, amount_kopecks, f"{amount_rub:.2f}", url)

    text = (
        "🧾 <b>Инвойс на крипто-оплату создан</b>\n\n"
        f"Сумма: <b>{amount_rub:.2f} ₽</b>\n"
        "Оплатите по ссылке ниже. После оплаты баланс начислится автоматически."
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💎 Оплатить криптой", url=url)],
            [InlineKeyboardButton(text="🔄 Проверить оплату", callback_data=f"crypto_check_{invoice_id}")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="back_main")],
        ]
    )
    await bot.send_message(chat_id, text, reply_markup=kb, parse_mode=ParseMode.HTML)


@dp.callback_query(lambda c: c.data.startswith("crypto_check_"))
async def crypto_check_callback(callback: types.CallbackQuery):
    await callback.answer()
    invoice_id = int(callback.data.replace("crypto_check_", "") or 0)
    meta = _get_crypto_invoice_meta(invoice_id)
    if not meta:
        await callback.message.answer("❌ Инвойс не найден.")
        return

    try:
        result = await _crypto_call("getInvoices", {"invoice_ids": str(invoice_id), "count": 100})
        inv = result[0] if isinstance(result, list) and result else None
    except Exception as e:
        await callback.message.answer(f"❌ Ошибка проверки: {e}")
        return

    if not inv:
        await callback.message.answer("❌ Инвойс не найден в Crypto Pay.")
        return

    status = str(inv.get("status"))
    if status == "paid":
        await _process_paid_crypto_invoice(invoice_id)
        await callback.message.answer("✅ Оплата подтверждена. Баланс начислен.")
        return

    await callback.message.answer(
        f"ℹ️ Статус инвойса: <b>{status}</b>. Если вы уже оплатили — подождите минуту и проверьте снова.",
        parse_mode=ParseMode.HTML,
    )


# =========================
# TELEGRAM PAYMENTS (CARD)
# =========================
async def send_topup_invoice(chat_id: int, user_id: int, amount_rub: int, reason: str = "Пополнение") -> None:
    if not PROVIDER_TOKEN:
        raise RuntimeError("PROVIDER_TOKEN is not set (set env PROVIDER_TOKEN=...)")

    amount_kopecks = int(amount_rub) * 100
    order_id = str(uuid.uuid4())
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


@dp.pre_checkout_query()
async def pre_checkout_handler(pre_checkout_query: types.PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)


@dp.message(lambda message: message.successful_payment is not None)
async def successful_payment_handler(message: types.Message):
    p = message.successful_payment

    telegram_charge_id = p.telegram_payment_charge_id
    if _is_tg_payment_processed(telegram_charge_id):
        await message.answer("ℹ️ Платёж уже учтён ранее.", parse_mode=ParseMode.HTML)
        return

    if p.currency != "RUB":
        await message.answer("❌ Валюта платежа не поддерживается.", parse_mode=ParseMode.HTML)
        return

    amount = int(p.total_amount)
    user_id = message.from_user.id

    new_bal = _add_balance_kopecks(user_id, amount)

    _mark_tg_payment_processed(telegram_charge_id, p.provider_payment_charge_id, user_id, amount)

    # Попытаемся автоматически оформить ожидающую покупку
    await _finalize_pending_order_if_possible(user_id, source="ЮKassa")

    text = (
        "✅ <b>Баланс пополнен!</b>\n\n"
        f"Сумма: <b>{_format_rub_from_kopecks(amount)}</b>\n"
        f"Текущий баланс: <b>{_format_rub_from_kopecks(new_bal)}</b>\n\n"
        "Откройте приложение — баланс отобразится автоматически."
    )
    await message.answer(text, reply_markup=open_webapp_kb(user_id), parse_mode=ParseMode.HTML)

    _notify_manager_bg(
        "💳 <b>Пополнение картой (Telegram Payments / ЮKassa)</b>\n\n"
        f"Пользователь: {_user_link(user_id, 'профиль')} (ID <code>{user_id}</code>)\n"
        f"Сумма: <b>{_format_rub_from_kopecks(amount)}</b>"
    )


# =========================
# WEBAPP DATA
# =========================
def _parse_decimal_rub(raw: Any) -> Decimal:
    try:
        return Decimal(str(raw).replace(",", ".")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except Exception:
        return Decimal("0")


def _build_order_from_webapp(data: Dict[str, Any]) -> Dict[str, Any]:
    # Нормализуем под единый формат
    order_id = str(data.get("order_id") or data.get("orderId") or data.get("id") or "")
    title = str(data.get("title") or "Заказ")
    platform = str(data.get("platform") or "—")
    service = str(data.get("service") or "—")
    category_name = str(
        data.get("category_name")
        or data.get("categoryName")
        or data.get("category")
        or "—"
    )
    quantity = data.get("quantity") or data.get("qty") or data.get("count") or "—"
    total = _parse_decimal_rub(data.get("total_price") or data.get("total") or "0")
    link = str(
        data.get("link")
        or data.get("target")
        or data.get("url")
        or data.get("post_link")
        or ""
    ).strip()

    return {
        "order_id": order_id or str(uuid.uuid4()),
        "title": title,
        "platform": platform,
        "service": service,
        "category_name": category_name,
        "quantity": quantity,
        "total_price": f"{total:.2f}",
        "link": link,
    }


@dp.message(F.web_app_data)
async def webapp_data_handler(message: types.Message):
    user_id = message.from_user.id
    try:
        data = json.loads(message.web_app_data.data)
    except Exception:
        await message.answer("❌ Не удалось прочитать данные из приложения.")
        return

    action = data.get("action")

    if action == "open_topup":
        try:
            need_rub = int(float(data.get("need_rub", 0) or 0))
        except Exception:
            need_rub = 0
        await show_topup_amounts(message.chat.id, user_id, need_rub=need_rub)
        return

    if action == "reserve_order":
        order = _build_order_from_webapp(data)
        total_rub = _parse_decimal_rub(order.get("total_price"))
        amount_kopecks = int((total_rub * 100).to_integral_value())

        try:
            order_json = json.dumps(order, ensure_ascii=False)
        except Exception:
            order_json = "{}"

        _set_pending_order(user_id, order["order_id"], amount_kopecks, order_json)
        need = max(0, amount_kopecks - _get_balance_kopecks(user_id))
        need_rub = int((need + 99) // 100)
        await message.answer(
            "🧾 <b>Заказ сохранён</b>\n\n"
            "Сейчас открою пополнение. После оплаты бот автоматически оформит покупку.",
            parse_mode=ParseMode.HTML,
        )
        await show_topup_amounts(message.chat.id, user_id, need_rub=need_rub)
        return

    if action != "pay_with_balance":
        await message.answer("ℹ️ Команда получена.")
        return

    order = _build_order_from_webapp(data)
    total_rub = _parse_decimal_rub(order.get("total_price"))
    if total_rub <= 0:
        await message.answer("❌ Некорректная сумма заказа.")
        return

    amount_kopecks = int((total_rub * 100).to_integral_value())
    if amount_kopecks > 5_000_000 * 100:
        await message.answer("❌ Сумма заказа слишком большая.")
        return

    # Идемпотентность
    if _is_order_processed(order["order_id"]):
        await message.answer(
            "ℹ️ Этот заказ уже был оплачен ранее.",
            parse_mode=ParseMode.HTML,
            reply_markup=open_webapp_kb(user_id, success_order=order),
        )
        return

    ok, before, after = _try_debit_balance_kopecks(user_id, amount_kopecks)
    if not ok:
        need = max(0, amount_kopecks - before)
        need_rub = int((need + 99) // 100)

        try:
            order_json = json.dumps(order, ensure_ascii=False)
        except Exception:
            order_json = "{}"
        _set_pending_order(user_id, order["order_id"], amount_kopecks, order_json)

        await show_topup_amounts(message.chat.id, user_id, need_rub=need_rub)
        await message.answer(
            "❌ <b>Недостаточно средств</b>\n\n"
            f"Сохранён заказ <code>{order['order_id']}</code>.\n"
            "Пополните баланс — бот автоматически оформит покупку.",
            parse_mode=ParseMode.HTML,
        )
        return

    # Оплата с баланса успешна
    try:
        order_json = json.dumps(order, ensure_ascii=False)
    except Exception:
        order_json = "{}"
    _mark_order_processed(order["order_id"], user_id, amount_kopecks, order_json)

    # История заказов (для вкладки «Профиль» в WebApp)
    try:
        _create_order(user_id, order, amount_kopecks)
    except Exception as e:
        logger.warning(f"Failed to store order in history: {e}")

    try:
        await _apply_referral_reward(user_id, order.get("order_id"), amount_kopecks)
    except Exception:
        pass

    text = (
        "✅ <b>Оплата списана с баланса</b>\n\n"
        f"{_order_text_block(order)}\n\n"
        f"Списано: <b>{_format_rub_from_kopecks(amount_kopecks)}</b>\n"
        f"Баланс: <b>{_format_rub_from_kopecks(after)}</b>\n\n"
        f"Менеджер: <b>@{MANAGER_USERNAME}</b>"
    )
    await message.answer(text, reply_markup=open_webapp_kb(user_id, success_order=order), parse_mode=ParseMode.HTML)

    mgr_text = (
        "🧾 <b>Новый оплаченный заказ</b>\n\n"
        f"Покупатель: {_user_link(user_id, 'профиль')} (ID <code>{user_id}</code>)\n"
        "Оплата: <b>Баланс</b>\n\n"
        f"{_order_text_block(order)}"
    )
    _notify_manager_bg(mgr_text, reply_markup=_mgr_confirm_kb(order.get("order_id")))



# =========================
# MANAGER COMMANDS (main bot only)
# =========================
def _is_manager_chat(chat_id: Optional[int], username: Optional[str] = None) -> bool:
    try:
        if MANAGER_CHAT_ID_INT is not None:
            return int(chat_id or 0) == int(MANAGER_CHAT_ID_INT)
    except Exception:
        pass
    if username:
        return username.lstrip("@").lower() == str(MANAGER_USERNAME or "").lstrip("@").lower()
    return False


def _mgr_orders_kb(order_id: str, status: str) -> Optional[InlineKeyboardMarkup]:
    if status == "done":
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"mgr_done:{order_id}")]]
    )


def _mgr_confirm_kb(order_id: str) -> Optional[InlineKeyboardMarkup]:
    if not order_id:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"mgr_done:{order_id}")]]
    )


@dp.message(Command("orders"))
@dp.message(F.text == "🧾 Заказы")
async def mgr_orders(message: types.Message):
    if not _is_manager_chat(message.chat.id, getattr(message.from_user, "username", None)):
        return
    orders = _list_all_orders(limit=20)
    if not orders:
        await message.answer("Заказов ещё не было.")
        return

    for o in orders:
        oid = o.get("order_id")
        uid = o.get("user_id")
        status = o.get("status") or "new"
        created = o.get("created_at")
        amount_k = int(o.get("amount_kopecks") or 0)
        cat = o.get("category_name") or "—"
        order_payload = o.get("order") or {}
        if isinstance(order_payload, dict) and not order_payload.get("order_id"):
            order_payload["order_id"] = oid

        text_msg = f"""🧾 <b>Заказ</b>
ID: <code>{oid}</code>
Статус: <b>{status}</b>
Покупатель ID: <code>{uid}</code>
Категория: <b>{cat}</b>
Сумма (DB): <b>{_format_rub_from_kopecks(amount_k)}</b>
Создан: <code>{created}</code>

{_order_text_block(order_payload)}"""
        kb = _mgr_orders_kb(str(oid), str(status))
        await message.answer(text_msg, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)


@dp.callback_query(lambda c: (c.data or "").startswith("mgr_done:"))
async def mgr_done(callback: types.CallbackQuery):
    if not callback.message or not _is_manager_chat(callback.message.chat.id, getattr(callback.from_user, "username", None)):
        await callback.answer("Нет доступа", show_alert=True)
        return
    oid = str((callback.data or "").split(":", 1)[1] or "").strip()
    if not oid:
        await callback.answer("Некорректный заказ", show_alert=True)
        return
    try:
        _set_order_status(oid, "done")
    except Exception:
        pass
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await callback.answer("Отмечено как выполнено")
    try:
        await callback.message.reply(f"✅ Заказ <code>{oid}</code> отмечен как выполненный.", parse_mode=ParseMode.HTML)
    except Exception:
        pass

    # Notify user about completion
    try:
        row = _get_order_by_id(oid)
        if row and int(row.get("user_id") or 0) > 0:
            uid = int(row.get("user_id"))
            await bot.send_message(
                uid,
                f"✅ Ваш заказ <code>{oid}</code> выполнен.",
                parse_mode=ParseMode.HTML,
            )
    except Exception:
        pass


# =========================
# TELEGRAM INITDATA VERIFY (for WebApp API)
# =========================
def _parse_init_data(init_data: str) -> Dict[str, str]:
    pairs = parse_qsl(init_data or "", keep_blank_values=True)
    return {k: v for k, v in pairs}


def _validate_init_data(init_data: str, bot_token: str) -> Dict[str, Any]:
    """
    Verifies Telegram WebApp initData (hash) and returns parsed payload.
    """
    data = _parse_init_data(init_data)
    recv_hash = data.pop("hash", "")
    if not recv_hash:
        raise ValueError("no_hash")

    # Build data check string
    check_list = [f"{k}={v}" for k, v in sorted(data.items())]
    data_check_string = "\n".join(check_list)

    secret_key = hmac.new(b"WebAppData", bot_token.encode("utf-8"), hashlib.sha256).digest()
    calc_hash = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()

    if not hmac.compare_digest(calc_hash, recv_hash):
        raise ValueError("bad_hash")

    # Parse user object if present
    user = {}
    if "user" in data:
        try:
            user = json.loads(data["user"])
        except Exception:
            user = {}
    return {"data": data, "user": user}




def _validate_init_data_any(init_data: str) -> Dict[str, Any]:
    """Validate initData against the main bot token only."""
    token = (BOT_TOKEN or "").strip()
    if not token:
        raise ValueError("no_token")
    return _validate_init_data(init_data, token)


def _user_id_from_init(init_data: str) -> int:
    payload = _validate_init_data_any(init_data)
    u = payload.get("user") or {}
    uid = int(u.get("id") or 0)
    if uid <= 0:
        raise ValueError("no_user_id")
    return uid


# =========================
# API SERVER (aiohttp)
# =========================
_MAIN_BOT_USERNAME: Optional[str] = None


async def _api_json(request: web.Request, obj: Dict[str, Any], status: int = 200) -> web.Response:
    return web.json_response(obj, status=status, dumps=lambda o: json.dumps(o, ensure_ascii=False))


def _get_initdata_from_request(request: web.Request) -> str:
    init_data = request.headers.get("X-Tg-Init-Data", "") or ""
    if not init_data:
        # Allow passing in JSON for debugging
        try:
            body = request.get("_json_body") or {}
            init_data = str(body.get("initData") or "")
        except Exception:
            init_data = ""
    return init_data


@web.middleware
async def _json_mw(request: web.Request, handler):
    if request.method in ("POST", "PUT", "PATCH"):
        ct = request.headers.get("Content-Type", "")
        if "application/json" in ct:
            try:
                request["_json_body"] = await request.json()
            except Exception:
                request["_json_body"] = {}
    return await handler(request)


@web.middleware
async def _cors_mw(request: web.Request, handler):
    cors_headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, X-Tg-Init-Data",
        "Access-Control-Max-Age": "86400",
    }
    if request.method == "OPTIONS":
        return web.Response(status=204, headers=cors_headers)
    resp = await handler(request)
    try:
        for k, v in cors_headers.items():
            resp.headers[k] = v
    except Exception:
        pass
    return resp


async def api_health(request: web.Request) -> web.Response:
    return await _api_json(request, {"ok": True, "ts": int(time.time())})


async def api_meta(request: web.Request) -> web.Response:
    return await _api_json(
        request,
        {
            "ok": True,
            "main_bot_username": _MAIN_BOT_USERNAME or "",
            "api_base": API_BASE_PATH,
        },
    )


async def api_balance(request: web.Request) -> web.Response:
    try:
        init_data = _get_initdata_from_request(request)
        user_id = _user_id_from_init(init_data)
        bal_k = _get_balance_kopecks(user_id)
        return await _api_json(
            request,
            {"ok": True, "balance_kopecks": bal_k, "balance_rub": f"{bal_k/100:.2f}"},
        )
    except Exception as e:
        return await _api_json(request, {"ok": False, "error": str(e)}, status=400)


async def api_orders_list(request: web.Request) -> web.Response:
    try:
        init_data = _get_initdata_from_request(request)
        user_id = _user_id_from_init(init_data)
        body = request.get("_json_body") or {}
        limit = int(body.get("limit") or 50)
        limit = max(1, min(200, limit))
        orders = _list_orders(user_id, limit=limit)
        slim = [
            {
                "order_id": o["order_id"],
                "created_at": o["created_at"],
                "category_name": o.get("category_name") or "—",
                "status": o.get("status") or "new",
                "link": (
                    (o.get("order") or {}).get("link")
                    or (o.get("order") or {}).get("target")
                    or (o.get("order") or {}).get("url")
                    or ""
                ),
            }
            for o in orders
        ]
        return await _api_json(request, {"ok": True, "orders": slim})
    except Exception as e:
        return await _api_json(request, {"ok": False, "error": str(e)}, status=400)


async def api_orders_detail(request: web.Request) -> web.Response:
    try:
        init_data = _get_initdata_from_request(request)
        user_id = _user_id_from_init(init_data)
        body = request.get("_json_body") or {}
        order_id = str(body.get("order_id") or "").strip()
        if not order_id:
            raise ValueError("no_order_id")
        row = _get_order(user_id, order_id)
        if not row:
            raise ValueError("order_not_found")
        amount_k = int(row.get("amount_kopecks") or 0)
        return await _api_json(
            request,
            {
                "ok": True,
                "order_id": row.get("order_id"),
                "created_at": row.get("created_at"),
                "amount_kopecks": amount_k,
                "amount_rub": f"{amount_k/100:.2f}",
                "category_name": row.get("category_name") or "—",
                "status": row.get("status") or "new",
                "order": row.get("order") or {},
            },
        )
    except Exception as e:
        return await _api_json(request, {"ok": False, "error": str(e)}, status=400)


async def _send_order_notifications(user_id: int, order: Dict[str, Any], amount_kopecks: int, balance_after: int) -> None:
    """Sends order confirmation to user and manager via the main bot."""

    # User confirmation must be sent from the main bot.
    try:
        text_user = f"""✅ <b>Покупка оформлена</b>

{_order_text_block(order)}

Списано: <b>{_format_rub_from_kopecks(amount_kopecks)}</b>
Баланс: <b>{_format_rub_from_kopecks(balance_after)}</b>"""
        await bot.send_message(user_id, text_user, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except Exception as e:
        logger.warning(f"Order notify to user failed: {e}")

    text_mgr = f"""🧾 <b>Новый заказ</b>

Покупатель: {_user_link(user_id, 'профиль')} (ID <code>{user_id}</code>)
Источник: <b>WebApp API</b>

{_order_text_block(order)}"""
    order_id = str(order.get("order_id") or order.get("orderId") or order.get("id") or "")
    _notify_manager_bg(text_mgr, reply_markup=_mgr_confirm_kb(order_id))


async def api_orders_create(request: web.Request) -> web.Response:
    """Creates order and debits balance atomically from DB.

    Returns:
      result=success + order_id + balance_rub
      result=insufficient + need_rub + balance_rub
    """
    try:
        init_data = _get_initdata_from_request(request)
        user_id = _user_id_from_init(init_data)
        body = request.get("_json_body") or {}
        order_in = body.get("order") if isinstance(body.get("order"), dict) else {}
        pay_method = str(body.get("pay_method") or "balance")

        if pay_method != "balance":
            raise ValueError("unsupported_pay_method")

        # Normalize order and amount
        order_norm = _build_order_from_webapp(order_in)
        final_order_id = str(order_norm.get("order_id") or "").strip() or uuid.uuid4().hex
        order_norm["order_id"] = final_order_id

        # Idempotency: if the same order_id was already processed, do not debit again
        if _is_order_processed(final_order_id):
            bal_k = _get_balance_kopecks(user_id)
            return await _api_json(
                request,
                {
                    "ok": True,
                    "result": "success",
                    "order_id": final_order_id,
                    "balance_kopecks": bal_k,
                    "balance_rub": f"{bal_k/100:.2f}",
                },
            )

        total_rub = _parse_decimal_rub(order_norm.get("total_price"))
        amount_kopecks = int((total_rub * 100).to_integral_value())
        if amount_kopecks <= 0:
            raise ValueError("bad_amount")

        ok, before, after = _try_debit_balance_kopecks(user_id, amount_kopecks)
        if not ok:
            need = max(0, amount_kopecks - before)
            need_rub = int((need + 99) // 100)
            return await _api_json(
                request,
                {
                    "ok": True,
                    "result": "insufficient",
                    "need_rub": need_rub,
                    "balance_kopecks": before,
                    "balance_rub": f"{before/100:.2f}",
                },
            )

        # Mark as processed (idempotency/audit)
        try:
            order_json = json.dumps(order_norm, ensure_ascii=False)
        except Exception:
            order_json = "{}"
        _mark_order_processed(final_order_id, user_id, amount_kopecks, order_json)

        # Create order record (paid) and default status "new"
        _create_order(user_id, order_norm, amount_kopecks)

        # Send notifications (user + manager)
        try:
            await _send_order_notifications(user_id, order_norm, amount_kopecks, after)
        except Exception:
            pass

        try:
            await _apply_referral_reward(user_id, final_order_id, amount_kopecks)
        except Exception:
            pass

        return await _api_json(
            request,
            {
                "ok": True,
                "result": "success",
                "order_id": final_order_id,
                "balance_kopecks": after,
                "balance_rub": f"{after/100:.2f}",
            },
        )
    except Exception as e:
        return await _api_json(request, {"ok": False, "error": str(e)}, status=400)


async def start_api_server() -> web.AppRunner:
    app = web.Application(middlewares=[_cors_mw, _json_mw])
    base = API_BASE_PATH

    app.router.add_get(f"{base}/health", api_health)
    app.router.add_post(f"{base}/meta", api_meta)
    app.router.add_post(f"{base}/balance", api_balance)
    app.router.add_post(f"{base}/orders/list", api_orders_list)
    app.router.add_post(f"{base}/orders/detail", api_orders_detail)
    app.router.add_post(f"{base}/orders/create", api_orders_create)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, API_HOST, API_PORT)
    await site.start()
    logger.info(f"API server started on http://{API_HOST}:{API_PORT}{base}")
    return runner


# =========================
# RUN
# =========================
async def main():
    logger.info("Bot starting...")

    # Safety: ensure no webhook is set
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass

    watcher_task = None
    if CRYPTO_PAY_TOKEN:
        watcher_task = asyncio.create_task(crypto_invoices_watcher())

    # Resolve main bot username for deep-links
    global _MAIN_BOT_USERNAME
    try:
        me = await bot.get_me()
        _MAIN_BOT_USERNAME = getattr(me, 'username', '') or ''
    except Exception:
        _MAIN_BOT_USERNAME = ''

    api_runner = None
    try:
        api_runner = await start_api_server()
    except Exception as e:
        logger.error(f"API server failed to start: {e}")

    try:
        tasks = [asyncio.create_task(dp.start_polling(bot))]
        await asyncio.gather(*tasks)
    finally:
        if watcher_task:
            watcher_task.cancel()
        if api_runner:
            try:
                await api_runner.cleanup()
            except Exception:
                pass
        if _crypto_session and not _crypto_session.closed:
            await _crypto_session.close()


if __name__ == "__main__":
    asyncio.run(main())
