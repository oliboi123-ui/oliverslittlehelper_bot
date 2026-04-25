import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import socket
import sys
import threading
import time
import uuid
import zlib
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)


LOGGER = logging.getLogger("gatekeeper_bot")

PAYPAL_MAIN_LOOP: asyncio.AbstractEventLoop | None = None
PAYPAL_BOT: Any | None = None
PAYPAL_WEBHOOK_SERVER: ThreadingHTTPServer | None = None
PAYPAL_WEBHOOK_THREAD: threading.Thread | None = None
PAYPAL_CHECKOUT_BLOCKED_REASON: str | None = None


LOG_RECORD_RESERVED_KEYS = set(
    logging.LogRecord("", 0, "", 0, "", (), None).__dict__.keys()
)


class MaxLevelFilter(logging.Filter):
    def __init__(self, max_level: int) -> None:
        super().__init__()
        self.max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno < self.max_level


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z"),
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in sorted(record.__dict__.items()):
            if key.startswith("_") or key in LOG_RECORD_RESERVED_KEYS:
                continue
            payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=True, default=str)


def configure_logging() -> None:
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.INFO)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    stdout_handler.addFilter(MaxLevelFilter(logging.WARNING))
    stdout_handler.setFormatter(JsonLogFormatter())

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.WARNING)
    stderr_handler.setFormatter(JsonLogFormatter())

    root_logger.addHandler(stdout_handler)
    root_logger.addHandler(stderr_handler)

    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def log_event(event: str, level: int = logging.INFO, **attributes: Any) -> None:
    safe_attributes = {
        key: value
        for key, value in attributes.items()
        if value is not None
    }
    LOGGER.log(level, event.replace("_", " "), extra={"event": event, **safe_attributes})

ENV_PATH = Path(__file__).with_name(".env")
try:
    DISPLAY_TIMEZONE = ZoneInfo("Europe/Stockholm")
except ZoneInfoNotFoundError:
    DISPLAY_TIMEZONE = timezone.utc

BUDGET_OPTIONS = [
    {"key": "under_50", "label": "Under $50", "floor": 0, "ceiling": 49, "priority": "low"},
    {"key": "50_99", "label": "$50-$99", "floor": 50, "ceiling": 99, "priority": "low"},
    {"key": "100_199", "label": "$100-$199", "floor": 100, "ceiling": 199, "priority": "normal"},
    {"key": "200_249", "label": "$200-$249", "floor": 200, "ceiling": 249, "priority": "priority"},
    {"key": "250_499", "label": "$250-$499", "floor": 250, "ceiling": 499, "priority": "priority"},
    {"key": "500_plus", "label": "$500+", "floor": 500, "ceiling": None, "priority": "priority"},
]

TEST_SESSION_ID_OFFSET = 9_000_000_000_000_000

QUICK_PHRASES = [
    {
        "key": "bought_before",
        "label": "\U0001F4AC Bought before?",
        "text": "Have you purchased content from me before?",
    },
    {
        "key": "what_content",
        "label": "\u2728 What do you want?",
        "text": "What kind of content are you looking for today?",
    },
    {
        "key": "budget",
        "label": "\U0001F4B8 Ask budget",
        "text": "What budget range are you thinking for this?",
    },
    {
        "key": "price_reply",
        "label": "\U0001F4B0 Budget reply",
        "text": None,
    },
]

PRICE_RULES = [
    {
        "key": "jerkoff",
        "keywords": ("jerkoff", "jerk off", "j/o", "joi", "wank"),
        "label": "jerkoff vids",
        "minimum": 250,
    },
    {
        "key": "ass_spreading",
        "keywords": ("ass", "spreading", "spread"),
        "label": "ass content (spreading)",
        "minimum": 300,
    },
    {
        "key": "fingering",
        "keywords": ("finger", "fingering"),
        "label": "fingering",
        "minimum": 400,
    },
]

DEFAULT_CONTENT_PRICE = {
    "label": "2-3 vanilla photos",
    "minimum": 75,
}


TEMPLATES = {
    "already_pending": "Your request is already pending review. Please wait for a decision.",
    "low_priority": (
        "Your request is saved in my slower review queue. You do not need to resend anything."
    ),
    "banned": "This request is closed. Do not contact this bot again.",
    "rejected": "Sorry, this request was not approved.",
    "payment_reminder": (
        "Quick reminder: please use the pinned payment link when you are ready. "
        "Payment keeps your purchase information easy to find."
    ),
    "payment_confirmed": "Payment marked as received. Thank you.",
    "clarification_request": (
        "Thanks. Could you briefly clarify what you are looking for? "
        "A short answer is enough."
    ),
}

def template(name: str) -> str:
    return TEMPLATES[name]


def of_username_help_message() -> str:
    return (
        "Welcome.\n\n"
        "This private access bot is reserved for active OnlyFans subscribers. "
        "It helps keep requests organized while access stays personal and private.\n\n"
        "To continue, please send your OnlyFans username.\n\n"
        "This means the username on your OnlyFans profile, not your Telegram username, display name, email, or a link.\n\n"
        "Examples:\n"
        "- If your profile is onlyfans.com/example, send: example\n"
        "- If you never chose a custom username, it may look like @u123456789. "
        "In that case, send the full @u number username."
    )


def of_username_not_verified_message(of_username: str | None = None) -> str:
    submitted = clean_text(of_username, empty="that username")
    return (
        f"I couldn't verify an active OnlyFans subscription for {submitted}.\n\n"
        "Most of the time this is just the wrong name being entered. Please send the "
        "OnlyFans username from your profile, not your display name, Telegram name, "
        "email, or a full link.\n\n"
        "Examples:\n"
        "- If your profile is onlyfans.com/example, send: example\n"
        "- If you never chose a custom username, it may look like @u123456789. "
        "In that case, send the full @u number username."
    )


def application_confirmation_message(record: dict[str, Any]) -> str:
    return (
        "Thanks. I have your request.\n\n"
        f"OnlyFans username: {clean_text(record.get('of_username'))}\n"
        f"Looking for: {clean_text(record.get('purchase_intent'))}\n\n"
        "I review requests based on availability, fit, and the kind of request. "
        "If it looks like a fit, I will follow up here."
    )


def low_priority_message() -> str:
    return (
        "Thanks. I have your request saved in my slower review queue.\n\n"
        "I review requests based on availability, fit, and the kind of request. "
        "You do not need to resend anything. If it looks like a fit, I will follow up here."
    )


def load_dotenv_file() -> None:
    if not ENV_PATH.exists():
        return

    for raw_line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def get_access_duration_days() -> int:
    return int(os.getenv("ACCESS_DURATION_DAYS", "30"))


def get_data_dir() -> Path:
    return Path(
        os.getenv("BOT_DATA_DIR", "").strip()
        or os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "").strip()
        or Path(__file__).resolve().parent
    )


def get_state_path() -> Path:
    return get_data_dir() / "bot_state.json"


def get_ofauth_base_url() -> str:
    return os.getenv("OFAUTH_BASE_URL", "https://api.ofauth.com").rstrip("/")


def get_ofauth_user_agent() -> str:
    return os.getenv(
        "OFAUTH_USER_AGENT",
        "oliverslittlehelper-bot/1.0 (+https://github.com/oliboi123-ui/oliverslittlehelper_bot)",
    ).strip()


def get_ofauth_timeout_seconds() -> float:
    return float(os.getenv("OFAUTH_TIMEOUT_SECONDS", "10"))


def get_ofauth_max_pages() -> int:
    return int(os.getenv("OFAUTH_MAX_PAGES", "5"))


def get_ofauth_page_size() -> int:
    return int(os.getenv("OFAUTH_PAGE_SIZE", "10"))


def get_relay_group_id() -> int | None:
    value = get_optional_env("RELAY_ADMIN_GROUP_ID")
    if not value:
        return None
    return int(value)


def get_payment_url() -> str:
    return os.getenv("PAYMENT_URL", "https://paypal.me/mirage22m").strip()


def get_paypal_env() -> str:
    return os.getenv("PAYPAL_ENV", "live").strip().lower()


def get_paypal_api_base() -> str:
    return "https://api-m.sandbox.paypal.com" if get_paypal_env() == "sandbox" else "https://api-m.paypal.com"


def get_paypal_client_id() -> str | None:
    return get_optional_env("PAYPAL_CLIENT_ID")


def get_paypal_client_secret() -> str | None:
    return get_optional_env("PAYPAL_CLIENT_SECRET")


def get_paypal_webhook_id() -> str | None:
    return get_optional_env("PAYPAL_WEBHOOK_ID")


def get_paypal_public_base_url() -> str | None:
    base_url = get_optional_env("PAYPAL_PUBLIC_BASE_URL")
    if not base_url:
        return None
    base_url = base_url.strip()
    if not base_url:
        return None
    if not base_url.startswith(("http://", "https://")):
        base_url = "https://" + base_url.lstrip("/")
    return base_url.rstrip("/")


def get_paypal_return_url() -> str | None:
    base_url = get_paypal_public_base_url()
    if not base_url:
        return None
    return base_url.rstrip("/") + "/paypal/return"


def get_paypal_cancel_url() -> str | None:
    base_url = get_paypal_public_base_url()
    if not base_url:
        return None
    return base_url.rstrip("/") + "/paypal/cancel"


def get_paypal_webhook_url() -> str | None:
    base_url = get_paypal_public_base_url()
    if not base_url:
        return None
    return base_url.rstrip("/") + "/paypal/webhook"


def get_paypal_webhook_port() -> int:
    return int(os.getenv("PAYPAL_WEBHOOK_PORT", os.getenv("PORT", "8080")))


def relay_is_configured() -> bool:
    return get_relay_group_id() is not None


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def format_datetime_for_user(value: str | datetime | None, *, empty: str = "Not set") -> str:
    if isinstance(value, datetime):
        parsed = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        parsed = parsed.astimezone(timezone.utc)
    else:
        parsed = parse_iso(value)
    if parsed is None:
        return empty
    return parsed.astimezone(DISPLAY_TIMEZONE).strftime("%d %b %Y, %H:%M")


def format_date_for_user(value: str | datetime | None, *, empty: str = "Not set") -> str:
    if isinstance(value, datetime):
        parsed = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        parsed = parsed.astimezone(timezone.utc)
    else:
        parsed = parse_iso(value)
    if parsed is None:
        return empty
    return parsed.astimezone(DISPLAY_TIMEZONE).strftime("%d %b %Y")


def format_duration_for_user(seconds: float | int | None) -> str:
    if seconds is None:
        return "Unknown"
    value = f"{float(seconds):.1f}".rstrip("0").rstrip(".")
    return f"{value}s"


def display_name(user_data: dict[str, Any]) -> str:
    parts = [
        str(user_data.get("first_name") or "").strip(),
        str(user_data.get("last_name") or "").strip(),
    ]
    full_name = " ".join(part for part in parts if part).strip()
    if full_name:
        return full_name
    username = str(user_data.get("telegram_username") or "").strip()
    if username:
        return f"@{username}"
    return "Unknown"


def telegram_handle(user_data: dict[str, Any]) -> str | None:
    username = str(user_data.get("telegram_username") or "").strip()
    if username:
        return f"@{username}"
    return None


def clean_text(value: Any, *, empty: str = "Not provided") -> str:
    text = str(value or "").strip()
    return text or empty


def format_person_label(user_data: dict[str, Any]) -> str:
    name = display_name(user_data)
    handle = telegram_handle(user_data)
    if handle and handle != name:
        return f"{name} ({handle})"
    return name


def access_status_line(record: dict[str, Any]) -> str:
    if record.get("status") == "approved":
        return f"Approved until {format_date_for_user(record.get('expires_at'))}"
    if record.get("status") == "expired":
        return "Expired"
    return "Not approved yet"


def verification_badge(record: dict[str, Any]) -> str:
    status = record.get("subscription_status") or "unknown"
    if status == "active":
        return "Verified"
    if status == "inactive":
        return "Not verified"
    return "Not checked"


def verification_summary(record: dict[str, Any]) -> str:
    status = record.get("subscription_status") or "unknown"
    if status == "active":
        return "Verified"
    if status == "inactive":
        return "Unverified"
    return "Verification pending"


def count_line(count: int, singular: str, plural: str | None = None) -> str:
    word = singular if count == 1 else (plural or f"{singular}s")
    return f"{count} {word}"


def load_state() -> dict[str, Any]:
    state_path = get_state_path()
    if not state_path.exists():
        return {
            "admin_chat_id": None,
            "users": {},
            "relay_topics": {},
            "content_vault_chat_id": None,
            "vault_items": {},
            "ppv_items": {},
            "paypal_orders": {},
            "test_sessions": {},
        }
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        LOGGER.warning("State file was invalid JSON, starting fresh.")
        return {
            "admin_chat_id": None,
            "users": {},
            "relay_topics": {},
            "content_vault_chat_id": None,
            "vault_items": {},
            "ppv_items": {},
            "paypal_orders": {},
            "test_sessions": {},
        }
    state.setdefault("admin_chat_id", None)
    state.setdefault("users", {})
    state.setdefault("relay_topics", {})
    state.setdefault("content_vault_chat_id", None)
    state.setdefault("vault_items", {})
    state.setdefault("ppv_items", {})
    state.setdefault("paypal_orders", {})
    state.setdefault("test_sessions", {})
    return state


def save_state(state: dict[str, Any]) -> None:
    state_path = get_state_path()
    state_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = state_path.with_suffix(".json.tmp")
    temp_path.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temp_path.replace(state_path)


def get_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_optional_env(name: str) -> str | None:
    value = os.getenv(name, "").strip()
    return value or None


def normalize_username(value: str) -> str:
    return value.strip().lstrip("@").lower()


def normalize_of_username(value: str) -> str:
    return normalize_username(value)


def get_budget_option(key: str | None) -> dict[str, Any] | None:
    for option in BUDGET_OPTIONS:
        if option["key"] == key:
            return option
    return None


def default_user_record() -> dict[str, Any]:
    return {
        "status": "new",
        "of_username": None,
        "telegram_username": None,
        "first_name": None,
        "last_name": None,
        "approved_at": None,
        "expires_at": None,
        "last_checked_at": None,
        "subscription_status": "unknown",
        "subscription_expires_at": None,
        "onlyfans_user_id": None,
        "budget_range_key": None,
        "budget_range_label": None,
        "budget_floor": None,
        "review_priority": "normal",
        "purchase_intent": None,
        "queued_at": None,
        "contact_mode": None,
        "relay_topic_id": None,
        "relay_topic_name": None,
        "relay_enabled_at": None,
        "relay_closed_at": None,
        "callback_ref": None,
        "direct_shared_at": None,
        "identity_proof_requested_at": None,
        "identity_proof_sent_at": None,
        "payment_message_id": None,
        "payment_status": "not_requested",
        "payment_requested_at": None,
        "payment_confirmed_at": None,
        "payment_reminded_at": None,
        "payment_due_amount": None,
        "payment_currency": "USD",
        "paypal_order_id": None,
        "payment_context": "manual",
        "payment_item_keys": [],
        "payment_fulfilled_at": None,
        "payment_fulfilled_order_id": None,
        "ppv_selected_item_key": None,
        "ppv_selected_item_title": None,
        "ppv_selected_item_price": None,
        "ppv_cart": [],
        "ppv_delivery_history": {},
        "content_unlocks": [],
        "not_fit_at": None,
        "trash_at": None,
        "banned_at": None,
        "ban_reason": None,
        "clarification_requested_at": None,
        "clarification_response": None,
        "internal_label": None,
    }


def get_user_record(state: dict[str, Any], user_id: int) -> dict[str, Any]:
    users = state.setdefault("users", {})
    state.setdefault("relay_topics", {})
    record = users.setdefault(str(user_id), default_user_record())
    for key, value in default_user_record().items():
        record.setdefault(key, value)
    return record


def ensure_callback_ref(record: dict[str, Any], user_id: int) -> str:
    secret = get_required_env("BOT_TOKEN")
    digest = hmac.new(secret.encode("utf-8"), str(user_id).encode("utf-8"), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest[:12]).decode("ascii").rstrip("=")


def resolve_callback_user_id(state: dict[str, Any], callback_ref: str | None) -> int | None:
    if not callback_ref:
        return None
    if callback_ref.isdigit():
        return int(callback_ref)
    for user_id_text, record in state.get("users", {}).items():
        if ensure_callback_ref(record, int(user_id_text)) == callback_ref:
            return int(user_id_text)
    return None


def get_test_session_user_id(user_id: int) -> int:
    return TEST_SESSION_ID_OFFSET + abs(user_id)


def get_test_sessions(state: dict[str, Any]) -> dict[str, Any]:
    return state.setdefault("test_sessions", {})


def is_test_mode_active(state: dict[str, Any], user: Any) -> bool:
    if user is None:
        return False
    session = get_test_sessions(state).get(str(user.id))
    return bool(session and session.get("active"))


def get_active_private_record(state: dict[str, Any], user: Any) -> dict[str, Any]:
    if is_test_mode_active(state, user):
        record = get_user_record(state, get_test_session_user_id(user.id))
        record["test_mode"] = True
        return record
    return get_user_record(state, user.id)


def is_private_buyer_test_context(state: dict[str, Any], update: Any) -> bool:
    chat = getattr(update, "effective_chat", None)
    user = getattr(update, "effective_user", None)
    return bool(chat and user and chat.type == "private" and is_test_mode_active(state, user))


def is_sandbox_record(record: dict[str, Any]) -> bool:
    return bool(record.get("test_mode"))


def get_test_mode_flow(state: dict[str, Any], user: Any) -> str:
    session = get_test_sessions(state).get(str(user.id))
    if session is None:
        return "buyer"
    return str(session.get("mode") or "buyer").strip().lower() or "buyer"


def begin_test_mode_session(state: dict[str, Any], user: Any, *, mode: str = "buyer") -> dict[str, Any]:
    mode = str(mode or "buyer").strip().lower()
    if mode not in {"buyer", "full"}:
        mode = "buyer"
    session = get_test_sessions(state).setdefault(str(user.id), {})
    session["active"] = True
    session["started_at"] = to_iso(utc_now())
    session["buyer_user_id"] = get_test_session_user_id(user.id)
    session["buyer_chat_id"] = user.id
    session["mode"] = mode

    record = get_user_record(state, session["buyer_user_id"])
    record.clear()
    record.update(default_user_record())
    record["test_mode"] = True
    record["test_mode_buyer_user_id"] = session["buyer_user_id"]
    record["test_mode_chat_id"] = session["buyer_chat_id"]
    record["test_mode_flow"] = mode
    record["test_mode_started_at"] = session["started_at"]
    record["payment_message_id"] = None
    record["payment_status"] = "not_requested"
    record["payment_requested_at"] = None
    record["payment_confirmed_at"] = None
    record["payment_reminded_at"] = None
    record["payment_due_amount"] = None
    record["payment_currency"] = "USD"
    record["payment_context"] = "manual"
    record["payment_item_keys"] = []
    record["payment_fulfilled_at"] = None
    record["payment_fulfilled_order_id"] = None
    record["ppv_selected_item_key"] = None
    record["ppv_selected_item_title"] = None
    record["ppv_selected_item_price"] = None
    record["ppv_cart"] = []
    record["ppv_delivery_history"] = {}
    record["content_unlocks"] = []
    record["not_fit_at"] = None
    record["trash_at"] = None
    record["banned_at"] = None
    record["ban_reason"] = None
    record["clarification_requested_at"] = None
    record["clarification_response"] = None
    record["internal_label"] = None

    current_time = utc_now()
    if mode == "full":
        record["status"] = "new"
        record["telegram_username"] = "test_mode"
        record["first_name"] = "Test"
        record["last_name"] = "Buyer"
        record["approved_at"] = None
        record["expires_at"] = None
        record["last_checked_at"] = None
        record["subscription_status"] = "unknown"
        record["subscription_expires_at"] = None
        record["onlyfans_user_id"] = None
        record["budget_range_key"] = None
        record["budget_range_label"] = None
        record["budget_floor"] = None
        record["review_priority"] = "normal"
        record["purchase_intent"] = None
        record["queued_at"] = None
        record["contact_mode"] = None
        record["relay_topic_id"] = None
        record["relay_topic_name"] = None
        record["relay_enabled_at"] = None
        record["relay_closed_at"] = None
        record["direct_shared_at"] = None
        record["identity_proof_requested_at"] = None
        record["identity_proof_sent_at"] = None
        begin_application(record)
    else:
        pseudo_id = abs(int(user.id)) % 10000
        record["status"] = "approved"
        record["telegram_username"] = f"buyer_{pseudo_id}"
        record["first_name"] = "Mika"
        record["last_name"] = "Vale"
        record["approved_at"] = to_iso(current_time)
        record["expires_at"] = to_iso(current_time + timedelta(days=get_access_duration_days()))
        record["last_checked_at"] = None
        record["subscription_status"] = "active"
        record["subscription_expires_at"] = to_iso(current_time + timedelta(days=get_access_duration_days()))
        record["onlyfans_user_id"] = f"test-{session['buyer_user_id']}"
        record["budget_range_key"] = "test"
        record["budget_range_label"] = "Test buyer"
        record["budget_floor"] = 250
        record["review_priority"] = "normal"
        record["purchase_intent"] = "Testing the buyer journey"
        record["queued_at"] = to_iso(current_time)
        record["contact_mode"] = None
        record["relay_topic_id"] = None
        record["relay_topic_name"] = None
        record["relay_enabled_at"] = None
        record["relay_closed_at"] = None
        record["direct_shared_at"] = None
        record["identity_proof_requested_at"] = None
        record["identity_proof_sent_at"] = None
    record["test_mode_started_at"] = session["started_at"]
    return record


def end_test_mode_session(state: dict[str, Any], user: Any) -> None:
    session = get_test_sessions(state).get(str(user.id))
    if session is not None:
        session["active"] = False
        session["ended_at"] = to_iso(utc_now())


def get_test_mode_chat_id(state: dict[str, Any], sandbox_user_id: int) -> int | None:
    for admin_user_id, session in get_test_sessions(state).items():
        if int(session.get("buyer_user_id") or 0) == sandbox_user_id and session.get("active"):
            chat_id = session.get("buyer_chat_id")
            if chat_id is not None:
                return int(chat_id)
    return None


def get_relay_topics(state: dict[str, Any]) -> dict[str, Any]:
    return state.setdefault("relay_topics", {})


def set_contact_mode(record: dict[str, Any], mode: str, *, now: datetime | None = None) -> None:
    current_time = now or utc_now()
    previous_mode = record.get("contact_mode")
    record["contact_mode"] = mode
    if mode == "relay":
        record["relay_enabled_at"] = to_iso(current_time)
    elif mode == "direct":
        record["direct_shared_at"] = to_iso(current_time)
        if previous_mode == "relay":
            record["relay_closed_at"] = to_iso(current_time)


def relay_mode_enabled(record: dict[str, Any]) -> bool:
    return (
        record.get("status") == "approved"
        and record.get("contact_mode") == "relay"
        and record.get("relay_topic_id") is not None
    )


def testmode_contact_available(state: dict[str, Any], user_id: int, record: dict[str, Any]) -> bool:
    if not record.get("test_mode"):
        return False
    if record.get("status") != "approved":
        return False
    return get_buyer_chat_id(record, user_id) is not None


def contact_mode_label(record: dict[str, Any]) -> str:
    mode = str(record.get("contact_mode") or "").strip().lower()
    if mode == "relay":
        return "Relay"
    if mode == "direct" or (not mode and record.get("status") == "approved"):
        return "Direct"
    return "Not set"


def truncate_text(value: str, limit: int) -> str:
    text = value.strip()
    if len(text) <= limit:
        return text
    return text[: max(limit - 3, 1)].rstrip() + "..."


def build_relay_topic_name(record: dict[str, Any]) -> str:
    parts = [display_name(record)]
    of_username = clean_text(record.get("of_username"), empty="").strip()
    if of_username:
        parts.append(of_username)
    topic_name = " | ".join(part for part in parts if part)
    return truncate_text(topic_name or "Buyer relay", 120)


def relay_intro_text(user_id: int, record: dict[str, Any]) -> str:
    lines = [
        "Relay opened",
        format_person_label(record),
        f"ID: {user_id}",
        f"OnlyFans: {clean_text(record.get('of_username'))}",
        verification_summary(record),
        f"Budget: {budget_line(record)}",
        f"Wants: {clean_text(record.get('purchase_intent'))}",
        "",
        "Reply in this topic to message this buyer.",
        "Messages starting with // stay in this topic only.",
        "Quick buttons below keep replies and access actions fast.",
    ]
    return "\n".join(lines)


def relay_access_message(record: dict[str, Any]) -> str:
    access_date = format_date_for_user(record.get("expires_at"))
    return (
        "You're approved.\n\n"
        "I reply personally here through this bot to keep access private and organized.\n"
        "If you want identity confirmation first, just ask and I'll send a short hello video saying your name.\n\n"
        f"Access is tied to your active OnlyFans subscription and is valid until {access_date}.\n"
        "If your subscription ends, this bot access is revoked."
    )


def direct_access_message(private_username: str, record: dict[str, Any]) -> str:
    access_date = format_date_for_user(record.get("expires_at"))
    return (
        f"Approved. You can message me at {private_username}.\n\n"
        f"Access is tied to your active OnlyFans subscription and is valid until {access_date}.\n"
        "If your subscription ends, access is removed."
    )


def format_currency_amount(amount: int | float | str | None, currency: str = "USD") -> str:
    if amount is None:
        return ""
    if isinstance(amount, str):
        try:
            amount = float(amount)
        except ValueError:
            return amount
    if currency.upper() == "USD":
        return f"${float(amount):.2f}"
    return f"{currency.upper()} {float(amount):.2f}"


def format_currency_amount(amount: int | float | str | None, currency: str = "USD") -> str:
    if amount is None:
        return ""
    if isinstance(amount, str):
        try:
            amount = float(amount)
        except ValueError:
            return amount
    if currency.upper() == "USD":
        return f"${float(amount):.2f}"
    return f"{currency.upper()} {float(amount):.2f}"


def payment_message(record: dict[str, Any] | None = None) -> str:
    record = record or {}
    ppv_title = clean_text(record.get("ppv_selected_item_title"), empty="")
    ppv_price = record.get("ppv_selected_item_price")
    due_amount = record.get("payment_due_amount")
    due_currency = str(record.get("payment_currency") or "USD")
    lines = ["Checkout ready"]
    if due_amount is not None:
        lines.append(f"Amount due: {format_currency_amount(due_amount, due_currency)}")
    if ppv_title:
        ppv_line = f"PPV selected: {ppv_title}"
        if ppv_price is not None:
            ppv_line += f" ({format_currency_amount(ppv_price, due_currency)})"
        lines.append(ppv_line)
    lines.append("")
    lines.append("Tap the button below to continue.")
    return "\n".join(lines)


def build_payment_keyboard(
    user_id: int | None = None,
    record: dict[str, Any] | None = None,
    *,
    payment_url: str | None = None,
) -> InlineKeyboardMarkup:
    callback_ref = ensure_callback_ref(record or {}, user_id) if user_id is not None else None
    pay_target_url = payment_url or get_payment_url()
    rows = [[InlineKeyboardButton("Open PayPal", url=pay_target_url)]]
    if record is not None and record.get("test_mode") and record.get("status") == "approved" and callback_ref is not None:
        rows.append([InlineKeyboardButton("PPV catalog", callback_data=f"ppv:menu:{callback_ref}")])
    return InlineKeyboardMarkup(rows)


def paypal_is_configured() -> bool:
    return bool(
        get_paypal_client_id()
        and get_paypal_client_secret()
        and get_paypal_webhook_id()
        and get_paypal_public_base_url()
    )


def get_paypal_orders(state: dict[str, Any]) -> dict[str, Any]:
    return state.setdefault("paypal_orders", {})


def schedule_bot_message(*, chat_id: int, text: str, **kwargs: Any) -> None:
    if PAYPAL_BOT is None or PAYPAL_MAIN_LOOP is None:
        return
    asyncio.run_coroutine_threadsafe(
        PAYPAL_BOT.send_message(chat_id=chat_id, text=text, **kwargs),
        PAYPAL_MAIN_LOOP,
    )


def get_payment_context(record: dict[str, Any] | None) -> str:
    return clean_text((record or {}).get("payment_context"), empty="manual").strip().lower() or "manual"


def get_payment_item_keys(record: dict[str, Any] | None) -> list[str]:
    raw_keys = (record or {}).get("payment_item_keys")
    if not isinstance(raw_keys, list):
        return []
    keys: list[str] = []
    for raw_key in raw_keys:
        key = clean_text(raw_key, empty="")
        if key and key not in keys:
            keys.append(key)
    return keys


def get_payment_alert_chat_id(state: dict[str, Any], record: dict[str, Any]) -> int | None:
    if record.get("test_mode"):
        relay_group_id = get_relay_group_id()
        if relay_group_id is not None:
            return int(relay_group_id)
    admin_chat_id = state.get("admin_chat_id")
    if admin_chat_id is not None:
        try:
            return int(admin_chat_id)
        except (TypeError, ValueError):
            return None
    return None


async def send_manual_release_request(
    bot: Any,
    state: dict[str, Any],
    user_id: int,
    record: dict[str, Any],
    *,
    heading: str,
    reason: str,
    order_id: str | None = None,
) -> None:
    admin_chat_id = get_payment_alert_chat_id(state, record)
    if admin_chat_id is None:
        LOGGER.warning("No admin chat available for manual release request: %s", reason)
        return
    alert_lines = [
        heading,
        "",
        format_review_card(user_id, record, heading),
        "",
        f"Reason: {reason}",
    ]
    if order_id:
        alert_lines.append(f"Order: {order_id}")
    alert_lines.append("Please press Unlock content if this payment should release a PPV now.")
    await bot.send_message(
        chat_id=admin_chat_id,
        text="\n".join(alert_lines),
        reply_markup=build_post_approval_keyboard(user_id, record),
        protect_content=True,
    )


def ppv_request_record_update(record: dict[str, Any], item_key: str, item: dict[str, Any]) -> None:
    price = item.get("price")
    record["payment_context"] = "ppv"
    record["payment_item_keys"] = [item_key]
    record["ppv_cart"] = [item_key]
    record["ppv_selected_item_key"] = item_key
    record["ppv_selected_item_title"] = clean_text(item.get("title"), empty=item_key)
    record["ppv_selected_item_price"] = price if isinstance(price, (int, float)) else None
    record["payment_due_amount"] = price if isinstance(price, (int, float)) else None


async def fulfill_paid_content(
    bot: Any,
    state: dict[str, Any],
    user_id: int,
    record: dict[str, Any],
    *,
    order_id: str | None = None,
    target_chat_id: int | None = None,
) -> list[str]:
    ensure_content_delivery_allowed(record)

    current_order_id = clean_text(order_id or record.get("paypal_order_id"), empty="")
    if current_order_id and clean_text(record.get("payment_fulfilled_order_id"), empty="") == current_order_id:
        return []

    item_keys = get_payment_item_keys(record)
    if not item_keys:
        item_keys = get_ppv_cart(record)[:]
    if not item_keys and clean_text(record.get("ppv_selected_item_key"), empty=""):
        item_keys = [clean_text(record.get("ppv_selected_item_key"), empty="")]
    is_ppv_payment = (
        get_payment_context(record) == "ppv"
        or bool(get_payment_item_keys(record))
        or bool(get_ppv_cart(record))
        or bool(clean_text(record.get("ppv_selected_item_key"), empty=""))
    )
    if not item_keys:
        if is_ppv_payment:
            await send_manual_release_request(
                bot,
                state,
                user_id,
                record,
                heading="PPV release needs review",
                reason="Payment was confirmed, but no PPV item was attached to this order.",
                order_id=current_order_id or record.get("paypal_order_id"),
            )
        return []

    orders = get_paypal_orders(state)
    order_entry = orders.get(current_order_id) if current_order_id else None
    if order_entry is not None:
        delivery_status = clean_text(order_entry.get("delivery_status"), empty="pending").strip().lower() or "pending"
        if delivery_status in {"delivering", "fulfilled"}:
            return []
        order_entry["delivery_status"] = "delivering"
        order_entry["delivery_started_at"] = to_iso(utc_now())
        save_state(state)

    delivered_labels: list[str] = []
    try:
        if len(item_keys) == 1:
            delivered_label = await deliver_unlock_content(
                bot,
                state,
                user_id,
                record,
                target_chat_id=target_chat_id,
            )
            delivered_labels.append(delivered_label.replace("Delivered ", "", 1))
        else:
            for item_key in item_keys:
                item = get_ppv_items(state).get(item_key)
                if item is None:
                    continue
                await deliver_ppv_item(
                    bot,
                    state,
                    user_id,
                    item_key,
                    record=record,
                    target_chat_id=target_chat_id,
                )
                unlocks = record.setdefault("content_unlocks", [])
                if isinstance(unlocks, list):
                    unlocks.append(item_key)
                delivered_labels.append(build_ppv_item_label(item_key, item))
    except Exception:
        if order_entry is not None:
            order_entry["delivery_status"] = "failed"
            order_entry["delivery_failed_at"] = to_iso(utc_now())
        save_state(state)
        await send_manual_release_request(
            bot,
            state,
            user_id,
            record,
            heading="PPV release failed",
            reason="The bot confirmed payment but failed while sending the content.",
            order_id=current_order_id or record.get("paypal_order_id"),
        )
        raise

    if not delivered_labels:
        if order_entry is not None:
            order_entry["delivery_status"] = "needs_review"
            order_entry["delivery_failed_at"] = to_iso(utc_now())
        save_state(state)
        if is_ppv_payment:
            await send_manual_release_request(
                bot,
                state,
                user_id,
                record,
                heading="PPV release needs review",
                reason="Payment is confirmed, but no deliverable PPV item was found in the order.",
                order_id=current_order_id or record.get("paypal_order_id"),
            )
        return []

    record["payment_fulfilled_at"] = to_iso(utc_now())
    record["payment_fulfilled_order_id"] = current_order_id or record.get("paypal_order_id")
    record["payment_item_keys"] = []
    record["ppv_cart"] = []
    record["ppv_selected_item_key"] = None
    record["ppv_selected_item_title"] = None
    record["ppv_selected_item_price"] = None
    if order_entry is not None:
        order_entry["delivery_status"] = "fulfilled"
        order_entry["delivery_completed_at"] = to_iso(utc_now())
        order_entry["delivered_items"] = delivered_labels[:]
    save_state(state)
    return delivered_labels


def schedule_paid_content_fulfillment(
    user_id: int,
    *,
    order_id: str | None = None,
) -> None:
    if PAYPAL_BOT is None or PAYPAL_MAIN_LOOP is None:
        return

    async def _runner() -> None:
        try:
            state = load_state()
            record = get_user_record(state, user_id)
            await fulfill_paid_content(
                PAYPAL_BOT,
                state,
                user_id,
                record,
                order_id=order_id,
            )
        except Exception:
            LOGGER.exception("Auto-fulfillment failed for buyer %s.", user_id)

    asyncio.run_coroutine_threadsafe(_runner(), PAYPAL_MAIN_LOOP)


def paypal_api_request_json(method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    client_id = get_paypal_client_id()
    client_secret = get_paypal_client_secret()
    if not client_id or not client_secret:
        raise RuntimeError("PayPal is not configured.")
    request_data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        request_data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    auth = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
    headers["Authorization"] = f"Basic {auth}"
    request = urllib_request.Request(
        get_paypal_api_base().rstrip("/") + path,
        data=request_data,
        method=method,
        headers=headers,
    )
    try:
        with urllib_request.urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8")
    except urllib_error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"PayPal API error ({exc.code}): {detail}") from exc
    if not body:
        return {}
    return json.loads(body)


def paypal_get_access_token() -> str:
    client_id = get_paypal_client_id()
    client_secret = get_paypal_client_secret()
    if not client_id or not client_secret:
        raise RuntimeError("PayPal is not configured.")
    auth = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
    request = urllib_request.Request(
        get_paypal_api_base().rstrip("/") + "/v1/oauth2/token",
        data=urllib_parse.urlencode({"grant_type": "client_credentials"}).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Basic {auth}",
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    try:
        with urllib_request.urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib_error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"PayPal token request failed ({exc.code}): {detail}") from exc
    token = str(payload.get("access_token") or "").strip()
    if not token:
        raise RuntimeError("PayPal did not return an access token.")
    return token


def paypal_create_order(
    state: dict[str, Any],
    user_id: int,
    *,
    amount: int | float,
    currency: str = "USD",
    description: str,
    purpose: str,
) -> tuple[str, str]:
    if not paypal_is_configured():
        raise RuntimeError("PayPal checkout is not configured.")
    return_url = get_paypal_return_url()
    cancel_url = get_paypal_cancel_url()
    if not return_url or not cancel_url:
        raise RuntimeError("Set PAYPAL_PUBLIC_BASE_URL so the bot can build return and cancel URLs.")
    token = paypal_get_access_token()
    invoice_id = f"tg-{user_id}-{int(time.time())}-{uuid.uuid4().hex[:8]}"
    order_payload = {
        "intent": "CAPTURE",
        "purchase_units": [
            {
                "custom_id": str(user_id),
                "invoice_id": invoice_id,
                "description": description[:127],
                "amount": {
                    "currency_code": currency.upper(),
                    "value": f"{float(amount):.2f}",
                },
            }
        ],
        "application_context": {
            "brand_name": "Oliver's Little Helper",
            "return_url": return_url,
            "cancel_url": cancel_url,
        },
    }
    request = urllib_request.Request(
        get_paypal_api_base().rstrip("/") + "/v2/checkout/orders",
        data=json.dumps(order_payload).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "PayPal-Request-Id": f"{user_id}-{uuid.uuid4().hex}",
        },
    )
    try:
        with urllib_request.urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib_error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"PayPal order creation failed ({exc.code}): {detail}") from exc
    order_id = str(payload.get("id") or "").strip()
    if not order_id:
        raise RuntimeError("PayPal did not return an order id.")
    approval_url = ""
    for link in payload.get("links", []):
        if link.get("rel") in {"approve", "payer-action"} and link.get("href"):
            approval_url = str(link["href"])
            break
    if not approval_url:
        raise RuntimeError("PayPal did not return an approval URL.")
    orders = get_paypal_orders(state)
    record = get_user_record(state, user_id)
    record["paypal_order_id"] = order_id
    record["payment_due_amount"] = int(amount) if float(amount).is_integer() else float(amount)
    record["payment_currency"] = currency.upper()
    record["payment_status"] = "pending"
    record["payment_requested_at"] = to_iso(utc_now())
    orders[order_id] = {
        "user_id": user_id,
        "purpose": purpose,
        "amount": f"{float(amount):.2f}",
        "currency": currency.upper(),
        "description": description,
        "approval_url": approval_url,
        "status": "created",
        "created_at": to_iso(utc_now()),
    }
    save_state(state)
    return order_id, approval_url


def paypal_capture_order(order_id: str) -> dict[str, Any]:
    if not paypal_is_configured():
        raise RuntimeError("PayPal checkout is not configured.")
    token = paypal_get_access_token()
    request = urllib_request.Request(
        get_paypal_api_base().rstrip("/") + f"/v2/checkout/orders/{urllib_parse.quote(order_id)}/capture",
        data=b"{}",
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "PayPal-Request-Id": f"{order_id}-{uuid.uuid4().hex}",
        },
    )
    try:
        with urllib_request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib_error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"PayPal order capture failed ({exc.code}): {detail}") from exc


def paypal_verify_webhook(raw_body: bytes, headers: Any) -> dict[str, Any]:
    if not paypal_is_configured():
        raise RuntimeError("PayPal webhook verification is not configured.")
    webhook_event = json.loads(raw_body.decode("utf-8"))
    verification_payload = {
        "auth_algo": headers.get("PAYPAL-AUTH-ALGO") or headers.get("paypal-auth-algo"),
        "cert_url": headers.get("PAYPAL-CERT-URL") or headers.get("paypal-cert-url"),
        "transmission_id": headers.get("PAYPAL-TRANSMISSION-ID") or headers.get("paypal-transmission-id"),
        "transmission_sig": headers.get("PAYPAL-TRANSMISSION-SIG") or headers.get("paypal-transmission-sig"),
        "transmission_time": headers.get("PAYPAL-TRANSMISSION-TIME") or headers.get("paypal-transmission-time"),
        "webhook_id": get_paypal_webhook_id(),
        "webhook_event": webhook_event,
    }
    if not all(verification_payload.get(key) for key in ("auth_algo", "cert_url", "transmission_id", "transmission_sig", "transmission_time", "webhook_id")):
        raise RuntimeError("Missing PayPal signature headers.")
    token = paypal_get_access_token()
    request = urllib_request.Request(
        get_paypal_api_base().rstrip("/") + "/v1/notifications/verify-webhook-signature",
        data=json.dumps(verification_payload).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib_request.urlopen(request, timeout=30) as response:
            result = json.loads(response.read().decode("utf-8"))
    except urllib_error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"PayPal webhook verification failed ({exc.code}): {detail}") from exc
    if str(result.get("verification_status") or "").upper() != "SUCCESS":
        raise RuntimeError("PayPal webhook verification failed.")
    return webhook_event


def paypal_mark_payment_complete(state: dict[str, Any], order_id: str, event: dict[str, Any]) -> tuple[int, dict[str, Any]] | None:
    orders = get_paypal_orders(state)
    order_entry = orders.get(order_id)
    user_id = None
    if order_entry and str(order_entry.get("user_id") or "").isdigit():
        user_id = int(order_entry["user_id"])
    else:
        for raw_user_id, record in state.get("users", {}).items():
            if str(record.get("paypal_order_id") or "") == order_id:
                user_id = int(raw_user_id)
                break
    if user_id is None:
        return None
    record = get_user_record(state, user_id)
    if record.get("payment_status") == "paid":
        save_state(state)
        return None
    record["payment_status"] = "paid"
    record["payment_confirmed_at"] = to_iso(utc_now())
    record["payment_due_amount"] = record.get("payment_due_amount")
    record["paypal_order_id"] = order_id
    if order_entry is not None:
        order_entry["status"] = "completed"
        order_entry["completed_at"] = to_iso(utc_now())
        order_entry.setdefault("delivery_status", "pending")
    save_state(state)
    return user_id, record


def paypal_find_order_state(state: dict[str, Any], order_id: str) -> tuple[int, dict[str, Any]] | None:
    orders = get_paypal_orders(state)
    order_entry = orders.get(order_id)
    if order_entry and str(order_entry.get("user_id") or "").isdigit():
        user_id = int(order_entry["user_id"])
        return user_id, get_user_record(state, user_id)
    for raw_user_id, record in state.get("users", {}).items():
        if str(record.get("paypal_order_id") or "") == order_id:
            return int(raw_user_id), record
    return None


def paypal_notify_payment_complete(state: dict[str, Any], user_id: int, record: dict[str, Any], event: dict[str, Any]) -> None:
    buyer_chat_id = get_buyer_chat_id(record, user_id)
    schedule_bot_message(
        chat_id=buyer_chat_id,
        text=template("payment_confirmed"),
        protect_content=True,
    )
    admin_chat_id = get_relay_group_id() if record.get("test_mode") else state.get("admin_chat_id")
    if admin_chat_id is not None:
        schedule_bot_message(
            chat_id=int(admin_chat_id),
            text=f"PayPal payment confirmed for {format_person_label(record)} ({user_id}).",
        )
    schedule_paid_content_fulfillment(user_id, order_id=str(record.get("paypal_order_id") or "").strip() or None)


def paypal_process_webhook(raw_body: bytes, headers: Any) -> str:
    state = load_state()
    event = paypal_verify_webhook(raw_body, headers)
    event_type = str(event.get("event_type") or "").strip()
    if event_type != "PAYMENT.CAPTURE.COMPLETED":
        return f"Ignored {event_type or 'unknown'}"
    resource = event.get("resource") or {}
    related_ids = (resource.get("supplementary_data") or {}).get("related_ids") or {}
    order_id = str(related_ids.get("order_id") or "").strip()
    if not order_id:
        raise RuntimeError("PayPal webhook did not include an order id.")
    result = paypal_mark_payment_complete(state, order_id, event)
    if result is None:
        return f"Order {order_id} not found"
    user_id, record = result
    paypal_notify_payment_complete(state, user_id, record, event)
    log_event("paypal_payment_confirmed", buyer_id=user_id, order_id=order_id)
    return f"Payment confirmed for {user_id}"


class PaypalWebhookHandler(BaseHTTPRequestHandler):
    server_version = "OliverLittleHelperPayPal/1.0"

    def log_message(self, format: str, *args: Any) -> None:
        LOGGER.info("paypal webhook %s", format % args)

    def _send_text(self, status: HTTPStatus, body: str) -> None:
        data = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_html(self, status: HTTPStatus, body: str) -> None:
        data = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _render_return_page(self, title: str, message: str) -> str:
        return (
            "<html><head>"
            "<meta charset='utf-8'>"
            "<meta name='viewport' content='width=device-width, initial-scale=1'>"
            "<script>try{history.replaceState(null,'',location.pathname);}catch(e){}</script>"
            "</head><body>"
            f"<h1>{title}</h1>"
            f"<p>{message}</p>"
            "</body></html>"
        )

    def do_GET(self) -> None:
        if self.path.startswith("/paypal/return"):
            parsed = urllib_parse.urlsplit(self.path)
            query = urllib_parse.parse_qs(parsed.query)
            order_id = str((query.get("token") or [""])[0]).strip()
            if order_id:
                state = load_state()
                order_state = paypal_find_order_state(state, order_id)
                if order_state is not None:
                    user_id, record = order_state
                    if record.get("payment_status") == "paid":
                        self._send_html(
                            HTTPStatus.OK,
                            self._render_return_page(
                                "Payment confirmed",
                                "Your payment has been confirmed. You can close this page and return to Telegram.",
                            ),
                        )
                        return
                    try:
                        capture_payload = paypal_capture_order(order_id)
                        if str(capture_payload.get("status") or "").upper() == "COMPLETED":
                            fresh_state = load_state()
                            result = paypal_mark_payment_complete(fresh_state, order_id, capture_payload)
                            if result is not None:
                                notify_user_id, notify_record = result
                                paypal_notify_payment_complete(load_state(), notify_user_id, notify_record, capture_payload)
                                self._send_html(
                                    HTTPStatus.OK,
                                    self._render_return_page(
                                        "Payment confirmed",
                                        "Your payment has been confirmed. You can close this page and return to Telegram.",
                                    ),
                                )
                                return
                            confirmed_state = paypal_find_order_state(load_state(), order_id)
                            if confirmed_state is not None and confirmed_state[1].get("payment_status") == "paid":
                                self._send_html(
                                    HTTPStatus.OK,
                                    self._render_return_page(
                                        "Payment confirmed",
                                        "Your payment has been confirmed. You can close this page and return to Telegram.",
                                    ),
                                )
                                return
                    except Exception:
                        LOGGER.exception("PayPal return capture failed for order %s.", order_id)
            self._send_html(
                HTTPStatus.OK,
                self._render_return_page(
                    "Payment processing",
                    "Your payment is being confirmed now. You can close this page and return to Telegram.",
                ),
            )
            return
        if self.path.startswith("/paypal/cancel"):
            self._send_html(
                HTTPStatus.OK,
                self._render_return_page(
                    "Payment cancelled",
                    "Your payment was cancelled. You can return to Telegram.",
                ),
            )
            return
        self._send_text(HTTPStatus.OK, "OK")

    def do_POST(self) -> None:
        if not self.path.startswith("/paypal/webhook"):
            self._send_text(HTTPStatus.NOT_FOUND, "Not found")
            return
        try:
            length = int(self.headers.get("Content-Length") or "0")
        except ValueError:
            self._send_text(HTTPStatus.BAD_REQUEST, "Invalid content length")
            return
        raw_body = self.rfile.read(length)
        try:
            message = paypal_process_webhook(raw_body, self.headers)
        except Exception as exc:
            LOGGER.exception("PayPal webhook processing failed.")
            self._send_text(HTTPStatus.BAD_REQUEST, str(exc))
            return
        self._send_text(HTTPStatus.OK, message)


def start_paypal_webhook_server(loop: asyncio.AbstractEventLoop, bot: Any) -> None:
    global PAYPAL_MAIN_LOOP, PAYPAL_BOT, PAYPAL_WEBHOOK_SERVER, PAYPAL_WEBHOOK_THREAD
    PAYPAL_MAIN_LOOP = loop
    PAYPAL_BOT = bot
    if PAYPAL_WEBHOOK_SERVER is not None:
        return
    PAYPAL_WEBHOOK_SERVER = ThreadingHTTPServer(("0.0.0.0", get_paypal_webhook_port()), PaypalWebhookHandler)
    PAYPAL_WEBHOOK_THREAD = threading.Thread(target=PAYPAL_WEBHOOK_SERVER.serve_forever, name="paypal-webhook", daemon=True)
    PAYPAL_WEBHOOK_THREAD.start()
    log_event("paypal_webhook_server_started", port=get_paypal_webhook_port())


def stop_paypal_webhook_server() -> None:
    global PAYPAL_WEBHOOK_SERVER
    if PAYPAL_WEBHOOK_SERVER is None:
        return
    PAYPAL_WEBHOOK_SERVER.shutdown()
    PAYPAL_WEBHOOK_SERVER.server_close()
    PAYPAL_WEBHOOK_SERVER = None


def paypal_checkout_amount_from_record(record: dict[str, Any]) -> int | None:
    amount = record.get("payment_due_amount")
    if amount is None:
        return None
    try:
        return int(amount)
    except (TypeError, ValueError):
        try:
            return int(float(amount))
        except (TypeError, ValueError):
            return None


async def send_paypal_checkout_message(
    bot: Any,
    state: dict[str, Any],
    user_id: int,
    record: dict[str, Any],
    *,
    amount: int | float,
    currency: str = "USD",
    description: str,
    text: str,
    target_chat_id: int | None = None,
    payment_context: str = "manual",
    payment_item_keys: list[str] | None = None,
) -> None:
    global PAYPAL_CHECKOUT_BLOCKED_REASON
    if PAYPAL_CHECKOUT_BLOCKED_REASON:
        await send_and_pin_payment_message(
            bot,
            user_id,
            record,
            target_chat_id=target_chat_id,
            callback_user_id=user_id,
            payment_context=payment_context,
            payment_item_keys=payment_item_keys,
        )
        return

    record["payment_due_amount"] = int(amount) if float(amount).is_integer() else float(amount)
    record["payment_currency"] = currency.upper()
    record["payment_context"] = clean_text(payment_context, empty="manual").strip().lower() or "manual"
    record["payment_item_keys"] = [clean_text(key, empty="") for key in (payment_item_keys or []) if clean_text(key, empty="")]
    record["payment_fulfilled_at"] = None
    record["payment_fulfilled_order_id"] = None
    try:
        order_id, approval_url = await asyncio.to_thread(
            paypal_create_order,
            state,
            user_id,
            amount=amount,
            currency=currency,
            description=description,
            purpose=description[:64],
        )
    except Exception as exc:
        error_text = str(exc)
        if "PAYEE_ACCOUNT_RESTRICTED" in error_text or "merchant account is restricted" in error_text.lower():
            PAYPAL_CHECKOUT_BLOCKED_REASON = "restricted"
            LOGGER.warning("PayPal checkout blocked by restricted merchant account; falling back to manual payment card for user %s.", user_id)
            await send_and_pin_payment_message(
                bot,
                user_id,
                record,
                target_chat_id=target_chat_id,
                callback_user_id=user_id,
                payment_context=payment_context,
                payment_item_keys=payment_item_keys,
                payment_url=get_payment_url(),
            )
            return
        raise
    record["paypal_order_id"] = order_id
    record["payment_status"] = "pending"
    record["payment_requested_at"] = to_iso(utc_now())
    save_state(state)
    message = await bot.send_message(
        chat_id=target_chat_id if target_chat_id is not None else user_id,
        text=text,
        reply_markup=build_payment_keyboard(user_id, record, payment_url=approval_url),
        protect_content=True,
    )
    record["payment_message_id"] = message.message_id
    try:
        await bot.pin_chat_message(
            chat_id=target_chat_id if target_chat_id is not None else user_id,
            message_id=message.message_id,
            disable_notification=True,
        )
    except Exception:
        LOGGER.exception("Could not pin PayPal checkout message for user %s.", user_id)
    save_state(state)


def build_relay_topic_keyboard(user_id: int, record: dict[str, Any] | None = None) -> InlineKeyboardMarkup:
    callback_ref = ensure_callback_ref(record or {}, user_id)
    rows = [
        [InlineKeyboardButton(item["label"], callback_data=f"q:{item['key']}:{callback_ref}")]
        for item in QUICK_PHRASES
        if item["text"] is not None
    ]
    rows.extend(
        [
            [
                InlineKeyboardButton("\U0001F4B0 Budget reply", callback_data=f"q:price_reply:{callback_ref}"),
                InlineKeyboardButton("\U0001F4B3 Pay with PayPal", callback_data=f"pay:{callback_ref}"),
            ],
            [
                InlineKeyboardButton("\U0001F381 Unlock content", callback_data=f"ul:{callback_ref}"),
                InlineKeyboardButton("\U0001F4CA Status", callback_data=f"st:{callback_ref}"),
            ],
            [
                InlineKeyboardButton("\u26A0 Revoke", callback_data=f"rv:{callback_ref}"),
                InlineKeyboardButton("\U0001F5D1 Remove", callback_data=f"rm:{callback_ref}"),
            ],
        ]
    )
    return InlineKeyboardMarkup(rows)


def get_buyer_chat_id(record: dict[str, Any], fallback_user_id: int) -> int:
    chat_id = record.get("test_mode_chat_id")
    if isinstance(chat_id, int) and chat_id:
        return chat_id
    return fallback_user_id


def user_label(user_id: int, user_data: dict[str, Any]) -> str:
    return format_person_label(user_data)


def is_access_active(record: dict[str, Any], now: datetime | None = None) -> bool:
    if record.get("status") != "approved":
        return False
    expires_at = parse_iso(record.get("expires_at"))
    if expires_at is None:
        return True
    return expires_at > (now or utc_now())


def mark_expired_if_needed(record: dict[str, Any], now: datetime | None = None) -> bool:
    if record.get("status") != "approved":
        return False
    if is_access_active(record, now=now):
        return False
    record["status"] = "expired"
    return True


def grant_access(record: dict[str, Any], now: datetime | None = None) -> None:
    current_time = now or utc_now()
    record["status"] = "approved"
    record["approved_at"] = to_iso(current_time)
    record["expires_at"] = to_iso(current_time + timedelta(days=get_access_duration_days()))


def budget_line(record: dict[str, Any]) -> str:
    return str(record.get("budget_range_label") or "Not set")


def subscription_status_line(record: dict[str, Any]) -> str:
    return verification_summary(record)


def payment_status_line(record: dict[str, Any]) -> str:
    status = str(record.get("payment_status") or "not_requested").strip().lower()
    labels = {
        "not_requested": "Not requested",
        "requested": "Requested",
        "pending": "Awaiting payment",
        "paid": "Paid",
        "waived": "Waived",
    }
    label = labels.get(status, status.replace("_", " ").title())
    if status in {"requested", "pending"} and record.get("payment_requested_at"):
        return f"{label} since {format_datetime_for_user(record.get('payment_requested_at'))}"
    if status == "paid" and record.get("payment_confirmed_at"):
        return f"Paid on {format_datetime_for_user(record.get('payment_confirmed_at'))}"
    return label


def is_closed_record(record: dict[str, Any]) -> bool:
    return record.get("status") in {"not_fit", "banned"}


def priority_label(record: dict[str, Any]) -> str:
    priority = record.get("review_priority") or "normal"
    return priority.replace("_", " ").title()


def classify_low_priority(record: dict[str, Any]) -> bool:
    budget_floor = int(record.get("budget_floor") or 0)
    return budget_floor < 100


def classify_trash(record: dict[str, Any]) -> bool:
    budget_floor = int(record.get("budget_floor") or 0)
    return budget_floor < 50


def get_quick_phrase(key: str) -> dict[str, str] | None:
    for item in QUICK_PHRASES:
        if item["key"] == key:
            return item
    return None


def price_rule_for_record(record: dict[str, Any]) -> dict[str, Any]:
    intent = str(record.get("purchase_intent") or "").strip().lower()
    for rule in PRICE_RULES:
        if any(keyword in intent for keyword in rule["keywords"]):
            return rule
    return DEFAULT_CONTENT_PRICE


def build_budget_reply_message(record: dict[str, Any]) -> str:
    budget = clean_text(record.get("budget_range_label"), empty="that budget")
    intent = clean_text(record.get("purchase_intent"), empty="that request").strip().lower()
    rule = price_rule_for_record(record)
    if rule["minimum"] == DEFAULT_CONTENT_PRICE["minimum"]:
        return (
            f"I saw you put {budget} for {intent}. Sadly that is not gonna cut it :/\n\n"
            f"The lowest I sell for is ${rule['minimum']}, which gets you {rule['label']}."
        )

    return (
        f"I saw you put {budget} for {intent}. Sadly that is not gonna cut it :/\n\n"
        f"I charge ${rule['minimum']} minimum for {rule['label']}.\n"
        f"The lowest I sell for is ${DEFAULT_CONTENT_PRICE['minimum']}, which gets you {DEFAULT_CONTENT_PRICE['label']}."
    )


def get_vault_items(state: dict[str, Any]) -> dict[str, Any]:
    return state.setdefault("vault_items", {})


def get_ppv_items(state: dict[str, Any]) -> dict[str, Any]:
    return state.setdefault("ppv_items", {})


def vault_item_sort_key(item: dict[str, Any]) -> str:
    return str(item.get("created_at") or "")


def build_vault_item_label(item_key: str, item: dict[str, Any]) -> str:
    title = clean_text(item.get("title"), empty=item_key)
    price = item.get("price")
    if price is not None:
        return f"{title} (${price})"
    return title


def build_vault_item_picker_keyboard(state: dict[str, Any], user_id: int, record: dict[str, Any] | None = None) -> InlineKeyboardMarkup:
    record = record or get_user_record(state, user_id)
    callback_ref = ensure_callback_ref(record, user_id)
    items = sorted(get_vault_items(state).items(), key=lambda pair: vault_item_sort_key(pair[1]))
    rows = [
        [InlineKeyboardButton(build_vault_item_label(item_key, item), callback_data=f"vk:{item_key}:{callback_ref}")]
        for item_key, item in items[:20]
    ]
    if not rows:
        rows = [[InlineKeyboardButton("No vault items", callback_data=f"noop:{callback_ref}")]]
    return InlineKeyboardMarkup(rows)


def ppv_item_sort_key(item: dict[str, Any]) -> str:
    return str(item.get("created_at") or "")


def normalize_ppv_key(value: str) -> str:
    return normalize_vault_key(value)


def build_ppv_item_label(item_key: str, item: dict[str, Any]) -> str:
    title = clean_text(item.get("title"), empty=item_key)
    price = item.get("price")
    if price is not None:
        return f"{title} (${price})"
    return title


def build_ppv_item_detail(item_key: str, item: dict[str, Any]) -> str:
    title = build_ppv_item_label(item_key, item)
    sequence_key = clean_text(item.get("sequence_key"), empty=item_key)
    return f"{title} | Line: {sequence_key}"


def build_ppv_picker_keyboard(state: dict[str, Any], user_id: int, record: dict[str, Any] | None = None) -> InlineKeyboardMarkup:
    record = record or get_user_record(state, user_id)
    callback_ref = ensure_callback_ref(record, user_id)
    items = sorted(get_ppv_items(state).items(), key=lambda pair: ppv_item_sort_key(pair[1]))
    rows = [
        [InlineKeyboardButton(build_ppv_item_label(item_key, item), callback_data=f"ppv:pick:{item_key}:{callback_ref}")]
        for item_key, item in items[:20]
    ]
    rows.append(
        [
            InlineKeyboardButton("\U0001F9F3 View cart", callback_data=f"ppv:cart:{callback_ref}"),
            InlineKeyboardButton("\U0001F6D2 Checkout", callback_data=f"ppv:checkout:{callback_ref}"),
        ]
    )
    if not rows[:-1]:
        rows = [[InlineKeyboardButton("No PPVs yet", callback_data=f"noop:{callback_ref}")], rows[-1]]
    return InlineKeyboardMarkup(rows)


def build_budget_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for start in range(0, len(BUDGET_OPTIONS), 2):
        row = [
            InlineKeyboardButton(option["label"], callback_data=f"budget:{option['key']}")
            for option in BUDGET_OPTIONS[start : start + 2]
        ]
        rows.append(row)
    return InlineKeyboardMarkup(rows)


def build_admin_review_keyboard(user_id: int, record: dict[str, Any] | None = None) -> InlineKeyboardMarkup:
    callback_ref = ensure_callback_ref(record or {}, user_id)
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("\U0001F197 Approve", callback_data=f"ar:{callback_ref}"),
                InlineKeyboardButton("\U0001F4E8 Direct", callback_data=f"ad:{callback_ref}"),
            ],
            [
                InlineKeyboardButton("\u274C Reject", callback_data=f"r:{callback_ref}"),
            ],
            [
                InlineKeyboardButton("\u2753 Clarify", callback_data=f"clar:{callback_ref}"),
                InlineKeyboardButton("\U0001F501 Retry username", callback_data=f"retryof:{callback_ref}"),
            ],
            [
                InlineKeyboardButton("\u2728 Promising", callback_data=f"label_promising:{callback_ref}"),
                InlineKeyboardButton("Skip", callback_data=f"label_skip:{callback_ref}"),
                InlineKeyboardButton("\u26A0 Dangerous", callback_data=f"label_dangerous:{callback_ref}"),
            ],
            [
                InlineKeyboardButton("\u2B06 Move up", callback_data=f"p:{callback_ref}"),
                InlineKeyboardButton("\U0001F422 Slow queue", callback_data=f"l:{callback_ref}"),
            ],
            [
                InlineKeyboardButton("\u26D4 Ban", callback_data=f"ban:{callback_ref}"),
                InlineKeyboardButton("\U0001F4CB Details", callback_data=f"st:{callback_ref}"),
            ],
        ]
    )


def build_post_approval_keyboard(user_id: int, record: dict[str, Any] | None = None) -> InlineKeyboardMarkup:
    callback_ref = ensure_callback_ref(record or {}, user_id)
    rows = [
        [
            InlineKeyboardButton("\U0001F4B5 Paid", callback_data=f"paid:{callback_ref}"),
            InlineKeyboardButton("\U0001F514 Remind Pay", callback_data=f"rp:{callback_ref}"),
        ]
    ]
    payment_status = str((record or {}).get("payment_status") or "").strip().lower()
    if record is None or payment_status == "paid":
        rows.append(
            [
                InlineKeyboardButton("\U0001F381 Unlock content", callback_data=f"ul:{callback_ref}"),
                InlineKeyboardButton("\U0001F4CA Status", callback_data=f"st:{callback_ref}"),
            ]
        )
    else:
        rows.append(
            [
                InlineKeyboardButton("\U0001F4CA Status", callback_data=f"st:{callback_ref}"),
                InlineKeyboardButton("\u26A0 Revoke", callback_data=f"rv:{callback_ref}"),
            ]
        )
    rows.append(
        [
            InlineKeyboardButton("\U0001F5D1 Remove", callback_data=f"rm:{callback_ref}"),
            InlineKeyboardButton("\u26D4 Ban", callback_data=f"ban:{callback_ref}"),
        ]
    )
    return InlineKeyboardMarkup(rows)


async def send_ppv_picker(
    bot: Any,
    chat_id: int,
    state: dict[str, Any],
    user_id: int,
    *,
    record: dict[str, Any] | None = None,
    message_thread_id: int | None = None,
) -> None:
    record = record or get_user_record(state, user_id)
    kwargs: dict[str, Any] = {}
    if message_thread_id is not None:
        kwargs["message_thread_id"] = message_thread_id
    await bot.send_message(
        chat_id=chat_id,
        text=(
            f"PPV shop for {format_person_label(record)}\n"
            f"Payment: {payment_status_line(record)}\n"
            f"Cart: {len(get_ppv_cart(record))} item{'s' if len(get_ppv_cart(record)) != 1 else ''}\n"
            "Tap an item to add it to your cart.\n"
            "Buying the same PPV line again can move to the next item in that line."
        ),
        reply_markup=build_ppv_picker_keyboard(state, user_id, record),
        **kwargs,
    )


def get_ppv_cart(record: dict[str, Any]) -> list[str]:
    cart = record.setdefault("ppv_cart", [])
    if not isinstance(cart, list):
        cart = []
        record["ppv_cart"] = cart
    return cart


def get_ppv_delivery_history(record: dict[str, Any]) -> dict[str, int]:
    history = record.setdefault("ppv_delivery_history", {})
    if not isinstance(history, dict):
        history = {}
        record["ppv_delivery_history"] = history
    return history


def resolve_ppv_sequence_item_key(state: dict[str, Any], base_key: str, delivery_count: int) -> str | None:
    items = get_ppv_items(state)
    base_item = items.get(base_key)
    if base_item is None:
        return None
    sequence_key = str(base_item.get("sequence_key") or base_key).strip()
    if not sequence_key:
        return base_key
    sequence_items = [
        (item_key, item)
        for item_key, item in items.items()
        if str(item.get("sequence_key") or item_key).strip() == sequence_key
    ]
    sequence_items.sort(key=lambda pair: ppv_item_sort_key(pair[1]))
    if not sequence_items:
        return base_key
    index = min(delivery_count, len(sequence_items) - 1)
    return sequence_items[index][0]


def build_ppv_menu_text(record: dict[str, Any], state: dict[str, Any]) -> str:
    items = sorted(get_ppv_items(state).items(), key=lambda pair: ppv_item_sort_key(pair[1]))
    cart = get_ppv_cart(record)
    lines = [
        "PPV shop",
        "",
        "Tap any item below to add it to your cart.",
        "If you buy the same PPV line again, the next item in that line can unlock instead.",
        "",
        f"Cart: {len(cart)} item{'s' if len(cart) != 1 else ''}",
    ]
    if cart:
        preview = []
        for item_key in cart[:3]:
            item = get_ppv_items(state).get(item_key)
            if item is None:
                continue
            preview.append(build_ppv_item_label(item_key, item))
        if preview:
            lines.append(f"Cart preview: {', '.join(preview)}")
            lines.append("")
    for item_key, item in items[:10]:
        lines.append(f"- {build_ppv_item_detail(item_key, item)}")
    if not items:
        lines.append("No PPVs are set up yet.")
    return "\n".join(lines)


def build_ppv_cart_keyboard(user_id: int, record: dict[str, Any] | None = None) -> InlineKeyboardMarkup:
    callback_ref = ensure_callback_ref(record or {}, user_id)
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Checkout", callback_data=f"ppv:checkout:{callback_ref}"),
            InlineKeyboardButton("Back", callback_data=f"ppv:menu:{callback_ref}"),
            ]
        ]
    )


def ensure_content_delivery_allowed(record: dict[str, Any]) -> None:
    if record.get("status") != "approved":
        raise PermissionError("Only approved buyers can receive content.")
    if record.get("payment_status") != "paid":
        raise PermissionError("Mark this buyer paid first.")


def build_ppv_checkout_summary(record: dict[str, Any], state: dict[str, Any]) -> str:
    cart = get_ppv_cart(record)
    if not cart:
        return "Your cart is empty."
    lines = [
        "Checkout summary",
        "",
        f"Items: {len(cart)}",
    ]
    total = 0
    for index, item_key in enumerate(cart, start=1):
        item = get_ppv_items(state).get(item_key)
        if item is None:
            continue
        lines.append(f"{index}. {build_ppv_item_label(item_key, item)}")
        if isinstance(item.get("price"), int):
            total += int(item["price"])
    lines.extend(["", f"Total: ${total}"])
    return "\n".join(lines)


async def deliver_ppv_item(
    bot: Any,
    state: dict[str, Any],
    user_id: int,
    item_key: str,
    *,
    record: dict[str, Any] | None = None,
    target_chat_id: int | None = None,
) -> None:
    record = record or get_user_record(state, user_id)
    ensure_content_delivery_allowed(record)
    item = get_ppv_items(state).get(item_key)
    if item is None:
        raise RuntimeError("That PPV item is no longer registered.")
    await bot.copy_message(
        chat_id=target_chat_id if target_chat_id is not None else user_id,
        from_chat_id=int(item["source_chat_id"]),
        message_id=int(item["source_message_id"]),
        protect_content=True,
    )


def build_closed_record_keyboard(user_id: int, record: dict[str, Any] | None = None) -> InlineKeyboardMarkup:
    callback_ref = ensure_callback_ref(record, user_id) if record is not None else str(user_id)
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Status", callback_data=f"st:{callback_ref}"),
                InlineKeyboardButton("Remove", callback_data=f"rm:{callback_ref}"),
            ],
        ]
    )


def build_user_action_keyboard(user_id: int, record: dict[str, Any]) -> InlineKeyboardMarkup:
    status = record.get("status")
    if status in {"pending", "low_priority"}:
        return build_admin_review_keyboard(user_id, record)
    if status == "approved":
        return build_post_approval_keyboard(user_id, record)
    return build_closed_record_keyboard(user_id, record)


def build_admin_home_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("\U0001F4E5 Review Inbox", callback_data="adm:pending:all"),
                InlineKeyboardButton("\u26A1 Hot Leads", callback_data="adm:pending:priority"),
            ],
            [
                InlineKeyboardButton("\U0001F422 Slow queue", callback_data="adm:pending:low"),
                InlineKeyboardButton("\u23F3 Access Watch", callback_data="adm:expiring"),
            ],
            [
                InlineKeyboardButton("\U0001F4DD Full Briefing", callback_data="adm:digest"),
                InlineKeyboardButton("\U0001F504 Sync OFAuth", callback_data="adm:sync"),
            ],
            [
                InlineKeyboardButton("\U0001F6E0 Help unverified", callback_data="adm:notify_unverified"),
            ],
            [
                InlineKeyboardButton("\u267B Refresh", callback_data="adm:home"),
                InlineKeyboardButton("\U0001F9ED Command Menu", callback_data="adm:help"),
            ],
        ]
    )


def format_review_card(user_id: int, record: dict[str, Any], heading: str) -> str:
    lines = [
        heading,
        "",
        f"Buyer: {format_person_label(record)}",
        f"OnlyFans: {clean_text(record.get('of_username'))} ({verification_summary(record)})",
        f"Budget: {budget_line(record)}",
        f"Looking for: {clean_text(record.get('purchase_intent'))}",
    ]
    if record.get("ppv_selected_item_title"):
        ppv_line = clean_text(record.get("ppv_selected_item_title"))
        if record.get("ppv_selected_item_price") is not None:
            ppv_line = f"{ppv_line} (${record.get('ppv_selected_item_price')})"
        lines.append(f"PPV: {ppv_line}")
    if record.get("clarification_response"):
        lines.append(f"Clarified: {clean_text(record.get('clarification_response'))}")
    if record.get("internal_label"):
        lines.append(f"Internal label: {clean_text(record.get('internal_label')).replace('_', ' ').title()}")
    lines.append("")
    lines.append("Use Details for full technical history.")
    return "\n".join(lines)


def format_detailed_status_message(user_id: int, record: dict[str, Any]) -> str:
    status = str(record.get("status") or "unknown").replace("_", " ").title()
    lines = [
        "Buyer details",
        "",
        f"Buyer: {format_person_label(record)}",
        f"Telegram ID: {user_id}",
        f"Telegram: {telegram_handle(record) or 'No username'}",
        f"Status: {status}",
        f"OnlyFans: {clean_text(record.get('of_username'))}",
        f"OFAuth: {verification_summary(record)}",
        f"OnlyFans user id: {clean_text(record.get('onlyfans_user_id'))}",
        f"Subscription expires: {format_datetime_for_user(record.get('subscription_expires_at'))}",
        f"Budget: {budget_line(record)}",
        f"Budget floor: {clean_text(record.get('budget_floor'))}",
        f"Looking for: {clean_text(record.get('purchase_intent'))}",
        f"PPV: {clean_text(record.get('ppv_selected_item_title'))}",
        f"PPV price: {clean_text(record.get('ppv_selected_item_price'))}",
        f"Clarification: {clean_text(record.get('clarification_response'))}",
        f"Queue: {priority_label(record)}",
        f"Internal label: {clean_text(record.get('internal_label'))}",
        f"Contact: {contact_mode_label(record)}",
        f"Payment: {payment_status_line(record)}",
        f"Access: {access_status_line(record)}",
        f"Queued at: {format_datetime_for_user(record.get('queued_at'))}",
        f"Approved at: {format_datetime_for_user(record.get('approved_at'))}",
        f"Relay topic: {clean_text(record.get('relay_topic_name'))}",
        f"Not fit at: {format_datetime_for_user(record.get('not_fit_at'))}",
        f"Banned at: {format_datetime_for_user(record.get('banned_at'))}",
    ]
    return "\n".join(lines)


def format_admin_home(state: dict[str, Any]) -> str:
    counts = {
        "priority": 0,
        "normal": 0,
        "low": 0,
        "awaiting_payment": 0,
        "expiring": 0,
        "expired": 0,
    }
    soon = utc_now() + timedelta(days=7)
    for record in state.get("users", {}).values():
        if is_sandbox_record(record):
            continue
        status = record.get("status")
        priority = record.get("review_priority")
        expires_at = parse_iso(record.get("expires_at"))
        if status == "pending" and priority == "priority":
            counts["priority"] += 1
        elif status == "pending":
            counts["normal"] += 1
        elif status == "low_priority":
            counts["low"] += 1
        if status == "approved" and record.get("payment_status") in {"requested", "pending"}:
            counts["awaiting_payment"] += 1
        if status == "approved" and expires_at and expires_at <= soon:
            counts["expiring"] += 1
        elif status == "expired":
            counts["expired"] += 1

    active_review_count = counts["priority"] + counts["normal"]
    followup_count = counts["awaiting_payment"] + counts["expiring"] + counts["expired"]
    lines = ["Oliver's Little Helper", "Control room", ""]

    if active_review_count == 0 and followup_count == 0 and counts["low"] == 0:
        lines.append("All clear.")
    else:
        lines.append("Needs attention")
        if active_review_count:
            lines.append(f"Review inbox: {active_review_count}")
        if counts["awaiting_payment"]:
            lines.append(f"Payments to follow up: {counts['awaiting_payment']}")
        if counts["expiring"]:
            lines.append(f"Expiring soon: {counts['expiring']}")
        if counts["expired"]:
            lines.append(f"Expired access: {counts['expired']}")
        if counts["low"]:
            lead_word = "lead" if counts["low"] == 1 else "leads"
            lines.append(f"Slow queue: {counts['low']} {lead_word}")

    return "\n".join(lines)


def format_admin_help() -> str:
    return (
        "Admin controls\n\n"
        "Use the dashboard buttons for the common workflow.\n\n"
        "Useful commands:\n"
        "/pending [all|low|normal|priority|expired]\n"
        "/details <user_id>\n"
        "/approve <user_id>\n"
        "/approverelay <user_id>\n"
        "/reject <user_id>\n"
        "/priority <user_id>\n"
        "/lowpriority <user_id>\n"
        "/setof <user_id> <onlyfans_username>\n"
        "/requestpay <user_id> <amount> [currency]\n"
        "/renew <user_id>\n"
        "/senddirect <user_id>\n"
        "/revoke <user_id>\n"
        "/removeuser <user_id>\n"
        "/trash <user_id>\n"
        "/vaultregister\n"
        "/vaultadd <key> [title] (reply to a vault post)\n"
        "/vaultlist\n"
        "/ppvadd <key> <price> [line:<group>] [title] (reply to a media post; same key overwrites)\n"
        "/ppvlist\n"
        "/status <user_id>\n"
        "/expiring\n"
        "/notifyunverified\n"
        "/syncsubs\n"
        "/testmode\n"
        "/testmodefull\n"
        "/testreset\n"
        "/testend\n"
        "/verifyof <onlyfans_username>\n"
        "/ofdiag"
    )


def format_operator_help() -> str:
    return (
        "Command guide\n\n"
        "PPVs:\n"
        "/ppvlist\n"
        "/ppvadd <key> <price> [line:<group>] [title] (reply to media)\n\n"
        "/ppvsend <user_id> <item_key>\n"
        "/ppvrelease <user_id>\n\n"
        "Queue actions:\n"
        "/pending [all|low|normal|priority|expired]\n"
        "/trash <user_id>\n"
        "/revoke <user_id>\n"
        "/approve <user_id>\n"
        "/approverelay <user_id>\n\n"
        "Test mode:\n"
        "/testmode\n"
        "/testmodefull\n"
        "/testreset\n"
        "/testend\n\n"
        "PPV notes:\n"
        "Add the same PPV key again to replace the media, title, or price.\n"
        "Use a sequence key if you want repeat purchases to move to the next item in line.\n\n"
        "PPV buyer-side shop is disabled for now; send and release PPVs from the admin chat.\n\n"
        "PPV add example:\n"
        "/ppvadd dickpic_01 250 line:dickpic Dickpic 01\n\n"
        "OnlyFans:\n"
        "/setof <user_id> <onlyfans_username>\n\n"
        "/requestpay <user_id> <amount> [currency]\n\n"
        "PayPal:\n"
        "The current setup can generate a PayPal checkout link once you set an amount, or fall back to the payment link button. Full automation still needs the webhook endpoint."
    )


def format_pending_line(user_id: int, record: dict[str, Any]) -> str:
    parts = [
        str(user_id),
        display_name(record),
        f"OnlyFans: {clean_text(record.get('of_username'))}",
        budget_line(record),
        verification_badge(record),
    ]
    if record.get("review_priority") != "normal":
        parts.append(priority_label(record))
    return " | ".join(parts)


def get_queue_records(state: dict[str, Any], mode: str) -> list[tuple[int, dict[str, Any]]]:
    records: list[tuple[int, dict[str, Any]]] = []
    for user_id_text, record in state.get("users", {}).items():
        if is_sandbox_record(record):
            continue
        status = record.get("status")
        priority = record.get("review_priority")
        if mode == "all" and status in {"pending", "low_priority"}:
            records.append((int(user_id_text), record))
        elif mode == "low" and status == "low_priority":
            records.append((int(user_id_text), record))
        elif mode == "normal" and status == "pending" and priority == "normal":
            records.append((int(user_id_text), record))
        elif mode == "priority" and status == "pending" and priority == "priority":
            records.append((int(user_id_text), record))
        elif mode == "expired" and status == "expired":
            records.append((int(user_id_text), record))
    records.sort(key=lambda item: item[1].get("queued_at") or item[1].get("approved_at") or "")
    return records


def queue_mode_title(mode: str) -> str:
    titles = {
        "all": "Review inbox",
        "low": "Slow queue",
        "normal": "Standard queue",
        "priority": "Priority queue",
        "expired": "Expired access",
    }
    return titles.get(mode, f"{mode.title()} queue")


def get_pending_items(state: dict[str, Any], mode: str) -> list[str]:
    items = []
    for user_id_text, record in state.get("users", {}).items():
        if is_sandbox_record(record):
            continue
        status = record.get("status")
        priority = record.get("review_priority")
        if mode == "all" and status in {"pending", "low_priority"}:
            items.append(format_pending_line(int(user_id_text), record))
        elif mode == "low" and status == "low_priority":
            items.append(format_pending_line(int(user_id_text), record))
        elif mode == "normal" and status == "pending" and priority == "normal":
            items.append(format_pending_line(int(user_id_text), record))
        elif mode == "priority" and status == "pending" and priority == "priority":
            items.append(format_pending_line(int(user_id_text), record))
        elif mode == "expired" and status == "expired":
            items.append(format_pending_line(int(user_id_text), record))
    return items


def format_pending_message(state: dict[str, Any], mode: str) -> str:
    items = get_pending_items(state, mode)
    if not items:
        return f"No requests found for filter '{mode}'."
    return "\n".join(items[:50])


def get_expiring_items(state: dict[str, Any]) -> list[str]:
    now = utc_now()
    soon = now + timedelta(days=7)
    items = []
    for user_id_text, record in state.get("users", {}).items():
        if is_sandbox_record(record):
            continue
        expires_at = parse_iso(record.get("expires_at"))
        if record.get("status") == "approved" and expires_at and expires_at <= soon:
            items.append(
                f"{int(user_id_text)} | {display_name(record)} | expires {format_date_for_user(record.get('expires_at'))} | {budget_line(record)}"
            )
        elif record.get("status") == "expired":
            items.append(
                f"{int(user_id_text)} | {display_name(record)} | expired"
            )
    return items


def format_expiring_message(state: dict[str, Any]) -> str:
    items = get_expiring_items(state)
    if not items:
        return "No users expiring soon."
    return "\n".join(items[:50])


async def notify_unverified_low_priority_users(bot: Any, state: dict[str, Any]) -> dict[str, int]:
    notified = 0
    failed = 0
    skipped = 0
    for user_id_text, record in state.get("users", {}).items():
        if is_sandbox_record(record):
            continue
        if record.get("status") != "low_priority" or record.get("subscription_status") != "inactive":
            continue

        user_id = int(user_id_text)
        submitted_username = str(record.get("of_username") or "").strip()
        try:
            await bot.send_message(
                chat_id=user_id,
                text=of_username_not_verified_message(submitted_username),
            )
        except Exception:
            LOGGER.exception("Could not notify unverified low-priority user %s.", user_id)
            failed += 1
            continue

        begin_application(record)
        notified += 1

    return {"notified": notified, "failed": failed, "skipped": skipped}


def get_low_priority_records(state: dict[str, Any]) -> list[tuple[int, dict[str, Any]]]:
    items: list[tuple[int, dict[str, Any]]] = []
    for user_id_text, record in state.get("users", {}).items():
        if is_sandbox_record(record):
            continue
        if record.get("status") == "low_priority":
            items.append((int(user_id_text), record))
    items.sort(key=lambda item: item[1].get("queued_at") or "", reverse=False)
    return items


def format_low_priority_digest(state: dict[str, Any]) -> str:
    records = get_low_priority_records(state)
    if not records:
        return "Low-priority queue\n\nNothing waiting."

    lines = [
        "Low-priority queue",
    ]
    for user_id, record in records[:50]:
        lines.append(
            f"{user_id} | {display_name(record)} | {clean_text(record.get('of_username'))} | {budget_line(record)}"
        )
    lines.append(
        "Open /pending low in the admin chat for approval buttons."
    )
    return "\n".join(lines)


async def send_queue_cards(bot: Any, chat_id: int, state: dict[str, Any], mode: str) -> None:
    records = get_queue_records(state, mode)
    title = queue_mode_title(mode)
    if not records:
        await bot.send_message(
            chat_id=chat_id,
            text=f"{title}\n\nNothing waiting.",
            reply_markup=build_admin_home_keyboard(),
        )
        return

    await bot.send_message(
        chat_id=chat_id,
        text=f"{title}\n\n{count_line(len(records), 'request')} found. Tap a card to act on a user.",
        reply_markup=build_admin_home_keyboard(),
    )
    for user_id, record in records[:20]:
        await bot.send_message(
            chat_id=chat_id,
            text=format_review_card(user_id, record, title),
            reply_markup=build_user_action_keyboard(user_id, record),
        )


async def send_expiring_cards(bot: Any, chat_id: int, state: dict[str, Any]) -> None:
    now = utc_now()
    soon = now + timedelta(days=7)
    records: list[tuple[int, dict[str, Any]]] = []
    for user_id_text, record in state.get("users", {}).items():
        if is_sandbox_record(record):
            continue
        user_id = int(user_id_text)
        expires_at = parse_iso(record.get("expires_at"))
        if record.get("status") == "approved" and expires_at and expires_at <= soon:
            records.append((user_id, record))
        elif record.get("status") == "expired":
            records.append((user_id, record))

    if not records:
        await bot.send_message(
            chat_id=chat_id,
            text="No users expiring soon.",
            reply_markup=build_admin_home_keyboard(),
        )
        return

    await bot.send_message(
        chat_id=chat_id,
        text=f"Access watch\n\n{count_line(len(records), 'user')} found.",
        reply_markup=build_admin_home_keyboard(),
    )
    for user_id, record in records[:20]:
        heading = "Expires soon" if record.get("status") == "approved" else "Expired"
        await bot.send_message(
            chat_id=chat_id,
            text=format_review_card(user_id, record, heading),
            reply_markup=build_user_action_keyboard(user_id, record),
        )


def format_admin_digest(state: dict[str, Any]) -> str:
    now = utc_now()
    soon = now + timedelta(days=7)
    sections: list[str] = ["Weekly admin digest"]
    users = [(int(user_id), record) for user_id, record in state.get("users", {}).items()]
    users = [(user_id, record) for user_id, record in users if not is_sandbox_record(record)]

    def add_section(title: str, rows: list[str], *, empty: str = "Nothing waiting.") -> None:
        sections.extend(["", title])
        sections.extend(rows[:20] or [empty])

    priority_rows = [
        format_pending_line(user_id, record)
        for user_id, record in users
        if record.get("status") == "pending" and record.get("review_priority") == "priority"
    ]
    normal_rows = [
        format_pending_line(user_id, record)
        for user_id, record in users
        if record.get("status") == "pending" and record.get("review_priority") != "priority"
    ]
    low_rows = [
        format_pending_line(user_id, record)
        for user_id, record in users
        if record.get("status") == "low_priority"
    ]
    payment_rows = [
        f"{user_id} | {display_name(record)} | {budget_line(record)} | {payment_status_line(record)}"
        for user_id, record in users
        if record.get("status") == "approved" and record.get("payment_status") in {"requested", "pending"}
    ]
    expiring_rows = []
    expired_rows = []
    for user_id, record in users:
        expires_at = parse_iso(record.get("expires_at"))
        if record.get("status") == "approved" and expires_at and expires_at <= soon:
            expiring_rows.append(
                f"{user_id} | {display_name(record)} | expires {format_date_for_user(record.get('expires_at'))}"
            )
        elif record.get("status") == "expired":
            expired_rows.append(f"{user_id} | {display_name(record)} | expired")

    add_section("Priority leads", priority_rows)
    add_section("Normal pending leads", normal_rows)
    add_section("Low-priority queue", low_rows)
    add_section("Awaiting payment", payment_rows)
    add_section("Expiring soon", expiring_rows)
    add_section("Expired", expired_rows)

    banned_count = sum(1 for _, record in users if record.get("status") == "banned")
    sections.extend(["", f"Closed: {banned_count} banned."])
    sections.append("Use /pending, /status <user_id>, or the admin buttons for action.")
    return "\n".join(sections)


def send_telegram_text(chat_id: int, text: str) -> None:
    token = get_required_env("BOT_TOKEN")
    payload = urllib_parse.urlencode(
        {
            "chat_id": str(chat_id),
            "text": text,
        }
    ).encode("utf-8")
    request = urllib_request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urllib_request.urlopen(request, timeout=30) as response:
            json.loads(response.read().decode("utf-8"))
    except urllib_error.URLError as exc:
        raise RuntimeError(f"Could not notify Telegram chat {chat_id}: {exc.reason}") from exc


def ofauth_is_configured() -> bool:
    return bool(get_optional_env("OFAUTH_API_KEY") and get_optional_env("OFAUTH_CONNECTION_ID"))


def ofauth_request_json(
    path: str,
    query: dict[str, Any] | None = None,
    *,
    timeout_seconds: float | None = None,
) -> dict[str, Any]:
    api_key = get_required_env("OFAUTH_API_KEY")
    connection_id = get_required_env("OFAUTH_CONNECTION_ID")
    query_string = ""
    if query:
        query_string = "?" + urllib_parse.urlencode(query)
    url = f"{get_ofauth_base_url()}{path}{query_string}"
    timeout = timeout_seconds if timeout_seconds is not None else get_ofauth_timeout_seconds()
    request = urllib_request.Request(
        url,
        headers={
            "apikey": api_key,
            "x-connection-id": connection_id,
            "Accept": "application/json",
            "User-Agent": get_ofauth_user_agent(),
        },
        method="GET",
    )
    try:
        with urllib_request.urlopen(request, timeout=timeout) as response:
            payload = response.read().decode("utf-8")
    except TimeoutError as exc:
        raise RuntimeError(f"OFAuth request timed out after {timeout:g}s.") from exc
    except socket.timeout as exc:
        raise RuntimeError(f"OFAuth request timed out after {timeout:g}s.") from exc
    except urllib_error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"OFAuth request failed with {exc.code}: {body}") from exc
    except urllib_error.URLError as exc:
        if isinstance(exc.reason, (TimeoutError, socket.timeout)):
            raise RuntimeError(f"OFAuth request timed out after {timeout:g}s.") from exc
        raise RuntimeError(f"OFAuth request failed: {exc.reason}") from exc
    try:
        return json.loads(payload)
    except json.JSONDecodeError as exc:
        raise RuntimeError("OFAuth returned invalid JSON.") from exc


def get_subscriber_page(
    *,
    subscriber_type: str = "active",
    limit: int | None = None,
    offset: int = 0,
) -> dict[str, Any]:
    page_size = limit or get_ofauth_page_size()
    return ofauth_request_json(
        "/v2/access/subscribers",
        {"type": subscriber_type, "limit": page_size, "offset": offset},
        timeout_seconds=get_ofauth_timeout_seconds(),
    )


def get_user_profile(user_id_or_username: str) -> dict[str, Any]:
    normalized = str(user_id_or_username).strip()
    if not normalized:
        raise RuntimeError("Missing OnlyFans user identifier.")
    return ofauth_request_json(
        f"/v2/access/users/{urllib_parse.quote(normalized, safe='')}",
        timeout_seconds=get_ofauth_timeout_seconds(),
    )


def get_users_by_ids(user_ids: list[str | int]) -> list[dict[str, Any]]:
    normalized_ids = [str(item).strip() for item in user_ids if str(item).strip()]
    if not normalized_ids:
        return []
    payload = ofauth_request_json(
        "/v2/access/users/list",
        {"userIds": ",".join(normalized_ids)},
        timeout_seconds=get_ofauth_timeout_seconds(),
    )
    users = payload.get("users") or []
    if not isinstance(users, list):
        raise RuntimeError("OFAuth returned an unexpected users/list payload.")
    return users


def verify_onlyfans_username(claimed_username: str) -> dict[str, Any]:
    normalized = normalize_of_username(claimed_username)
    if not normalized:
        raise RuntimeError("Missing OnlyFans username.")

    try:
        profile = get_user_profile(normalized)
    except RuntimeError as exc:
        message = str(exc)
        if "with 404" in message:
            return {
                "verified": False,
                "username": normalized,
                "id": None,
                "expired_at": None,
                "source": "users/{username}",
                "reason": "username not found",
            }
        raise
    onlyfans_user_id = profile.get("id")
    if onlyfans_user_id is None:
        raise RuntimeError("OFAuth did not return a user id for that username.")

    detailed_users = get_users_by_ids([onlyfans_user_id])
    detailed = detailed_users[0] if detailed_users else profile
    subscribed_on = detailed.get("subscribedOn")
    subscribed_data = detailed.get("subscribedOnData") or {}
    has_active_paid = bool(subscribed_data.get("hasActivePaidSubscriptions"))
    status_text = str(subscribed_data.get("status") or "").strip().lower()
    expired_at = (
        subscribed_data.get("expiredAt")
        or detailed.get("subscribedOnExpireDate")
        or detailed.get("subscribedByExpireDate")
        or detailed.get("expiredAt")
    )
    is_expired_now = bool(detailed.get("subscribedOnExpiredNow") or detailed.get("subscribedIsExpiredNow"))

    verified = False
    if has_active_paid:
        verified = True
    elif status_text in {"active", "current"} and not is_expired_now:
        verified = True
    elif subscribed_on is True and not is_expired_now:
        verified = True

    if verified:
        return {
            "verified": True,
            "username": detailed.get("username") or normalized,
            "id": onlyfans_user_id,
            "expired_at": expired_at,
            "source": "users/list",
            "reason": None,
        }

    return {
        "verified": False,
        "username": detailed.get("username") or normalized,
        "id": onlyfans_user_id,
        "expired_at": expired_at,
        "source": "users/list",
        "reason": (
            f"subscribedOn={subscribed_on}, "
            f"hasActivePaidSubscriptions={has_active_paid}, "
            f"status={status_text or 'unknown'}, "
            f"subscribedOnExpiredNow={is_expired_now}"
        ),
    }


def fingerprint_subscriber_batch(batch: list[dict[str, Any]]) -> str:
    return json.dumps(
        [
            {
                "id": item.get("id"),
                "username": item.get("username"),
                "expiredAt": item.get("expiredAt"),
            }
            for item in batch
        ],
        ensure_ascii=True,
        sort_keys=True,
    )


def run_ofauth_diagnostics() -> dict[str, Any]:
    page_size = get_ofauth_page_size()
    diagnostics: dict[str, Any] = {
        "page_size": page_size,
        "self_username": None,
        "self_subscribers_count": None,
        "active_page_1_count": 0,
        "active_page_2_count": 0,
        "active_page_2_repeats_page_1": False,
        "all_page_1_count": 0,
        "all_page_2_count": 0,
        "all_page_2_repeats_page_1": False,
        "conclusion": "",
    }

    try:
        self_payload = ofauth_request_json("/v2/access/self", timeout_seconds=get_ofauth_timeout_seconds())
    except Exception as exc:
        diagnostics["self_error"] = str(exc)
    else:
        diagnostics["self_username"] = self_payload.get("username")
        diagnostics["self_subscribers_count"] = self_payload.get("subscribersCount")

    for subscriber_type in ("active", "all"):
        page_one = get_subscriber_page(subscriber_type=subscriber_type, limit=page_size, offset=0)
        batch_one = page_one.get("list") or []
        if not isinstance(batch_one, list):
            raise RuntimeError(f"OFAuth returned an unexpected {subscriber_type} subscriber payload.")
        page_two = get_subscriber_page(subscriber_type=subscriber_type, limit=page_size, offset=page_size)
        batch_two = page_two.get("list") or []
        if not isinstance(batch_two, list):
            raise RuntimeError(f"OFAuth returned an unexpected {subscriber_type} subscriber payload.")

        diagnostics[f"{subscriber_type}_page_1_count"] = len(batch_one)
        diagnostics[f"{subscriber_type}_page_2_count"] = len(batch_two)
        diagnostics[f"{subscriber_type}_page_2_repeats_page_1"] = (
            fingerprint_subscriber_batch(batch_one) == fingerprint_subscriber_batch(batch_two)
        )

    self_count = diagnostics.get("self_subscribers_count")
    active_repeat = diagnostics.get("active_page_2_repeats_page_1")
    all_repeat = diagnostics.get("all_page_2_repeats_page_1")
    active_page_1_count = diagnostics.get("active_page_1_count")

    if isinstance(self_count, int) and self_count > page_size and active_repeat:
        diagnostics["conclusion"] = (
            "OFAuth knows the account has more subscribers than fit on one page, "
            "but the subscriber list endpoint repeats page 1 on page 2. "
            "That points to broken pagination or an ignored offset."
        )
    elif active_repeat and all_repeat:
        diagnostics["conclusion"] = (
            "Both active and all subscriber listings repeat page 1 on page 2. "
            "That points to a general pagination issue for this connection."
        )
    elif active_repeat:
        diagnostics["conclusion"] = (
            "The active subscriber listing repeats page 1 on page 2, while the all listing behaves differently. "
            "That points to an OFAuth issue with the active filter."
        )
    elif isinstance(self_count, int) and active_page_1_count == self_count:
        diagnostics["conclusion"] = (
            "OFAuth appears to think the full active subscriber count fits on the first page. "
            "If that count is lower than expected, the connection may be seeing incomplete account data."
        )
    else:
        diagnostics["conclusion"] = "No obvious OFAuth pagination issue was detected."

    return diagnostics


def sync_warnings_indicate_partial_data(warnings: list[str]) -> bool:
    return bool(warnings)


def lookup_active_subscriber_by_username(claimed_username: str) -> tuple[dict[str, Any] | None, list[str]]:
    normalized = normalize_of_username(claimed_username)
    if not normalized:
        return None, []
    subscribers, warnings = fetch_active_subscribers()
    for subscriber in subscribers:
        if normalize_of_username(str(subscriber.get("username") or "")) == normalized:
            return subscriber, warnings
    return None, warnings


def fetch_active_subscribers(limit: int | None = None) -> tuple[list[dict[str, Any]], list[str]]:
    subscribers: list[dict[str, Any]] = []
    warnings: list[str] = []
    seen_keys: set[str] = set()
    page_size = limit or get_ofauth_page_size()
    offset = 0
    page_number = 0
    max_pages = get_ofauth_max_pages()
    timeout = get_ofauth_timeout_seconds()
    previous_page_fingerprint: str | None = None
    while True:
        page_number += 1
        if page_number > max_pages:
            raise RuntimeError(
                f"OFAuth sync stopped after {max_pages} pages. Increase OFAUTH_MAX_PAGES if this is expected."
            )
        LOGGER.info(
            "OFAuth sync fetching subscribers page %s (offset=%s, limit=%s, timeout=%ss).",
            page_number,
            offset,
            page_size,
            timeout,
        )
        payload = get_subscriber_page(subscriber_type="active", limit=page_size, offset=offset)
        batch = payload.get("list") or []
        if not isinstance(batch, list):
            raise RuntimeError("OFAuth returned an unexpected subscriber payload.")
        page_fingerprint = fingerprint_subscriber_batch(batch)
        if previous_page_fingerprint is not None and page_fingerprint == previous_page_fingerprint:
            warning = (
                "OFAuth repeated the same subscriber page twice. "
                "The sync stopped early and used the unique subscribers already collected."
            )
            warnings.append(warning)
            LOGGER.warning(warning)
            break
        previous_page_fingerprint = page_fingerprint
        LOGGER.info(
            "OFAuth sync received page %s with %s subscribers (hasMore=%s).",
            page_number,
            len(batch),
            bool(payload.get("hasMore")),
        )
        added_this_page = 0
        for item in batch:
            unique_key = str(item.get("id") or normalize_of_username(str(item.get("username") or "")))
            if not unique_key or unique_key in seen_keys:
                continue
            seen_keys.add(unique_key)
            subscribers.append(item)
            added_this_page += 1
        if not batch:
            break
        if added_this_page == 0:
            warning = (
                "OFAuth returned a subscriber page with no new unique entries. "
                "The sync stopped early to avoid looping forever."
            )
            warnings.append(warning)
            LOGGER.warning(warning)
            break
        if not payload.get("hasMore"):
            break
        offset += len(batch)
    return subscribers, warnings


def find_active_subscriber_by_username(claimed_username: str) -> dict[str, Any] | None:
    subscriber, _warnings = lookup_active_subscriber_by_username(claimed_username)
    return subscriber


def sync_subscribers(state: dict[str, Any]) -> dict[str, Any]:
    now = utc_now()
    started_at = time.monotonic()
    subscribers, sync_warnings = fetch_active_subscribers()
    partial_sync = sync_warnings_indicate_partial_data(sync_warnings)
    by_username = {
        normalize_of_username(str(item.get("username") or "")): item
        for item in subscribers
        if item.get("username")
    }
    by_id = {str(item.get("id")): item for item in subscribers if item.get("id") is not None}

    summary = {
        "checked_at": to_iso(now),
        "active_subscribers_seen": len(subscribers),
        "renewed": 0,
        "expired": 0,
        "inactive": 0,
        "matched": 0,
        "duration_seconds": 0.0,
        "expired_users": [],
        "warnings": sync_warnings,
        "partial_sync": partial_sync,
        "skipped_inactive_due_to_partial_sync": 0,
    }

    for user_id_text, record in state.get("users", {}).items():
        if is_sandbox_record(record):
            continue
        user_id = int(user_id_text)
        mark_expired_if_needed(record, now=now)
        claimed_username = normalize_of_username(str(record.get("of_username") or ""))
        if not claimed_username:
            continue

        subscriber = None
        onlyfans_user_id = record.get("onlyfans_user_id")
        if onlyfans_user_id is not None:
            subscriber = by_id.get(str(onlyfans_user_id))
        if subscriber is None:
            subscriber = by_username.get(claimed_username)

        record["last_checked_at"] = to_iso(now)

        if subscriber:
            summary["matched"] += 1
            record["subscription_status"] = "active"
            record["onlyfans_user_id"] = subscriber.get("id")
            record["subscription_expires_at"] = subscriber.get("expiredAt")
            if record.get("status") in {"approved", "expired"}:
                previous_expiry = parse_iso(record.get("expires_at"))
                grant_access(record, now=now)
                if previous_expiry is None or previous_expiry <= now + timedelta(days=1):
                    summary["renewed"] += 1
            continue

        if partial_sync:
            summary["skipped_inactive_due_to_partial_sync"] += 1
            continue

        was_approved = record.get("status") == "approved"
        if was_approved:
            record["status"] = "expired"
            summary["expired"] += 1
            summary["expired_users"].append(
                {
                    "user_id": user_id,
                    "label": user_label(user_id, record),
                    "of_username": record.get("of_username"),
                    "budget": budget_line(record),
                }
            )
        record["subscription_status"] = "inactive"
        record["subscription_expires_at"] = None
        summary["inactive"] += 1
        LOGGER.info("Marked user %s inactive after OFAuth sync.", user_id_text)

    summary["duration_seconds"] = round(time.monotonic() - started_at, 2)
    return summary


def format_sync_summary(summary: dict[str, Any]) -> str:
    lines = [
        "OnlyFans sync done",
        count_line(int(summary.get("active_subscribers_seen") or 0), "subscriber") + " found.",
    ]
    matched = int(summary.get("matched") or 0)
    renewed = int(summary.get("renewed") or 0)
    expired = int(summary.get("expired") or 0)
    inactive = int(summary.get("inactive") or 0)
    skipped = int(summary.get("skipped_inactive_due_to_partial_sync") or 0)

    changes: list[str] = []
    if matched:
        changes.append(count_line(matched, "user") + " matched.")
    if renewed:
        changes.append(count_line(renewed, "access") + " renewed.")
    if expired:
        changes.append(count_line(expired, "access") + " expired.")
    if inactive:
        changes.append(count_line(inactive, "user") + " marked inactive.")
    if not changes:
        changes.append("No access changes.")
    lines.extend(changes)
    if skipped:
        lines.append("Inactive removals were skipped because the subscriber list was incomplete.")
    warnings = summary.get("warnings") or []
    if warnings:
        lines.append("OFAuth returned a partial subscriber list.")
    return "\n".join(lines)


def format_expired_access_alert(summary: dict[str, Any]) -> str | None:
    expired_users = summary.get("expired_users") or []
    if not expired_users:
        return None
    lines = [
        "Access expired for:",
    ]
    for item in expired_users[:50]:
        lines.append(
            f"{item['user_id']} | {item['label']} | {clean_text(item['of_username'])}"
        )
    lines.append("Remove them or ask them to resubscribe.")
    return "\n".join(lines)


def format_status_message(user_id: int, record: dict[str, Any]) -> str:
    return format_detailed_status_message(user_id, record)


def resolve_admin_chat_id(state: dict[str, Any], user: Any) -> int | None:
    env_admin_chat_id = os.getenv("ADMIN_CHAT_ID", "").strip()
    if env_admin_chat_id:
        return int(env_admin_chat_id)

    configured_username = normalize_username(os.getenv("ADMIN_USERNAME", ""))
    current_username = normalize_username(user.username or "")
    saved_admin_chat_id = state.get("admin_chat_id")

    if saved_admin_chat_id:
        return int(saved_admin_chat_id)
    if configured_username and configured_username == current_username:
        return user.id
    return None


def get_admin_private_command_state(
    update: Any,
    *,
    test_mode_message: str = "This chat is in buyer test mode. Use /testmodefull or switch back to the admin chat.",
    chat_message: str = "Open the admin chat first, then use this command there.",
) -> tuple[dict[str, Any] | None, str | None]:
    if not getattr(update, "effective_user", None) or not getattr(update, "effective_chat", None) or not getattr(update, "message", None):
        return None, None
    if update.effective_chat.type != "private":
        return None, None
    state = load_state()
    if is_private_buyer_test_context(state, update):
        return None, test_mode_message
    admin_chat_id = resolve_admin_chat_id(state, update.effective_user)
    if admin_chat_id != update.effective_chat.id:
        return None, chat_message
    return state, None


def callback_is_from_admin_surface(state: dict[str, Any], query: Any) -> bool:
    if query.message is None:
        return False
    if query.message.chat.type == "private" and is_test_mode_active(state, query.from_user):
        return False
    relay_group_id = get_relay_group_id()
    admin_chat_id = resolve_admin_chat_id(state, query.from_user)
    if relay_group_id is not None and query.message.chat.id == relay_group_id:
        return admin_chat_id is not None
    return bool(admin_chat_id and query.message.chat.id == admin_chat_id)


def clear_relay_topic(state: dict[str, Any], record: dict[str, Any]) -> int | None:
    topic_id = record.get("relay_topic_id")
    if isinstance(topic_id, int):
        get_relay_topics(state).pop(str(topic_id), None)
        record["relay_topic_id"] = None
    record["relay_topic_name"] = None
    return topic_id if isinstance(topic_id, int) else None


async def close_relay_topic_if_possible(bot: Any, topic_id: int | None) -> None:
    relay_group_id = get_relay_group_id()
    if relay_group_id is None or topic_id is None:
        return
    try:
        await bot.close_forum_topic(chat_id=relay_group_id, message_thread_id=topic_id)
    except Exception:
        LOGGER.exception("Could not close relay topic %s.", topic_id)


async def revoke_user_access(
    bot: Any,
    state: dict[str, Any],
    user_id: int,
    record: dict[str, Any],
    *,
    notify_user: bool = True,
) -> None:
    topic_id = clear_relay_topic(state, record)
    record["status"] = "expired"
    record["expires_at"] = to_iso(utc_now())
    record["contact_mode"] = None
    record["subscription_status"] = "inactive"
    record["subscription_expires_at"] = None
    record["payment_status"] = "not_requested"
    await close_relay_topic_if_possible(bot, topic_id)
    if notify_user:
        try:
            await bot.send_message(
                chat_id=user_id,
                text="Your access has ended. If you want back in later, send /start again.",
            )
        except Exception:
            LOGGER.exception("Could not notify revoked user %s.", user_id)


async def remove_user_from_system(
    bot: Any,
    state: dict[str, Any],
    user_id: int,
    record: dict[str, Any],
) -> None:
    await revoke_user_access(bot, state, user_id, record, notify_user=False)
    state.setdefault("users", {}).pop(str(user_id), None)


def normalize_vault_key(value: str) -> str:
    cleaned = normalize_username(value).replace("-", "_")
    return "".join(ch for ch in cleaned if ch.isalnum() or ch == "_")


def register_vault_item(
    state: dict[str, Any],
    *,
    key: str,
    title: str,
    source_chat_id: int,
    source_message_id: int,
    registered_by: int | None = None,
) -> None:
    vault_items = get_vault_items(state)
    vault_items[key] = {
        "title": title,
        "source_chat_id": source_chat_id,
        "source_message_id": source_message_id,
        "registered_by": registered_by,
        "created_at": to_iso(utc_now()),
    }


def register_ppv_item(
    state: dict[str, Any],
    *,
    key: str,
    title: str,
    price: int,
    sequence_key: str | None = None,
    source_chat_id: int,
    source_message_id: int,
    registered_by: int | None = None,
) -> None:
    ppv_items = get_ppv_items(state)
    ppv_items[key] = {
        "title": title,
        "price": price,
        "sequence_key": sequence_key or key,
        "source_chat_id": source_chat_id,
        "source_message_id": source_message_id,
        "registered_by": registered_by,
        "created_at": to_iso(utc_now()),
    }


def format_vault_items(state: dict[str, Any]) -> str:
    items = sorted(get_vault_items(state).items(), key=lambda pair: vault_item_sort_key(pair[1]))
    if not items:
        return "No vault items registered yet."
    lines = ["Vault items", ""]
    for key, item in items[:50]:
        lines.append(f"{key} | {build_vault_item_label(key, item)}")
    return "\n".join(lines)


def format_ppv_items(state: dict[str, Any]) -> str:
    items = sorted(get_ppv_items(state).items(), key=lambda pair: ppv_item_sort_key(pair[1]))
    if not items:
        return "No PPV items registered yet."
    lines = ["PPV items", ""]
    for key, item in items[:50]:
        lines.append(f"{key} | {build_ppv_item_label(key, item)} | line: {clean_text(item.get('sequence_key'), empty=key)}")
    return "\n".join(lines)


async def send_vault_picker(
    bot: Any,
    chat_id: int,
    state: dict[str, Any],
    user_id: int,
    *,
    message_thread_id: int | None = None,
) -> None:
    record = get_user_record(state, user_id)
    kwargs: dict[str, Any] = {}
    if message_thread_id is not None:
        kwargs["message_thread_id"] = message_thread_id
    await bot.send_message(
        chat_id=chat_id,
        text=(
            f"Unlock content for {format_person_label(record)}\n"
            f"Payment: {payment_status_line(record)}"
        ),
        reply_markup=build_vault_item_picker_keyboard(state, user_id, record),
        **kwargs,
    )


async def deliver_vault_item(
    bot: Any,
    state: dict[str, Any],
    user_id: int,
    item_key: str,
    *,
    record: dict[str, Any] | None = None,
    target_chat_id: int | None = None,
) -> None:
    record = record or get_user_record(state, user_id)
    ensure_content_delivery_allowed(record)
    item = get_vault_items(state).get(item_key)
    if item is None:
        raise RuntimeError("That vault item is no longer registered.")
    await bot.copy_message(
        chat_id=target_chat_id if target_chat_id is not None else user_id,
        from_chat_id=int(item["source_chat_id"]),
        message_id=int(item["source_message_id"]),
        protect_content=True,
    )


def resolve_next_ppv_item_key(state: dict[str, Any], record: dict[str, Any]) -> str | None:
    base_key = clean_text(record.get("ppv_selected_item_key"), empty="")
    if not base_key:
        cart = get_ppv_cart(record)
        if cart:
            base_key = clean_text(cart[0], empty="")
    if not base_key:
        return None

    items = get_ppv_items(state)
    base_item = items.get(base_key)
    if base_item is None:
        return None

    sequence_key = str(base_item.get("sequence_key") or base_key).strip()
    sequence_items = [
        (item_key, item)
        for item_key, item in items.items()
        if str(item.get("sequence_key") or item_key).strip() == sequence_key
    ]
    sequence_items.sort(key=lambda pair: ppv_item_sort_key(pair[1]))
    if not sequence_items:
        return base_key

    history = get_ppv_delivery_history(record)
    delivered_count = int(history.get(sequence_key) or 0)
    index = min(delivered_count, len(sequence_items) - 1)
    history[sequence_key] = delivered_count + 1
    return sequence_items[index][0]


async def deliver_unlock_content(
    bot: Any,
    state: dict[str, Any],
    user_id: int,
    record: dict[str, Any],
    *,
    target_chat_id: int | None = None,
) -> str:
    current_order_id = clean_text(record.get("paypal_order_id"), empty="")
    if get_payment_context(record) == "ppv" and current_order_id and clean_text(record.get("payment_fulfilled_order_id"), empty="") == current_order_id:
        raise RuntimeError("This PPV order has already been delivered.")
    ppv_key = resolve_next_ppv_item_key(state, record)
    if ppv_key is not None:
        await deliver_ppv_item(bot, state, user_id, ppv_key, record=record, target_chat_id=target_chat_id)
        delivered_label = build_ppv_item_label(ppv_key, get_ppv_items(state)[ppv_key])
        unlocks = record.setdefault("content_unlocks", [])
        if isinstance(unlocks, list):
            unlocks.append(ppv_key)
        return f"Delivered {delivered_label}"

    vault_items = sorted(get_vault_items(state).items(), key=lambda pair: vault_item_sort_key(pair[1]))
    if vault_items:
        item_key, item = vault_items[0]
        await deliver_vault_item(bot, state, user_id, item_key, record=record, target_chat_id=target_chat_id)
        unlocks = record.setdefault("content_unlocks", [])
        if isinstance(unlocks, list):
            unlocks.append(item_key)
        return f"Delivered {build_vault_item_label(item_key, item)}"

    raise RuntimeError("No unlockable content is registered yet.")


def begin_application(record: dict[str, Any]) -> None:
    record["status"] = "awaiting_of_username"
    record["of_username"] = None
    record["subscription_status"] = "unknown"
    record["subscription_expires_at"] = None
    record["onlyfans_user_id"] = None
    record["budget_range_key"] = None
    record["budget_range_label"] = None
    record["budget_floor"] = None
    record["purchase_intent"] = None
    record["ppv_selected_item_key"] = None
    record["ppv_selected_item_title"] = None
    record["ppv_selected_item_price"] = None
    record["clarification_requested_at"] = None
    record["clarification_response"] = None
    record["review_priority"] = "normal"
    record["queued_at"] = None


async def ensure_relay_topic(
    bot: Any,
    state: dict[str, Any],
    user_id: int,
    record: dict[str, Any],
) -> tuple[int, str]:
    relay_group_id = get_relay_group_id()
    if relay_group_id is None:
        raise RuntimeError("Relay mode is not configured. Set RELAY_ADMIN_GROUP_ID first.")

    existing_topic_id = record.get("relay_topic_id")
    existing_topic_name = str(record.get("relay_topic_name") or "").strip()
    if isinstance(existing_topic_id, int):
        get_relay_topics(state)[str(existing_topic_id)] = user_id
        return existing_topic_id, existing_topic_name or build_relay_topic_name(record)

    topic_name = build_relay_topic_name(record)
    topic = await bot.create_forum_topic(chat_id=relay_group_id, name=topic_name)
    topic_id = int(topic.message_thread_id)
    record["relay_topic_id"] = topic_id
    record["relay_topic_name"] = topic_name
    get_relay_topics(state)[str(topic_id)] = user_id
    await bot.send_message(
        chat_id=relay_group_id,
        message_thread_id=topic_id,
        text=relay_intro_text(user_id, record),
        reply_markup=build_relay_topic_keyboard(user_id, record),
    )
    return topic_id, topic_name


async def send_and_pin_payment_message(
    bot: Any,
    user_id: int,
    record: dict[str, Any],
    *,
    target_chat_id: int | None = None,
    callback_user_id: int | None = None,
    payment_context: str = "manual",
    payment_item_keys: list[str] | None = None,
    payment_url: str | None = None,
) -> None:
    current_time = utc_now()
    record["payment_status"] = "pending"
    record["payment_requested_at"] = to_iso(current_time)
    record["payment_context"] = clean_text(payment_context, empty="manual").strip().lower() or "manual"
    record["payment_item_keys"] = [clean_text(key, empty="") for key in (payment_item_keys or []) if clean_text(key, empty="")]
    record["payment_fulfilled_at"] = None
    record["payment_fulfilled_order_id"] = None
    callback_target_id = callback_user_id if callback_user_id is not None else user_id
    message = await bot.send_message(
        chat_id=target_chat_id if target_chat_id is not None else user_id,
        text=payment_message(record),
        reply_markup=build_payment_keyboard(callback_target_id, record, payment_url=payment_url),
        protect_content=True,
    )
    record["payment_message_id"] = message.message_id
    try:
        await bot.pin_chat_message(
            chat_id=user_id,
            message_id=message.message_id,
            disable_notification=True,
        )
    except Exception:
        LOGGER.exception("Could not pin payment message for user %s.", user_id)
        log_event("payment_pin_failed", logging.WARNING, buyer_id=user_id)
    log_event("payment_requested", buyer_id=user_id)


async def send_direct_contact(
    bot: Any,
    user_id: int,
    record: dict[str, Any],
    *,
    now: datetime | None = None,
    target_chat_id: int | None = None,
) -> None:
    current_time = now or utc_now()
    private_username = get_required_env("PRIVATE_TELEGRAM_USERNAME")
    set_contact_mode(record, "direct", now=current_time)
    await bot.send_message(chat_id=target_chat_id if target_chat_id is not None else user_id, text=direct_access_message(private_username, record))
    await send_and_pin_payment_message(bot, user_id, record, target_chat_id=target_chat_id)


async def send_relay_contact(
    bot: Any,
    state: dict[str, Any],
    user_id: int,
    record: dict[str, Any],
    *,
    now: datetime | None = None,
    target_chat_id: int | None = None,
) -> tuple[int, str]:
    current_time = now or utc_now()
    topic_id, topic_name = await ensure_relay_topic(bot, state, user_id, record)
    set_contact_mode(record, "relay", now=current_time)
    chat_id = target_chat_id if target_chat_id is not None else user_id
    await bot.send_message(chat_id=chat_id, text=relay_access_message(record), protect_content=True)
    await send_and_pin_payment_message(bot, user_id, record, target_chat_id=chat_id, callback_user_id=user_id)
    return topic_id, topic_name


async def send_testmode_contact(
    bot: Any,
    state: dict[str, Any],
    user_id: int,
    record: dict[str, Any],
    *,
    now: datetime | None = None,
    target_chat_id: int | None = None,
) -> tuple[str, str]:
    try:
        await send_relay_contact(
            bot,
            state,
            user_id,
            record,
            now=now,
            target_chat_id=target_chat_id,
        )
        return "relay", "Test mode started in relay mode."
    except Exception as exc:
        LOGGER.exception("Test buyer relay setup failed, falling back to direct mode.")
        await send_direct_contact(
            bot,
            user_id,
            record,
            now=now,
            target_chat_id=target_chat_id,
        )
        return "direct", f"Test mode started in direct fallback because relay was unavailable: {exc}"


def get_relay_user_id(state: dict[str, Any], topic_id: int | None) -> int | None:
    if topic_id is None:
        return None
    raw = get_relay_topics(state).get(str(topic_id))
    if raw is None:
        return None
    return int(raw)


def is_internal_topic_note(message: Any) -> bool:
    text = str(message.text or message.caption or "").strip()
    return text.startswith("//")


async def relay_buyer_message(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    state: dict[str, Any],
    record: dict[str, Any],
) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return

    relay_group_id = get_relay_group_id()
    relay_topic_id = record.get("relay_topic_id")
    if relay_group_id is None or not isinstance(relay_topic_id, int):
        if testmode_contact_available(state, update.effective_user.id, record):
            await update.message.reply_text(
                "Your test session is running in direct fallback right now. "
                "Messages from here won't relay into a topic until relay setup works again."
            )
        else:
            await update.message.reply_text("Your relay chat is not ready yet. Please wait a moment.")
        return
    logical_user_id = int(record.get("test_mode_buyer_user_id") or update.effective_user.id)
    get_relay_topics(state)[str(relay_topic_id)] = logical_user_id
    save_state(state)

    try:
        await context.bot.copy_message(
            chat_id=relay_group_id,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
            message_thread_id=relay_topic_id,
        )
    except Exception as exc:
        LOGGER.exception("Could not relay buyer message for user %s.", update.effective_user.id)
        log_event(
            "relay_delivery_failed",
            logging.ERROR,
            direction="buyer_to_admin",
            buyer_id=update.effective_user.id,
        )
        save_state(state)
        await update.message.reply_text("I couldn't send that through just now. Please try again in a moment.")
        admin_chat_id = get_relay_group_id() if is_test_mode_active(state, update.effective_user) else resolve_admin_chat_id(state, update.effective_user)
        if admin_chat_id:
            try:
                await context.bot.send_message(
                    chat_id=admin_chat_id,
                    text=f"Relay send failed for {update.effective_user.id}: {exc}",
                )
            except Exception:
                LOGGER.exception("Could not alert admin about buyer relay failure.")


async def relay_admin_group_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message or not update.effective_user:
        return
    relay_group_id = get_relay_group_id()
    if relay_group_id is None or update.effective_chat.id != relay_group_id:
        return
    if update.effective_chat.type != "supergroup":
        return
    if update.effective_user.is_bot:
        return
    if is_internal_topic_note(update.message):
        return

    state = load_state()
    user_id = get_relay_user_id(state, update.message.message_thread_id)
    if user_id is None:
        return

    record = get_user_record(state, user_id)
    mark_expired_if_needed(record)
    if not relay_mode_enabled(record):
        save_state(state)
        await context.bot.send_message(
            chat_id=relay_group_id,
            message_thread_id=update.message.message_thread_id,
            text="Relay is inactive for this buyer. Renew access or send them your direct handle if you want to continue.",
        )
        return

    target_chat_id = int(record.get("test_mode_chat_id") or 0) or get_test_mode_chat_id(state, user_id) or user_id

    try:
        await context.bot.copy_message(
            chat_id=target_chat_id,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
            protect_content=True,
        )
    except Exception as exc:
        LOGGER.exception("Could not relay admin message to user %s.", user_id)
        log_event(
            "relay_delivery_failed",
            logging.ERROR,
            direction="admin_to_buyer",
            buyer_id=user_id,
        )
        await context.bot.send_message(
            chat_id=relay_group_id,
            message_thread_id=record.get("relay_topic_id"),
            text=f"Delivery failed for this buyer: {exc}",
        )


async def ask_budget_question(message_target: Any) -> None:
    await message_target.reply_text(
        "Thanks. To route the request properly, what range are you planning for the first request?",
        reply_markup=build_budget_keyboard(),
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state = load_state()
    user = update.effective_user
    test_mode_active = is_test_mode_active(state, user)
    record = get_active_private_record(state, user) if test_mode_active else get_user_record(state, user.id)
    record["telegram_username"] = user.username
    record["first_name"] = user.first_name
    record["last_name"] = user.last_name
    mark_expired_if_needed(record)

    admin_chat_id = resolve_admin_chat_id(state, user)
    if not test_mode_active and admin_chat_id == update.effective_chat.id:
        first_admin_setup = state.get("admin_chat_id") != admin_chat_id
        state["admin_chat_id"] = admin_chat_id
        save_state(state)
        log_event("admin_dashboard_opened", first_setup=first_admin_setup)
        await update.message.reply_text(
            format_admin_home(state),
            reply_markup=build_admin_home_keyboard(),
        )
        return

    if is_closed_record(record):
        save_state(state)
        return

    if is_access_active(record):
        save_state(state)
        if record.get("contact_mode") == "relay":
            await update.message.reply_text(relay_access_message(record))
        else:
            private_username = get_required_env("PRIVATE_TELEGRAM_USERNAME")
            await update.message.reply_text(direct_access_message(private_username, record))
        return

    if record.get("status") == "pending":
        await update.message.reply_text(template("already_pending"))
        return

    if record.get("status") == "low_priority":
        await update.message.reply_text(
            template("low_priority")
        )
        return

    begin_application(record)
    save_state(state)
    log_event("application_started", buyer_id=user.id)
    await update.message.reply_text(of_username_help_message())


async def complete_application(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    state: dict[str, Any],
    record: dict[str, Any],
) -> None:
    user = update.effective_user
    if user is None or update.message is None:
        return

    test_mode = bool(record.get("test_mode"))
    admin_chat_id = get_relay_group_id() if test_mode else resolve_admin_chat_id(state, user)
    admin_user_id = int(record.get("test_mode_buyer_user_id") or user.id)
    exact_match_note = ""
    verified_subscription = True if test_mode else not ofauth_is_configured()
    if not test_mode and ofauth_is_configured():
        try:
            verification_result = await asyncio.to_thread(
                verify_onlyfans_username,
                str(record.get("of_username") or ""),
            )
        except Exception as exc:
            exact_match_note = f"OFAuth check: error ({exc})"
            verified_subscription = True
            record["subscription_status"] = "unknown"
            log_event("ofauth_error", logging.WARNING, buyer_id=user.id, stage="intake")
        else:
            if verification_result.get("verified"):
                record["subscription_status"] = "active"
                record["onlyfans_user_id"] = verification_result.get("id")
                record["subscription_expires_at"] = verification_result.get("expired_at")
                verified_subscription = True
                log_event("ofauth_verified", buyer_id=user.id, stage="intake")
            else:
                record["subscription_status"] = "inactive"
                record["subscription_expires_at"] = None
                verified_subscription = False
    elif test_mode:
        current_time = utc_now()
        record["subscription_status"] = "active"
        record["subscription_expires_at"] = to_iso(current_time + timedelta(days=get_access_duration_days()))
        record["onlyfans_user_id"] = f"test-{admin_user_id}"

    if not verified_subscription:
        submitted_username = str(record.get("of_username") or "").strip()
        record["status"] = "awaiting_of_username"
        record["of_username"] = None
        record["budget_range_key"] = None
        record["budget_range_label"] = None
        record["budget_floor"] = None
        record["purchase_intent"] = None
        record["review_priority"] = "normal"
        record["queued_at"] = None
        save_state(state)
        log_event("ofauth_unverified", buyer_id=user.id, stage="intake")
        await update.message.reply_text(of_username_not_verified_message(submitted_username))
        return

    record["queued_at"] = to_iso(utc_now())
    if classify_low_priority(record):
        record["status"] = "low_priority"
        record["review_priority"] = "low"
        save_state(state)
        log_event(
            "low_priority_queued",
            buyer_id=admin_user_id,
            budget_key=record.get("budget_range_key"),
            verified=record.get("subscription_status") == "active",
        )
        await update.message.reply_text(low_priority_message())
        return

    if classify_trash(record):
        record["status"] = "trash"
        record["trash_at"] = to_iso(utc_now())
        record["review_priority"] = "trash"
        save_state(state)
        log_event(
            "trash_queued",
            buyer_id=admin_user_id,
            budget_key=record.get("budget_range_key"),
            verified=record.get("subscription_status") == "active",
        )
        await update.message.reply_text("Queued as trash.")
        return

    record["status"] = "pending"
    save_state(state)
    log_event(
        "application_submitted",
        buyer_id=admin_user_id,
        priority=record.get("review_priority"),
        budget_key=record.get("budget_range_key"),
        verified=record.get("subscription_status") == "active",
    )
    await update.message.reply_text(application_confirmation_message(record))

    if not admin_chat_id:
        LOGGER.warning("No admin chat configured yet. Request stored but not delivered.")
        log_event("admin_not_configured", logging.WARNING, buyer_id=admin_user_id)
        return

    admin_text = format_review_card(admin_user_id, record, "New gatekeeper request")
    if exact_match_note:
        admin_text = f"{admin_text}\n{exact_match_note}"
    await context.bot.send_message(
        chat_id=admin_chat_id,
        text=admin_text,
        reply_markup=build_admin_review_keyboard(admin_user_id, record),
    )


async def text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state = load_state()
    user = update.effective_user
    test_mode_active = is_test_mode_active(state, user)
    record = get_active_private_record(state, user) if test_mode_active else get_user_record(state, user.id)
    record["telegram_username"] = user.username
    record["first_name"] = user.first_name
    record["last_name"] = user.last_name
    mark_expired_if_needed(record)

    admin_chat_id = resolve_admin_chat_id(state, user)
    if not test_mode_active and admin_chat_id == update.effective_chat.id:
        await update.message.reply_text(
            format_admin_home(state),
            reply_markup=build_admin_home_keyboard(),
        )
        state["admin_chat_id"] = admin_chat_id
        save_state(state)
        return

    if is_closed_record(record):
        save_state(state)
        return

    if is_access_active(record):
        if record.get("contact_mode") == "relay":
            await relay_buyer_message(update, context, state, record)
            return
        private_username = get_required_env("PRIVATE_TELEGRAM_USERNAME")
        save_state(state)
        await update.message.reply_text(direct_access_message(private_username, record))
        return

    status = record.get("status")
    if status in {"new", "rejected", "expired"}:
        begin_application(record)
        save_state(state)
        log_event("application_started", buyer_id=user.id, previous_status=status)
        await update.message.reply_text(of_username_help_message())
        return

    if status == "awaiting_of_username":
        record["of_username"] = update.message.text.strip()
        record["status"] = "awaiting_budget_range"
        save_state(state)
        log_event("onlyfans_username_received", buyer_id=user.id)
        await ask_budget_question(update.message)
        return

    if status == "awaiting_budget_range":
        await update.message.reply_text("Please choose a budget range using the buttons above.")
        return

    if status == "awaiting_purchase_intent":
        record["purchase_intent"] = update.message.text.strip()
        await complete_application(update, context, state, record)
        return

    if status == "awaiting_clarification":
        record["clarification_response"] = update.message.text.strip()
        record["status"] = "pending"
        record["queued_at"] = to_iso(utc_now())
        save_state(state)
        log_event("clarification_received", buyer_id=user.id)
        await update.message.reply_text(
            "Thanks. I added that to your request and will review it personally."
        )
        notification_chat_id = get_relay_group_id() if test_mode_active else admin_chat_id
        if notification_chat_id:
            await context.bot.send_message(
                chat_id=notification_chat_id,
                text=format_review_card(user.id, record, "Buyer clarified request"),
                reply_markup=build_admin_review_keyboard(user.id, record),
            )
        return

    if status == "pending":
        await update.message.reply_text(template("already_pending"))
        return

    if status == "low_priority":
        await update.message.reply_text(
            template("low_priority")
        )
        return

    await update.message.reply_text("Please send /start to begin.")


async def button_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.from_user:
        return

    state = load_state()
    data = query.data or ""
    action = data.partition(":")[0]
    callback_user_ref = data.rsplit(":", 1)[-1] if ":" in data else data
    callback_user_id = resolve_callback_user_id(state, callback_user_ref)

    if data.startswith("budget:"):
        record = get_active_private_record(state, query.from_user)
        mark_expired_if_needed(record)
        if record.get("status") != "awaiting_budget_range":
            await query.answer("That budget step is no longer active.", show_alert=True)
            return
        option = get_budget_option(data.partition(":")[2])
        if option is None:
            await query.answer("Invalid budget range.", show_alert=True)
            return
        record["budget_range_key"] = option["key"]
        record["budget_range_label"] = option["label"]
        record["budget_floor"] = option["floor"]
        record["review_priority"] = option["priority"]
        record["status"] = "awaiting_purchase_intent"
        save_state(state)
        log_event(
            "budget_selected",
            buyer_id=query.from_user.id,
            budget_key=option["key"],
            priority=option["priority"],
        )
        await query.answer("Budget range saved.")
        if query.message is not None:
            await query.edit_message_text(
                text=(
                    f"Budget range saved: {option['label']}"
                )
            )
            await context.bot.send_message(
                chat_id=query.message.chat.id,
                text="What are you looking to purchase in our first interaction?",
            )
        return

    if data.startswith("test:"):
        if callback_user_id is None:
            await query.answer("Invalid test action.", show_alert=True)
            return
        _, _, rest = data.partition(":")
        test_action, _, _ = rest.partition(":")
        user_id = callback_user_id
        record = get_user_record(state, user_id)
        if not record.get("test_mode"):
            await query.answer("That session is no longer active.", show_alert=True)
            return

        if test_action == "paid":
            if record.get("status") != "approved":
                await query.answer("Complete the test approval first.", show_alert=True)
                return
            record["payment_status"] = "paid"
            record["payment_confirmed_at"] = to_iso(utc_now())
            save_state(state)
            log_event("payment_confirmed", buyer_id=user_id, trigger="test")
            await query.answer("Payment simulated.")
            await fulfill_paid_content(
                context.bot,
                state,
                user_id,
                record,
                order_id=str(record.get("paypal_order_id") or "").strip() or None,
                target_chat_id=get_buyer_chat_id(record, user_id),
            )
            await query.edit_message_text(
                "Payment marked as received.",
                reply_markup=build_post_approval_keyboard(user_id, record),
            )
            return

        if test_action == "exit":
            await query.answer("Use /testend to end test mode.", show_alert=True)
            return

        await query.answer("Unknown test action.", show_alert=True)
        return

    if data.startswith("ppv:"):
        _, _, rest = data.partition(":")
        ppv_action, _, tail = rest.partition(":")
        record = get_active_private_record(state, query.from_user)
        mark_expired_if_needed(record)
        if query.message is None or query.message.chat.type != "private":
            await query.answer("Not allowed.", show_alert=True)
            return
        if not is_access_active(record):
            await query.answer("PPVs are available after approval.", show_alert=True)
            return
        if not record.get("test_mode"):
            await query.answer("PPVs are admin-only for now.", show_alert=True)
            return

        user_id = callback_user_id if callback_user_id is not None else query.from_user.id
        buyer_chat_id = query.message.chat.id

        if ppv_action == "menu":
            await send_ppv_picker(
                context.bot,
                buyer_chat_id,
                state,
                query.from_user.id,
                record=record,
            )
            await query.answer("PPVs opened.")
            return

        if ppv_action == "pick":
            item_key, _, _ = tail.partition(":")
            if not item_key:
                await query.answer("Invalid PPV item.", show_alert=True)
                return
            item = get_ppv_items(state).get(item_key)
            if item is None:
                await query.answer("That PPV item is no longer registered.", show_alert=True)
                return
            cart = get_ppv_cart(record)
            if item_key not in cart:
                cart.append(item_key)
            save_state(state)
            if query.message is not None:
                await query.edit_message_text(
                    build_ppv_menu_text(record, state),
                    reply_markup=build_ppv_picker_keyboard(state, query.from_user.id, record),
                )
            await query.answer(f"Added {build_ppv_item_label(item_key, item)} to cart.")
            await context.bot.send_message(
                chat_id=buyer_chat_id,
                text=f"Added to cart: {build_ppv_item_label(item_key, item)}",
                protect_content=True,
            )
            return

        if ppv_action == "cart":
            if query.message is not None:
                await query.edit_message_text(
                    build_ppv_menu_text(record, state),
                    reply_markup=build_ppv_cart_keyboard(query.from_user.id, record),
                )
            await query.answer("Cart opened.")
            return

        if ppv_action == "checkout":
            cart = get_ppv_cart(record)
            if not cart:
                await query.answer("Your cart is empty.", show_alert=True)
                return
            total = 0
            for item_key in cart:
                item = get_ppv_items(state).get(item_key)
                if item is not None and isinstance(item.get("price"), int):
                    total += int(item["price"])
            currency = str(record.get("payment_currency") or "USD").upper()
            record["ppv_selected_item_key"] = cart[0]
            first_item = get_ppv_items(state).get(cart[0])
            record["ppv_selected_item_title"] = first_item.get("title") if first_item else None
            record["ppv_selected_item_price"] = first_item.get("price") if first_item else None
            try:
                await send_paypal_checkout_message(
                    context.bot,
                    state,
                    user_id,
                    record,
                    amount=total,
                    currency=currency,
                    description="PPV cart checkout",
                    text=(
                        f"Payment ready\n\nAmount due: {format_currency_amount(total, currency)}\n\n"
                        "Tap PayPal below to complete the purchase."
                    ),
                    target_chat_id=buyer_chat_id,
                    payment_context="ppv",
                    payment_item_keys=cart,
                )
                await query.answer("PayPal checkout sent.")
                if query.message.chat.type == "supergroup":
                    await context.bot.send_message(
                        chat_id=query.message.chat.id,
                        message_thread_id=query.message.message_thread_id,
                        text="PayPal checkout sent.",
                    )
                return
            except Exception:
                LOGGER.exception("PayPal checkout creation failed for user %s.", user_id)
            await send_and_pin_payment_message(
                context.bot,
                user_id,
                record,
                target_chat_id=buyer_chat_id,
                callback_user_id=user_id,
                payment_context="ppv",
                payment_item_keys=cart,
            )
            save_state(state)
            await query.answer("Payment link sent.")
            if query.message.chat.type == "supergroup":
                await context.bot.send_message(
                    chat_id=query.message.chat.id,
                    message_thread_id=query.message.message_thread_id,
                    text="Payment link sent.",
                )
            return

        await query.answer("Unknown PPV action.", show_alert=True)
        return

    if data.startswith("adm:"):
        _, _, rest = data.partition(":")
        adm_action, _, adm_arg = rest.partition(":")
        admin_chat_id = query.message.chat.id if query.message is not None else query.from_user.id

        if adm_action == "home":
            if query.message is not None:
                await query.edit_message_text(
                    format_admin_home(state),
                    reply_markup=build_admin_home_keyboard(),
                )
            await query.answer("Control room opened.")
            return

        if adm_action == "help":
            if query.message is not None:
                await query.edit_message_text(
                    format_admin_help(),
                    reply_markup=build_admin_home_keyboard(),
                )
            await query.answer("Command menu opened.")
            return

        if adm_action == "pending":
            mode = adm_arg or "all"
            await send_queue_cards(context.bot, admin_chat_id, state, mode)
            await query.answer("Review inbox opened.")
            return

        if adm_action == "expiring":
            await send_expiring_cards(context.bot, admin_chat_id, state)
            await query.answer("Access watch opened.")
            return

        if adm_action == "digest":
            await context.bot.send_message(
                chat_id=admin_chat_id,
                text=format_admin_digest(state),
                reply_markup=build_admin_home_keyboard(),
            )
            await query.answer("Digest sent.")
            return

        if adm_action == "sync":
            summary = await asyncio.to_thread(sync_subscribers, state)
            save_state(state)
            log_event(
                "subs_synced",
                trigger="button",
                synced=summary.get("matched"),
                renewed=summary.get("renewed"),
                expired=summary.get("expired"),
                inactive=summary.get("inactive"),
            )
            await context.bot.send_message(
                chat_id=admin_chat_id,
                text=format_sync_summary(summary),
                reply_markup=build_admin_home_keyboard(),
            )
            await query.answer("Sync finished.")
            return

        if adm_action == "notify_unverified":
            summary = await notify_unverified_low_priority_users(context.bot, state)
            save_state(state)
            log_event(
                "unverified_users_notified",
                trigger="button",
                notified=summary["notified"],
                failed=summary["failed"],
                skipped=summary["skipped"],
            )
            await context.bot.send_message(
                chat_id=admin_chat_id,
                text=(
                    "Unverified low-priority users notified.\n\n"
                    f"Sent: {summary['notified']}\n"
                    f"Failed: {summary['failed']}"
                ),
                reply_markup=build_admin_home_keyboard(),
            )
            await query.answer("Unverified users notified.")
            return

        await query.answer("Unknown admin action.", show_alert=True)
        return

    if data.startswith("q:"):
        _, _, rest = data.partition(":")
        quick_key, _, _ = rest.partition(":")
        if callback_user_id is None:
            await query.answer("Invalid callback data.", show_alert=True)
            return

        user_id = callback_user_id
        record = get_user_record(state, user_id)
        if not relay_mode_enabled(record) and not testmode_contact_available(state, user_id, record):
            await query.answer("That relay is no longer active.", show_alert=True)
            return

        quick_phrase = get_quick_phrase(quick_key)
        if quick_key == "price_reply":
            reply_text = build_budget_reply_message(record)
        elif quick_phrase is not None and quick_phrase.get("text"):
            reply_text = str(quick_phrase["text"])
        else:
            await query.answer("Unknown quick reply.", show_alert=True)
            return

        target_chat_id = get_buyer_chat_id(record, user_id)
        try:
            await context.bot.send_message(
                chat_id=target_chat_id,
                text=reply_text,
                protect_content=True,
            )
            if query.message is not None:
                confirmation = f"Sent quick reply: {reply_text}"
                if query.message.chat.type == "supergroup" and query.message.message_thread_id is not None:
                    await context.bot.send_message(
                        chat_id=query.message.chat.id,
                        message_thread_id=query.message.message_thread_id,
                        text=confirmation,
                    )
                else:
                    await context.bot.send_message(
                        chat_id=query.message.chat.id,
                        text=confirmation,
                    )
            await query.answer("Quick reply sent.")
        except Exception as exc:
            LOGGER.exception("Quick reply failed for user %s.", user_id)
            await query.answer("Could not send quick reply.", show_alert=True)
            if query.message is not None:
                await context.bot.send_message(
                    chat_id=query.message.chat.id,
                    text=f"Quick reply failed for {user_id}: {exc}",
                )
        return

    if data.startswith("noop:"):
        await query.answer("Nothing to act on.")
        return

    if data.startswith("vk:"):
        _, _, rest = data.partition(":")
        item_key, _, _ = rest.partition(":")
        if callback_user_id is None:
            await query.answer("Invalid callback data.", show_alert=True)
            return

        user_id = callback_user_id
        record = get_user_record(state, user_id)
        if record.get("status") != "approved":
            await query.answer("Only approved buyers can receive content.", show_alert=True)
            return
        if record.get("payment_status") != "paid":
            await query.answer("Mark this buyer paid first.", show_alert=True)
            return

        item = get_vault_items(state).get(item_key)
        if item is None:
            await query.answer("That vault item is no longer registered.", show_alert=True)
            return

        try:
            await deliver_vault_item(
                context.bot,
                state,
                user_id,
                item_key,
                record=record,
                target_chat_id=get_buyer_chat_id(record, user_id),
            )
            save_state(state)
            await query.answer("Vault content sent.")
            if query.message is not None:
                await context.bot.send_message(
                    chat_id=query.message.chat.id,
                    text=f"Delivered {build_vault_item_label(item_key, item)}",
                )
        except Exception as exc:
            LOGGER.exception("Vault delivery failed for user %s.", user_id)
            await query.answer("Could not deliver vault content.", show_alert=True)
            if query.message is not None:
                await context.bot.send_message(
                    chat_id=query.message.chat.id,
                    text=f"Vault delivery failed for {user_id}: {exc}",
                )
        return

    if callback_user_id is None:
        await query.answer("Invalid callback data.", show_alert=True)
        return

    user_id = callback_user_id
    record = get_user_record(state, user_id)

    if action == "pay":
        payment_target_chat_id = get_buyer_chat_id(record, user_id)
        due_amount = paypal_checkout_amount_from_record(record)
        if due_amount is None:
            if record.get("ppv_selected_item_price") is not None:
                try:
                    due_amount = int(float(record.get("ppv_selected_item_price")))
                except (TypeError, ValueError):
                    due_amount = None
        due_currency = str(record.get("payment_currency") or "USD").upper()
        if due_amount is None:
            await query.answer("Set a payment amount first.", show_alert=True)
            return
        try:
            if paypal_is_configured():
                await send_paypal_checkout_message(
                    context.bot,
                    state,
                    user_id,
                    record,
                    amount=due_amount,
                    currency=due_currency,
                    description="Payment request",
                    text=(
                        f"Payment request\n\n"
                        f"Amount due: {format_currency_amount(due_amount, due_currency)}\n\n"
                        "Tap Pay with PayPal to continue."
                    ),
                    target_chat_id=payment_target_chat_id,
                )
                await query.answer("PayPal checkout sent.")
            else:
                await send_and_pin_payment_message(
                    context.bot,
                    user_id,
                    record,
                    target_chat_id=payment_target_chat_id,
                    callback_user_id=user_id,
                    payment_url=get_payment_url(),
                )
                await query.answer("Payment link sent.")
        except Exception as exc:
            LOGGER.exception("Payment button failed for user %s.", user_id)
            await query.answer("Could not open payment.", show_alert=True)
            if query.message is not None:
                await context.bot.send_message(
                    chat_id=query.message.chat.id,
                    text=f"Payment button failed for {user_id}: {exc}",
                )
        return

    if action == "ul":
        if record.get("status") != "approved":
            await query.answer("Only approved buyers can receive content.", show_alert=True)
            return
        if record.get("payment_status") != "paid":
            await query.answer("Mark this buyer paid first.", show_alert=True)
            return
        try:
            delivered_label = await deliver_unlock_content(
                context.bot,
                state,
                user_id,
                record,
                target_chat_id=get_buyer_chat_id(record, user_id),
            )
            save_state(state)
            await query.answer("Content sent.")
            if query.message is not None:
                await context.bot.send_message(
                    chat_id=query.message.chat.id,
                    text=delivered_label,
                )
        except Exception as exc:
            LOGGER.exception("Unlock content failed for user %s.", user_id)
            await query.answer("Could not unlock content.", show_alert=True)
            if query.message is not None:
                await context.bot.send_message(
                    chat_id=query.message.chat.id,
                    text=f"Unlock failed for {user_id}: {exc}",
                )
        return

    if action == "st":
        await query.edit_message_text(
            format_detailed_status_message(user_id, record),
            reply_markup=build_post_approval_keyboard(user_id, record)
            if record.get("status") == "approved"
            else build_admin_review_keyboard(user_id, record),
        )
        await query.answer("Details opened.")
        return

    if action == "rv":
        if record.get("status") not in {"approved", "expired"}:
            await query.answer("That user does not currently have active access.", show_alert=True)
            return
        await revoke_user_access(context.bot, state, user_id, record, notify_user=True)
        save_state(state)
        await query.edit_message_text(
            format_detailed_status_message(user_id, record),
            reply_markup=build_user_action_keyboard(user_id, record),
        )
        await query.answer("Access revoked.")
        return

    if action == "rm":
        if str(user_id) not in state.get("users", {}):
            await query.answer("User is already removed.", show_alert=True)
            return
        await remove_user_from_system(context.bot, state, user_id, record)
        save_state(state)
        await query.edit_message_text(f"Removed user {user_id} from the system.")
        await query.answer("User removed.")
        return

    if action == "clar":
        if record.get("status") not in {"pending", "low_priority"}:
            await query.answer("This request is not waiting for review.", show_alert=True)
            return
        record["status"] = "awaiting_clarification"
        record["clarification_requested_at"] = to_iso(utc_now())
        save_state(state)
        log_event("clarification_requested", buyer_id=user_id)
        await context.bot.send_message(
            chat_id=get_buyer_chat_id(record, user_id),
            text=template("clarification_request"),
        )
        await query.edit_message_text(format_review_card(user_id, record, "Clarification requested"))
        await query.answer("Clarification requested.")
        return

    if action == "retryof":
        record["status"] = "awaiting_of_username"
        record["of_username"] = None
        record["subscription_status"] = "unknown"
        record["subscription_expires_at"] = None
        record["onlyfans_user_id"] = None
        save_state(state)
        log_event("onlyfans_username_retry_requested", buyer_id=user_id)
        await context.bot.send_message(
            chat_id=get_buyer_chat_id(record, user_id),
            text=of_username_not_verified_message(None),
        )
        await query.edit_message_text(format_review_card(user_id, record, "Asked buyer to retry OnlyFans username"))
        await query.answer("Username retry requested.")
        return

    if action in {"label_promising", "label_skip", "label_dangerous"}:
        if action == "label_promising":
            record["internal_label"] = "promising"
        elif action == "label_skip":
            record["internal_label"] = "not_worth_time"
        else:
            record["internal_label"] = "dangerous"
        save_state(state)
        log_event("internal_label_set", buyer_id=user_id, label=record["internal_label"])
        await query.edit_message_text(
            format_review_card(user_id, record, "Internal label updated"),
            reply_markup=build_admin_review_keyboard(user_id, record),
        )
        await query.answer("Internal label saved.")
        return

    if action == "ban":
        current_time = utc_now()
        record["status"] = "banned"
        record["banned_at"] = to_iso(current_time)
        record["ban_reason"] = "Admin button"
        save_state(state)
        log_event("banned", buyer_id=user_id, trigger="button")
        try:
            await context.bot.send_message(
                chat_id=get_buyer_chat_id(record, user_id),
                text=template("banned"),
            )
        except Exception:
            LOGGER.exception("Could not notify banned user %s.", user_id)
        await query.edit_message_text(format_review_card(user_id, record, "Banned"))
        await query.answer("Banned.")
        return

    if action == "paid":
        if record.get("status") != "approved":
            await query.answer("Only approved buyers can be marked paid.", show_alert=True)
            return
        record["payment_status"] = "paid"
        record["payment_confirmed_at"] = to_iso(utc_now())
        save_state(state)
        log_event("payment_confirmed", buyer_id=user_id, trigger="button")
        await context.bot.send_message(
            chat_id=get_buyer_chat_id(record, user_id),
            text=template("payment_confirmed"),
        )
        await fulfill_paid_content(
            context.bot,
            state,
            user_id,
            record,
            order_id=str(record.get("paypal_order_id") or "").strip() or None,
            target_chat_id=get_buyer_chat_id(record, user_id),
        )
        await query.edit_message_text(
            format_review_card(user_id, record, "Payment confirmed"),
            reply_markup=build_post_approval_keyboard(user_id, record),
        )
        await query.answer("Marked paid.")
        return

    if action == "rp":
        if record.get("status") != "approved":
            await query.answer("Only approved buyers can receive payment reminders.", show_alert=True)
            return
        record["payment_status"] = "pending"
        record["payment_reminded_at"] = to_iso(utc_now())
        save_state(state)
        log_event("payment_reminder_sent", buyer_id=user_id, trigger="button")
        await context.bot.send_message(
            chat_id=get_buyer_chat_id(record, user_id),
            text=template("payment_reminder"),
        )
        await query.edit_message_text(
            format_review_card(user_id, record, "Payment reminder sent"),
            reply_markup=build_post_approval_keyboard(user_id, record),
        )
        await query.answer("Reminder sent.")
        return

    if record.get("status") not in {"pending", "low_priority"}:
        await query.answer("This request is no longer reviewable.", show_alert=True)
        return

    if action in {"a", "ad", "ar"}:
        current_time = utc_now()
        grant_access(record, now=current_time)
        try:
            if action == "ar":
                payment_target_chat_id = int(record.get("test_mode_chat_id") or 0) or None
                topic_id, topic_name = await send_relay_contact(
                    context.bot,
                    state,
                    user_id,
                    record,
                    now=current_time,
                    target_chat_id=payment_target_chat_id,
                )
                save_state(state)
                log_event(
                    "approved_relay",
                    buyer_id=user_id,
                    trigger="button",
                    budget_key=record.get("budget_range_key"),
                )
                await query.edit_message_text(
                    format_review_card(user_id, record, f"Approved in relay mode\nTopic: {topic_name}"),
                    reply_markup=build_post_approval_keyboard(user_id, record),
                )
                await query.answer(f"Relay approved in topic {topic_id}.")
            else:
                direct_target_chat_id = int(record.get("test_mode_chat_id") or 0) or None
                await send_direct_contact(
                    context.bot,
                    user_id,
                    record,
                    now=current_time,
                    target_chat_id=direct_target_chat_id,
                )
                save_state(state)
                log_event(
                    "approved_direct",
                    buyer_id=user_id,
                    trigger="button",
                    budget_key=record.get("budget_range_key"),
                )
                await query.edit_message_text(
                    format_review_card(user_id, record, "Approved direct"),
                    reply_markup=build_post_approval_keyboard(user_id, record),
                )
                await query.answer("Approved direct.")
        except Exception as exc:
            LOGGER.exception("Approval flow failed for user %s.", user_id)
            log_event("approval_failed", logging.ERROR, buyer_id=user_id, trigger="button")
            record["status"] = "pending"
            save_state(state)
            await query.answer("Approval failed.", show_alert=True)
            await context.bot.send_message(
                chat_id=query.message.chat.id,
                text=f"Approval failed for {user_id}: {exc}",
            )
        return

    if action == "r":
        record["status"] = "rejected"
        save_state(state)
        log_event("rejected", buyer_id=user_id, trigger="button")
        await context.bot.send_message(
            chat_id=get_buyer_chat_id(record, user_id),
            text=template("rejected"),
        )
        await query.edit_message_text(format_review_card(user_id, record, "Rejected"))
        await query.answer("Rejected.")
        return

    if action == "p":
        record["review_priority"] = "priority"
        if record.get("status") == "low_priority":
            record["status"] = "pending"
        save_state(state)
        log_event("queue_changed", buyer_id=user_id, queue="priority", trigger="button")
        await query.edit_message_text(
            format_review_card(user_id, record, "Marked priority"),
            reply_markup=build_admin_review_keyboard(user_id, record),
        )
        await query.answer("Marked priority.")
        return

    if action == "l":
        record["review_priority"] = "low"
        record["status"] = "low_priority"
        save_state(state)
        log_event("queue_changed", buyer_id=user_id, queue="low", trigger="button")
        await query.edit_message_text(format_review_card(user_id, record, "Moved to low-priority queue"))
        await query.answer("Moved to low-priority queue.")
        return

    if action == "trash":
        record["review_priority"] = "trash"
        record["status"] = "trash"
        record["trash_at"] = to_iso(utc_now())
        save_state(state)
        log_event("queue_changed", buyer_id=user_id, queue="trash", trigger="button")
        await query.edit_message_text(format_review_card(user_id, record, "Moved to trash queue"))
        await query.answer("Moved to trash queue.")
        return

    await query.answer("Unknown action.", show_alert=True)


async def pending(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state, gate_message = get_admin_private_command_state(update)
    if state is None:
        if gate_message:
            await update.message.reply_text(gate_message)
        return

    mode = normalize_username(context.args[0]) if context.args else "all"
    await send_queue_cards(context.bot, update.effective_chat.id, state, mode)


async def expiring(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state, gate_message = get_admin_private_command_state(update)
    if state is None:
        if gate_message:
            await update.message.reply_text(gate_message)
        return

    await send_expiring_cards(context.bot, update.effective_chat.id, state)


async def notify_unverified_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state, gate_message = get_admin_private_command_state(update)
    if state is None:
        if gate_message:
            await update.message.reply_text(gate_message)
        return

    summary = await notify_unverified_low_priority_users(context.bot, state)
    save_state(state)
    log_event(
        "unverified_users_notified",
        trigger="command",
        notified=summary["notified"],
        failed=summary["failed"],
        skipped=summary["skipped"],
    )
    await update.message.reply_text(
        "Unverified low-priority users notified.\n\n"
        f"Sent: {summary['notified']}\n"
        f"Failed: {summary['failed']}",
        reply_markup=build_admin_home_keyboard(),
    )


async def sync_subs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state, gate_message = get_admin_private_command_state(update)
    if state is None:
        if gate_message:
            await update.message.reply_text(gate_message)
        return

    if not ofauth_is_configured():
        await update.message.reply_text(
            "OFAuth is not configured. Set OFAUTH_API_KEY and OFAUTH_CONNECTION_ID first."
        )
        return

    await update.message.reply_text("Syncing OnlyFans...")
    log_event("ofauth_sync_started", trigger="command")
    try:
        summary = await asyncio.to_thread(sync_subscribers, state)
    except Exception as exc:
        LOGGER.exception("OFAuth sync failed.")
        log_event("ofauth_sync_failed", logging.ERROR, trigger="command")
        await update.message.reply_text(f"OnlyFans sync failed: {exc}")
        return

    save_state(state)
    log_event(
        "ofauth_sync_completed",
        trigger="command",
        active_seen=summary.get("active_subscribers_seen"),
        matched=summary.get("matched"),
        renewed=summary.get("renewed"),
        expired=summary.get("expired"),
        inactive=summary.get("inactive"),
        partial=bool(summary.get("warnings")),
    )
    await update.message.reply_text(format_sync_summary(summary))
    expired_alert = format_expired_access_alert(summary)
    if expired_alert:
        await update.message.reply_text(expired_alert)


async def testmode(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state = load_state()
    admin_chat_id = resolve_admin_chat_id(state, update.effective_user)
    if admin_chat_id != update.effective_chat.id:
        await update.message.reply_text("Open the admin private chat first, then use /testmode there.")
        return

    begin_test_mode_session(state, update.effective_user, mode="buyer")
    save_state(state)
    log_event("test_mode_started", buyer_id=update.effective_user.id, mode="buyer")
    record = get_active_private_record(state, update.effective_user)
    _, status_message = await send_testmode_contact(
        context.bot,
        state,
        int(record.get("test_mode_buyer_user_id") or update.effective_user.id),
        record,
        now=utc_now(),
    )
    save_state(state)
    await update.message.reply_text(status_message)


async def testmodefull(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state = load_state()
    admin_chat_id = resolve_admin_chat_id(state, update.effective_user)
    if admin_chat_id != update.effective_chat.id:
        await update.message.reply_text("Open the admin private chat first, then use /testmodefull there.")
        return

    begin_test_mode_session(state, update.effective_user, mode="full")
    save_state(state)
    log_event("test_mode_started", buyer_id=update.effective_user.id, mode="full")
    await start(update, context)


async def testreset(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state = load_state()
    admin_chat_id = resolve_admin_chat_id(state, update.effective_user)
    if admin_chat_id != update.effective_chat.id:
        await update.message.reply_text("Open the admin private chat first, then use /testreset there.")
        return

    mode = get_test_mode_flow(state, update.effective_user)
    begin_test_mode_session(state, update.effective_user, mode=mode)
    save_state(state)
    log_event("test_mode_reset", buyer_id=update.effective_user.id, mode=mode)
    if mode == "buyer":
        record = get_active_private_record(state, update.effective_user)
        _, status_message = await send_testmode_contact(
            context.bot,
            state,
            int(record.get("test_mode_buyer_user_id") or update.effective_user.id),
            record,
            now=utc_now(),
        )
        save_state(state)
        await update.message.reply_text(status_message.replace("started", "reset"))
        return
    await start(update, context)


async def testend(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state = load_state()
    admin_chat_id = resolve_admin_chat_id(state, update.effective_user)
    if admin_chat_id != update.effective_chat.id:
        await update.message.reply_text("Open the admin private chat first, then use /testend there.")
        return

    if not is_test_mode_active(state, update.effective_user):
        await update.message.reply_text("No active test mode session.")
        return

    end_test_mode_session(state, update.effective_user)
    save_state(state)
    log_event("test_mode_ended", buyer_id=update.effective_user.id, trigger="command")
    await update.message.reply_text("Test mode ended.")


async def verifyof(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state, gate_message = get_admin_private_command_state(update)
    if state is None:
        if gate_message:
            await update.message.reply_text(gate_message)
        return

    if not ofauth_is_configured():
        await update.message.reply_text(
            "OFAuth is not configured. Set OFAUTH_API_KEY and OFAUTH_CONNECTION_ID first."
        )
        return

    if not context.args:
        await update.message.reply_text("Usage: /verifyof <onlyfans_username>")
        return

    claimed_username = normalize_of_username(context.args[0])
    show_debug = len(context.args) > 1 and normalize_username(context.args[1]) in {"debug", "verbose"}
    if not claimed_username:
        await update.message.reply_text("Usage: /verifyof <onlyfans_username>")
        return

    await update.message.reply_text(f"Checking {claimed_username}...")
    try:
        verification_result = await asyncio.to_thread(verify_onlyfans_username, claimed_username)
    except Exception as exc:
        LOGGER.exception("OFAuth verify command failed.")
        log_event("ofauth_verify_failed", logging.ERROR, trigger="command")
        await update.message.reply_text(f"OFAuth verification failed: {exc}")
        return

    if verification_result.get("verified"):
        log_event("ofauth_verified", trigger="command")
        lines = [
            "Verified",
            "",
            f"OnlyFans username: {verification_result.get('username') or claimed_username}",
            f"Subscription: active until {format_datetime_for_user(verification_result.get('expired_at'), empty='an unknown date')}",
        ]
        if show_debug:
            lines.extend(
                [
                    "",
                    f"OnlyFans user id: {verification_result.get('id')}",
                    f"Source: {verification_result.get('source')}",
                ]
            )
        await update.message.reply_text("\n".join(lines))
        return

    log_event("ofauth_unverified", trigger="command")
    lines = [
        "Unverified",
        "",
        f"OnlyFans username: {verification_result.get('username') or claimed_username}",
        "No active subscription found.",
    ]
    if show_debug:
        lines.append(f"Source: {verification_result.get('source')}")
        if verification_result.get("reason"):
            lines.append(f"Reason: {verification_result['reason']}")
    await update.message.reply_text("\n".join(lines))


async def setof_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state, gate_message = get_admin_private_command_state(update)
    if state is None:
        if gate_message:
            await update.message.reply_text(gate_message)
        return

    if len(context.args) < 2 or not context.args[0].isdigit():
        await update.message.reply_text("Usage: /setof <user_id> <onlyfans_username>")
        return

    user_id = int(context.args[0])
    claimed_username = normalize_of_username(" ".join(context.args[1:]))
    if not claimed_username:
        await update.message.reply_text("Usage: /setof <user_id> <onlyfans_username>")
        return

    record = get_user_record(state, user_id)
    record["of_username"] = claimed_username
    record["subscription_status"] = "unknown"
    record["subscription_expires_at"] = None
    record["onlyfans_user_id"] = None
    record["last_checked_at"] = None
    save_state(state)
    log_event("onlyfans_username_updated", buyer_id=user_id, trigger="command")
    await update.message.reply_text(
        f"Updated OnlyFans username for {user_id} to {claimed_username}."
    )


async def requestpay_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state = load_state()
    if is_private_buyer_test_context(state, update):
        await update.message.reply_text("Use /testend first to use /requestpay.")
        return
    admin_chat_id = resolve_admin_chat_id(state, update.effective_user)
    if admin_chat_id != update.effective_chat.id:
        await update.message.reply_text("Open the admin chat first, then use /requestpay there.")
        return

    if len(context.args) < 2 or not context.args[0].isdigit():
        await update.message.reply_text("Usage: /requestpay <user_id> <amount> [currency]")
        return

    user_id = int(context.args[0])
    try:
        amount = float(str(context.args[1]).strip())
    except ValueError:
        await update.message.reply_text("Amount must be a number.")
        return
    if amount <= 0:
        await update.message.reply_text("Amount must be above zero.")
        return

    currency = str(context.args[2]).strip().upper() if len(context.args) > 2 else "USD"
    record = get_user_record(state, user_id)
    record["payment_due_amount"] = int(amount) if amount.is_integer() else amount
    record["payment_currency"] = currency or "USD"
    record["payment_status"] = "requested"
    record["payment_requested_at"] = to_iso(utc_now())
    record["payment_confirmed_at"] = None
    record["paypal_order_id"] = None
    save_state(state)
    log_event("payment_requested_amount_set", buyer_id=user_id, trigger="command", amount=record["payment_due_amount"], currency=record["payment_currency"])
    buyer_chat_id = get_buyer_chat_id(record, user_id)
    amount_text = format_currency_amount(record["payment_due_amount"], record["payment_currency"])
    try:
        if paypal_is_configured():
            await send_paypal_checkout_message(
                context.bot,
                state,
                user_id,
                record,
                amount=record["payment_due_amount"],
                currency=record["payment_currency"],
                description="Payment request",
                text=(
                    f"Payment ready\n\nAmount due: {amount_text}\n\n"
                    "Tap PayPal below to complete the purchase."
                ),
                target_chat_id=buyer_chat_id,
            )
        else:
            await send_and_pin_payment_message(
                context.bot,
                user_id,
                record,
                target_chat_id=buyer_chat_id,
                callback_user_id=user_id,
            )
    except Exception as exc:
        LOGGER.exception("Could not send payment request for user %s.", user_id)
        try:
            await send_and_pin_payment_message(
                context.bot,
                user_id,
                record,
                target_chat_id=buyer_chat_id,
                callback_user_id=user_id,
            )
        except Exception:
            await update.message.reply_text(
                f"Set payment request for {user_id} to {amount_text}, but I couldn't send the payment card: {exc}"
            )
            return
        await update.message.reply_text(
            f"Set payment request for {user_id} to {amount_text}. PayPal was blocked, so I sent the manual payment card instead."
        )
        return

    await update.message.reply_text(
        f"Set payment request for {user_id} to {amount_text} and sent the payment card."
    )


async def ppvsend_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state, gate_message = get_admin_private_command_state(update)
    if state is None:
        if gate_message:
            await update.message.reply_text(gate_message)
        return

    if len(context.args) < 2 or not context.args[0].isdigit():
        await update.message.reply_text("Usage: /ppvsend <user_id> <item_key>")
        return

    user_id = int(context.args[0])
    item_key = normalize_ppv_key(context.args[1])
    if not item_key:
        await update.message.reply_text("Usage: /ppvsend <user_id> <item_key>")
        return

    item = get_ppv_items(state).get(item_key)
    if item is None:
        await update.message.reply_text("That PPV item is not registered.")
        return

    record = get_user_record(state, user_id)
    if record.get("status") != "approved":
        await update.message.reply_text("That buyer must be approved first.")
        return

    ppv_request_record_update(record, item_key, item)
    record["payment_status"] = "requested"
    record["payment_requested_at"] = to_iso(utc_now())
    record["payment_confirmed_at"] = None
    record["payment_fulfilled_at"] = None
    record["payment_fulfilled_order_id"] = None
    save_state(state)

    buyer_chat_id = get_buyer_chat_id(record, user_id)
    amount_text = format_currency_amount(record["payment_due_amount"], record["payment_currency"])
    try:
        await send_and_pin_payment_message(
            context.bot,
            user_id,
            record,
            target_chat_id=buyer_chat_id,
            callback_user_id=user_id,
            payment_context="ppv",
            payment_item_keys=[item_key],
        )
    except Exception as exc:
        LOGGER.exception("Could not send PPV request for user %s.", user_id)
        await update.message.reply_text(
            f"Set PPV request for {user_id} to {amount_text}, but I couldn't send the payment card: {exc}"
        )
        return

    await update.message.reply_text(
        f"Set PPV request for {user_id} to {amount_text} and sent the payment card."
    )


async def ppvrelease_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state, gate_message = get_admin_private_command_state(update)
    if state is None:
        if gate_message:
            await update.message.reply_text(gate_message)
        return

    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: /ppvrelease <user_id>")
        return

    user_id = int(context.args[0])
    record = get_user_record(state, user_id)
    if record.get("payment_status") != "paid":
        await update.message.reply_text("That buyer is not marked paid yet.")
        return

    try:
        delivered_labels = await fulfill_paid_content(
            context.bot,
            state,
            user_id,
            record,
            order_id=str(record.get("paypal_order_id") or "").strip() or None,
            target_chat_id=get_buyer_chat_id(record, user_id),
        )
    except Exception as exc:
        LOGGER.exception("PPV release retry failed for user %s.", user_id)
        await update.message.reply_text(f"PPV release retry failed: {exc}")
        return

    if delivered_labels:
        await update.message.reply_text(
            "PPV released.\n\n" + "\n".join(f"- {label}" for label in delivered_labels)
        )
        return

    await update.message.reply_text("Nothing was released. I sent a manual review request instead.")


async def ofdiag(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state, gate_message = get_admin_private_command_state(update)
    if state is None:
        if gate_message:
            await update.message.reply_text(gate_message)
        return

    if not ofauth_is_configured():
        await update.message.reply_text(
            "OFAuth is not configured. Set OFAUTH_API_KEY and OFAUTH_CONNECTION_ID first."
        )
        return

    await update.message.reply_text("Checking OFAuth...")
    try:
        diagnostics = await asyncio.to_thread(run_ofauth_diagnostics)
    except Exception as exc:
        LOGGER.exception("OFAuth diagnostics failed.")
        log_event("ofauth_diagnostics_failed", logging.ERROR, trigger="command")
        await update.message.reply_text(f"OFAuth diagnostics failed: {exc}")
        return

    show_debug = len(context.args) > 0 and normalize_username(context.args[0]) in {"debug", "verbose"}
    if show_debug:
        lines = [
            "OFAuth diagnostics",
            "",
            f"Username: {diagnostics.get('self_username') or 'Unavailable'}",
            f"Subscribers: {diagnostics.get('self_subscribers_count')}",
            f"Page size: {diagnostics.get('page_size')}",
            (
                "Active list: "
                f"page1={diagnostics.get('active_page_1_count')}, "
                f"page2={diagnostics.get('active_page_2_count')}, "
                f"repeats={diagnostics.get('active_page_2_repeats_page_1')}"
            ),
            (
                "All list: "
                f"page1={diagnostics.get('all_page_1_count')}, "
                f"page2={diagnostics.get('all_page_2_count')}, "
                f"repeats={diagnostics.get('all_page_2_repeats_page_1')}"
            ),
        ]
        if diagnostics.get("self_error"):
            lines.append(f"Self endpoint error: {diagnostics['self_error']}")
        lines.extend(["", diagnostics.get("conclusion") or "No conclusion."])
    else:
        healthy = not diagnostics.get("active_page_2_repeats_page_1") and not diagnostics.get(
            "all_page_2_repeats_page_1"
        )
        lines = [
            "OFAuth looks healthy." if healthy else "OFAuth still looks off.",
            f"Account: {diagnostics.get('self_username') or 'Unavailable'}",
            f"Subscribers: {diagnostics.get('self_subscribers_count')}",
        ]
        if diagnostics.get("self_error"):
            lines.append(f"Error: {diagnostics['self_error']}")
    await update.message.reply_text("\n".join(lines))


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state, gate_message = get_admin_private_command_state(update)
    if state is None:
        if gate_message:
            await update.message.reply_text(gate_message)
        return

    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: /status <user_id>")
        return

    user_id = int(context.args[0])
    record = get_user_record(state, user_id)
    await update.message.reply_text(
        format_status_message(user_id, record),
        reply_markup=build_user_action_keyboard(user_id, record),
    )


async def details_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state, gate_message = get_admin_private_command_state(update)
    if state is None:
        if gate_message:
            await update.message.reply_text(gate_message)
        return

    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: /details <user_id>")
        return

    user_id = int(context.args[0])
    record = get_user_record(state, user_id)
    await update.message.reply_text(
        format_detailed_status_message(user_id, record),
        reply_markup=build_user_action_keyboard(user_id, record),
    )


async def approve_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await manual_decision(update, context, approved=True, approval_mode="direct")


async def approverelay_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await manual_decision(update, context, approved=True, approval_mode="relay")


async def reject_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await manual_decision(update, context, approved=False, approval_mode="direct")


async def priority_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await reprioritize(update, context, "priority")


async def lowpriority_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await reprioritize(update, context, "low")


async def trash_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state, gate_message = get_admin_private_command_state(update)
    if state is None:
        if gate_message:
            await update.message.reply_text(gate_message)
        return

    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: /trash <user_id>")
        return

    user_id = int(context.args[0])
    record = get_user_record(state, user_id)
    record["status"] = "trash"
    record["review_priority"] = "trash"
    record["trash_at"] = to_iso(utc_now())
    save_state(state)
    log_event("queue_changed", buyer_id=user_id, queue="trash", trigger="command")
    await update.message.reply_text("Queued as trash.")


async def ppvhelp_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state, gate_message = get_admin_private_command_state(update)
    if state is None:
        if gate_message:
            await update.message.reply_text(gate_message)
        return

    await update.message.reply_text(format_operator_help())


async def reprioritize(update: Update, context: ContextTypes.DEFAULT_TYPE, new_priority: str) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state, gate_message = get_admin_private_command_state(update)
    if state is None:
        if gate_message:
            await update.message.reply_text(gate_message)
        return

    if not context.args or not context.args[0].isdigit():
        command_name = "priority" if new_priority == "priority" else "lowpriority"
        await update.message.reply_text(f"Usage: /{command_name} <user_id>")
        return

    user_id = int(context.args[0])
    record = get_user_record(state, user_id)
    if record.get("status") not in {"pending", "low_priority"}:
        await update.message.reply_text("That request is not in a review queue.")
        return

    record["review_priority"] = new_priority
    if new_priority == "priority":
        record["status"] = "pending"
    else:
        record["status"] = "low_priority"
    save_state(state)
    log_event("queue_changed", buyer_id=user_id, queue=new_priority, trigger="command")
    await update.message.reply_text(f"Updated queue to {new_priority}.")


async def renew_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state, gate_message = get_admin_private_command_state(update)
    if state is None:
        if gate_message:
            await update.message.reply_text(gate_message)
        return

    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: /renew <user_id>")
        return

    user_id = int(context.args[0])
    record = get_user_record(state, user_id)
    grant_access(record)
    save_state(state)
    log_event("access_renewed", buyer_id=user_id, trigger="command")
    await update.message.reply_text(
        f"Renewed. Access now ends {format_date_for_user(record.get('expires_at'))}."
    )


async def senddirect_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state, gate_message = get_admin_private_command_state(update)
    if state is None:
        if gate_message:
            await update.message.reply_text(gate_message)
        return

    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: /senddirect <user_id>")
        return

    user_id = int(context.args[0])
    record = get_user_record(state, user_id)
    if record.get("status") != "approved":
        await update.message.reply_text("That buyer needs to be approved first.")
        return

    await send_direct_contact(context.bot, user_id, record, now=utc_now())
    save_state(state)
    log_event("direct_handle_sent", buyer_id=user_id, trigger="command")
    await update.message.reply_text("Direct handle sent.")


async def vaultregister_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "supergroup":
        return

    state = load_state()
    admin_chat_id = resolve_admin_chat_id(state, update.effective_user)
    if admin_chat_id is None:
        await update.message.reply_text("Not allowed.")
        return

    state["content_vault_chat_id"] = update.effective_chat.id
    save_state(state)
    await update.message.reply_text("Content vault registered for this chat.")


async def vaultadd_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "supergroup":
        return

    state = load_state()
    admin_chat_id = resolve_admin_chat_id(state, update.effective_user)
    if admin_chat_id is None:
        await update.message.reply_text("Not allowed.")
        return

    if update.message.reply_to_message is None:
        await update.message.reply_text("Reply to the vault post with /vaultadd <key> [title]")
        return

    if not context.args:
        await update.message.reply_text("Usage: /vaultadd <key> [title]")
        return

    key = normalize_vault_key(context.args[0])
    if not key:
        await update.message.reply_text("Usage: /vaultadd <key> [title]")
        return

    title = " ".join(context.args[1:]).strip()
    if not title:
        title = clean_text(update.message.reply_to_message.caption or update.message.reply_to_message.text, empty=key)

    register_vault_item(
        state,
        key=key,
        title=title,
        source_chat_id=update.effective_chat.id,
        source_message_id=update.message.reply_to_message.message_id,
        registered_by=update.effective_user.id,
    )
    state["content_vault_chat_id"] = update.effective_chat.id
    save_state(state)
    await update.message.reply_text(f"Saved vault item {key}.")


async def vaultlist_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private" and update.effective_chat.type != "supergroup":
        return

    state = load_state()
    if is_private_buyer_test_context(state, update):
        return
    admin_chat_id = resolve_admin_chat_id(state, update.effective_user)
    if admin_chat_id is None and update.effective_chat.id != state.get("content_vault_chat_id"):
        await update.message.reply_text("Not allowed.")
        return

    await update.message.reply_text(format_vault_items(state))


async def ppvadd_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "supergroup":
        return

    state = load_state()
    if is_private_buyer_test_context(state, update):
        return
    admin_chat_id = resolve_admin_chat_id(state, update.effective_user)
    if admin_chat_id is None:
        await update.message.reply_text("Not allowed.")
        return

    if update.message.reply_to_message is None:
        await update.message.reply_text("Reply to the PPV post with /ppvadd <key> <price> [title]")
        return

    if len(context.args) < 2:
        await update.message.reply_text("Usage: /ppvadd <key> <price> [line:<group>] [title]")
        return

    key = normalize_ppv_key(context.args[0])
    if not key:
        await update.message.reply_text("Usage: /ppvadd <key> <price> [line:<group>] [title]")
        return

    try:
        price = int(str(context.args[1]).strip())
    except ValueError:
        await update.message.reply_text("Price must be a whole number.")
        return

    sequence_key = key
    title_start_index = 2
    if len(context.args) >= 3:
        line_arg = str(context.args[2]).strip()
        line_value = None
        for prefix in ("line:", "sequence:", "seq:"):
            if line_arg.lower().startswith(prefix):
                line_value = line_arg.split(":", 1)[1].strip()
                break
        if line_value:
            normalized_line = normalize_ppv_key(line_value)
            if normalized_line:
                sequence_key = normalized_line
                title_start_index = 3

    title = " ".join(context.args[title_start_index:]).strip()
    if not title:
        title = clean_text(update.message.reply_to_message.caption or update.message.reply_to_message.text, empty=key)

    register_ppv_item(
        state,
        key=key,
        title=title,
        price=price,
        sequence_key=sequence_key,
        source_chat_id=update.effective_chat.id,
        source_message_id=update.message.reply_to_message.message_id,
        registered_by=update.effective_user.id,
    )
    state["content_vault_chat_id"] = update.effective_chat.id
    save_state(state)
    await update.message.reply_text(f"Saved PPV item {key}.")


async def ppvlist_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private" and update.effective_chat.type != "supergroup":
        return

    state = load_state()
    if is_private_buyer_test_context(state, update):
        return
    admin_chat_id = resolve_admin_chat_id(state, update.effective_user)
    if admin_chat_id is None and update.effective_chat.id != state.get("content_vault_chat_id"):
        await update.message.reply_text("Not allowed.")
        return

    await update.message.reply_text(format_ppv_items(state))


async def revoke_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state, gate_message = get_admin_private_command_state(update)
    if state is None:
        if gate_message:
            await update.message.reply_text(gate_message)
        return

    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: /revoke <user_id>")
        return

    user_id = int(context.args[0])
    record = get_user_record(state, user_id)
    if record.get("status") not in {"approved", "expired"}:
        await update.message.reply_text("That user does not currently have active access.")
        return
    await revoke_user_access(context.bot, state, user_id, record)
    save_state(state)
    log_event("access_revoked", buyer_id=user_id, trigger="command")
    await update.message.reply_text(
        format_detailed_status_message(user_id, record),
        reply_markup=build_user_action_keyboard(user_id, record),
    )


async def removeuser_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state, gate_message = get_admin_private_command_state(update)
    if state is None:
        if gate_message:
            await update.message.reply_text(gate_message)
        return

    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: /removeuser <user_id>")
        return

    user_id = int(context.args[0])
    if str(user_id) not in state.get("users", {}):
        await update.message.reply_text("That user is already removed.")
        return

    record = get_user_record(state, user_id)
    await remove_user_from_system(context.bot, state, user_id, record)
    save_state(state)
    log_event("user_removed", buyer_id=user_id, trigger="command")
    await update.message.reply_text(f"Removed user {user_id} from the system.")


async def manual_decision(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    approved: bool,
    approval_mode: str,
) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state, gate_message = get_admin_private_command_state(update)
    if state is None:
        if gate_message:
            await update.message.reply_text(gate_message)
        return

    if not context.args or not context.args[0].isdigit():
        if approved and approval_mode == "relay":
            command_name = "approverelay"
        else:
            command_name = "approve" if approved else "reject"
        await update.message.reply_text(f"Usage: /{command_name} <user_id>")
        return

    user_id = int(context.args[0])
    record = get_user_record(state, user_id)
    if record.get("status") not in {"pending", "low_priority"}:
        await update.message.reply_text("That request is not reviewable.")
        return

    if approved:
        current_time = utc_now()
        grant_access(record, now=current_time)
        try:
            target_chat_id = int(record.get("test_mode_chat_id") or 0) or None
            if approval_mode == "relay":
                await send_relay_contact(
                    context.bot,
                    state,
                    user_id,
                    record,
                    now=current_time,
                    target_chat_id=target_chat_id,
                )
                save_state(state)
                log_event("approved_relay", buyer_id=user_id, trigger="command")
                await update.message.reply_text("Approved in relay mode.")
            else:
                await send_direct_contact(
                    context.bot,
                    user_id,
                    record,
                    now=current_time,
                    target_chat_id=target_chat_id,
                )
                save_state(state)
                log_event("approved_direct", buyer_id=user_id, trigger="command")
                await update.message.reply_text("Approved and sent.")
        except Exception as exc:
            LOGGER.exception("Manual approval failed for user %s.", user_id)
            log_event("approval_failed", logging.ERROR, buyer_id=user_id, trigger="command")
            record["status"] = "pending"
            save_state(state)
            await update.message.reply_text(f"Approval failed: {exc}")
        return

    record["status"] = "rejected"
    save_state(state)
    log_event("rejected", buyer_id=user_id, trigger="command")
    await context.bot.send_message(
        chat_id=user_id,
        text=template("rejected"),
    )
    await update.message.reply_text("Rejected.")


async def non_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state = load_state()
    user = update.effective_user
    record = get_active_private_record(state, user) if is_test_mode_active(state, user) else get_user_record(state, user.id)
    record["telegram_username"] = user.username
    record["first_name"] = user.first_name
    record["last_name"] = user.last_name
    mark_expired_if_needed(record)
    if is_closed_record(record):
        save_state(state)
        return
    if relay_mode_enabled(record):
        await relay_buyer_message(update, context, state, record)
        return
    if record.get("status") == "awaiting_of_username":
        await update.message.reply_text(of_username_help_message())
    elif record.get("status") == "awaiting_budget_range":
        await update.message.reply_text("Please choose a budget range using the buttons above.")
    elif record.get("status") == "awaiting_purchase_intent":
        await update.message.reply_text("Please tell me what you are looking to purchase in text.")
    elif record.get("status") == "awaiting_clarification":
        await update.message.reply_text(template("clarification_request"))


def main() -> None:
    configure_logging()
    load_dotenv_file()
    token = get_required_env("BOT_TOKEN")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    app = Application.builder().token(token).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("pending", pending))
    app.add_handler(CommandHandler("approve", approve_manual))
    app.add_handler(CommandHandler("approverelay", approverelay_manual))
    app.add_handler(CommandHandler("reject", reject_manual))
    app.add_handler(CommandHandler("priority", priority_manual))
    app.add_handler(CommandHandler("lowpriority", lowpriority_manual))
    app.add_handler(CommandHandler("trash", trash_manual))
    app.add_handler(CommandHandler("renew", renew_manual))
    app.add_handler(CommandHandler("senddirect", senddirect_manual))
    app.add_handler(CommandHandler("requestpay", requestpay_manual))
    app.add_handler(CommandHandler("ppvsend", ppvsend_manual))
    app.add_handler(CommandHandler("ppvrelease", ppvrelease_manual))
    app.add_handler(CommandHandler("revoke", revoke_manual))
    app.add_handler(CommandHandler("removeuser", removeuser_manual))
    app.add_handler(CommandHandler("vaultregister", vaultregister_manual))
    app.add_handler(CommandHandler("vaultadd", vaultadd_manual))
    app.add_handler(CommandHandler("vaultlist", vaultlist_manual))
    app.add_handler(CommandHandler("ppvadd", ppvadd_manual))
    app.add_handler(CommandHandler("ppvlist", ppvlist_manual))
    app.add_handler(CommandHandler("ppvhelp", ppvhelp_manual))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("details", details_command))
    app.add_handler(CommandHandler("expiring", expiring))
    app.add_handler(CommandHandler("notifyunverified", notify_unverified_manual))
    app.add_handler(CommandHandler("syncsubs", sync_subs))
    app.add_handler(CommandHandler("testmode", testmode))
    app.add_handler(CommandHandler("testmodefull", testmodefull))
    app.add_handler(CommandHandler("testreset", testreset))
    app.add_handler(CommandHandler("testend", testend))
    app.add_handler(CommandHandler("verifyof", verifyof))
    app.add_handler(CommandHandler("setof", setof_manual))
    app.add_handler(CommandHandler("ofdiag", ofdiag))
    app.add_handler(CallbackQueryHandler(button_click))
    app.add_handler(
        MessageHandler(filters.ChatType.SUPERGROUP & ~filters.COMMAND, relay_admin_group_message),
        group=-1,
    )
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_message))
    app.add_handler(MessageHandler(~filters.TEXT & ~filters.COMMAND, non_text_message))

    log_event("bot_started")
    start_paypal_webhook_server(loop, app.bot)
    try:
        app.run_polling(allowed_updates=Update.ALL_TYPES)
    finally:
        stop_paypal_webhook_server()


if __name__ == "__main__":
    main()
