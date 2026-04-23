import asyncio
import json
import logging
import os
import socket
import time
from datetime import datetime, timedelta, timezone
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


logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    level=logging.INFO,
)
LOGGER = logging.getLogger("gatekeeper_bot")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

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


TEMPLATES = {
    "already_pending": "Your request is already pending review. Please wait for a decision.",
    "low_priority": (
        "Your request is in a lower-priority review queue. I check that queue weekly."
    ),
    "not_fit": (
        "Thanks for reaching out. I do not think this is the right fit, "
        "so I will not be moving forward."
    ),
    "banned": "This request is closed. Do not contact this bot again.",
    "rejected": "Sorry, this request was not approved.",
    "payment_reminder": (
        "Quick reminder: please use the pinned payment link when you are ready. "
        "Payment keeps your purchase information easy to find."
    ),
    "payment_confirmed": "Payment marked as received. Thank you.",
}

def template(name: str) -> str:
    return TEMPLATES[name]


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
        return "OK"
    if status == "inactive":
        return "NO"
    return "?"


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
        return {"admin_chat_id": None, "users": {}, "relay_topics": {}}
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        LOGGER.warning("State file was invalid JSON, starting fresh.")
        return {"admin_chat_id": None, "users": {}, "relay_topics": {}}
    state.setdefault("admin_chat_id", None)
    state.setdefault("users", {})
    state.setdefault("relay_topics", {})
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
        "direct_shared_at": None,
        "identity_proof_requested_at": None,
        "identity_proof_sent_at": None,
        "payment_message_id": None,
        "payment_status": "not_requested",
        "payment_requested_at": None,
        "payment_confirmed_at": None,
        "payment_reminded_at": None,
        "not_fit_at": None,
        "banned_at": None,
        "ban_reason": None,
    }


def get_user_record(state: dict[str, Any], user_id: int) -> dict[str, Any]:
    users = state.setdefault("users", {})
    state.setdefault("relay_topics", {})
    record = users.setdefault(str(user_id), default_user_record())
    for key, value in default_user_record().items():
        record.setdefault(key, value)
    return record


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
        f"OF: {clean_text(record.get('of_username'))}",
        verification_summary(record),
        f"Budget: {budget_line(record)}",
        f"Wants: {clean_text(record.get('purchase_intent'))}",
        "",
        "Reply in this topic to message this buyer.",
        "Messages starting with // stay in this topic only.",
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


def payment_message() -> str:
    return (
        "Payment link\n"
        f"{get_payment_url()}\n\n"
        "Use this for purchases so payment info stays easy to find."
    )


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


def build_budget_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for start in range(0, len(BUDGET_OPTIONS), 2):
        row = [
            InlineKeyboardButton(option["label"], callback_data=f"budget:{option['key']}")
            for option in BUDGET_OPTIONS[start : start + 2]
        ]
        rows.append(row)
    return InlineKeyboardMarkup(rows)


def build_admin_review_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Approve Relay", callback_data=f"ar:{user_id}"),
                InlineKeyboardButton("Approve Direct", callback_data=f"ad:{user_id}"),
            ],
            [
                InlineKeyboardButton("Not a Fit", callback_data=f"nf:{user_id}"),
                InlineKeyboardButton("Reject", callback_data=f"r:{user_id}"),
            ],
            [
                InlineKeyboardButton("Priority", callback_data=f"p:{user_id}"),
                InlineKeyboardButton("Low Priority", callback_data=f"l:{user_id}"),
            ],
            [
                InlineKeyboardButton("Ban", callback_data=f"ban:{user_id}"),
                InlineKeyboardButton("Status", callback_data=f"st:{user_id}"),
            ],
        ]
    )


def build_post_approval_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Paid", callback_data=f"paid:{user_id}"),
                InlineKeyboardButton("Remind Pay", callback_data=f"rp:{user_id}"),
            ],
            [
                InlineKeyboardButton("Status", callback_data=f"st:{user_id}"),
                InlineKeyboardButton("Ban", callback_data=f"ban:{user_id}"),
            ],
        ]
    )


def format_review_card(user_id: int, record: dict[str, Any], heading: str) -> str:
    status = str(record.get("status") or "unknown").replace("_", " ").title()
    lines = [
        heading,
        "",
        f"Buyer: {format_person_label(record)}",
        f"Telegram ID: {user_id}",
        f"Telegram: {telegram_handle(record) or 'No username'}",
        f"Status: {status}",
        f"OF: {clean_text(record.get('of_username'))}",
        f"OFAuth: {verification_summary(record)}",
        f"Budget: {budget_line(record)}",
        f"Wants: {clean_text(record.get('purchase_intent'))}",
        f"Queue: {priority_label(record)}",
        f"Contact: {contact_mode_label(record)}",
        f"Payment: {payment_status_line(record)}",
    ]
    if record.get("status") in {"approved", "expired"}:
        lines.append(f"Access: {access_status_line(record)}")
    if record.get("relay_topic_name"):
        lines.append(f"Relay topic: {record.get('relay_topic_name')}")
    if record.get("not_fit_at"):
        lines.append(f"Not fit at: {format_datetime_for_user(record.get('not_fit_at'))}")
    if record.get("banned_at"):
        lines.append(f"Banned at: {format_datetime_for_user(record.get('banned_at'))}")
    return "\n".join(lines)


def format_pending_line(user_id: int, record: dict[str, Any]) -> str:
    parts = [
        str(user_id),
        display_name(record),
        clean_text(record.get("of_username")),
        budget_line(record),
        verification_badge(record),
    ]
    if record.get("review_priority") != "normal":
        parts.append(priority_label(record))
    return " | ".join(parts)


def get_low_priority_records(state: dict[str, Any]) -> list[tuple[int, dict[str, Any]]]:
    items: list[tuple[int, dict[str, Any]]] = []
    for user_id_text, record in state.get("users", {}).items():
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
        "Use /approve <user_id>, /approverelay <user_id>, /reject <user_id>, /priority <user_id>, or /status <user_id>."
    )
    return "\n".join(lines)


def format_admin_digest(state: dict[str, Any]) -> str:
    now = utc_now()
    soon = now + timedelta(days=7)
    sections: list[str] = ["Weekly admin digest"]
    users = [(int(user_id), record) for user_id, record in state.get("users", {}).items()]

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
    not_fit_count = sum(1 for _, record in users if record.get("status") == "not_fit")
    sections.extend(["", f"Closed: {not_fit_count} not fit, {banned_count} banned."])
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
    return format_review_card(user_id, record, "Buyer status")


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


def begin_application(record: dict[str, Any]) -> None:
    record["status"] = "awaiting_of_username"
    record["of_username"] = None
    record["budget_range_key"] = None
    record["budget_range_label"] = None
    record["budget_floor"] = None
    record["purchase_intent"] = None
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
    )
    return topic_id, topic_name


async def send_and_pin_payment_message(bot: Any, user_id: int, record: dict[str, Any]) -> None:
    current_time = utc_now()
    record["payment_status"] = "pending"
    record["payment_requested_at"] = to_iso(current_time)
    message = await bot.send_message(
        chat_id=user_id,
        text=payment_message(),
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


async def send_direct_contact(
    bot: Any,
    user_id: int,
    record: dict[str, Any],
    *,
    now: datetime | None = None,
) -> None:
    current_time = now or utc_now()
    private_username = get_required_env("PRIVATE_TELEGRAM_USERNAME")
    set_contact_mode(record, "direct", now=current_time)
    await bot.send_message(chat_id=user_id, text=direct_access_message(private_username, record))
    await send_and_pin_payment_message(bot, user_id, record)


async def send_relay_contact(
    bot: Any,
    state: dict[str, Any],
    user_id: int,
    record: dict[str, Any],
    *,
    now: datetime | None = None,
) -> tuple[int, str]:
    current_time = now or utc_now()
    topic_id, topic_name = await ensure_relay_topic(bot, state, user_id, record)
    set_contact_mode(record, "relay", now=current_time)
    await bot.send_message(chat_id=user_id, text=relay_access_message(record), protect_content=True)
    await send_and_pin_payment_message(bot, user_id, record)
    return topic_id, topic_name


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
        await update.message.reply_text("Your relay chat is not ready yet. Please wait a moment.")
        return
    get_relay_topics(state)[str(relay_topic_id)] = update.effective_user.id
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
        save_state(state)
        await update.message.reply_text("I couldn't send that through just now. Please try again in a moment.")
        admin_chat_id = resolve_admin_chat_id(state, update.effective_user)
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

    try:
        await context.bot.copy_message(
            chat_id=user_id,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
            protect_content=True,
        )
    except Exception as exc:
        LOGGER.exception("Could not relay admin message to user %s.", user_id)
        await context.bot.send_message(
            chat_id=relay_group_id,
            message_thread_id=record.get("relay_topic_id"),
            text=f"Delivery failed for this buyer: {exc}",
        )


async def ask_budget_question(message_target: Any) -> None:
    await message_target.reply_text(
        "My time is limited and I prioritize serious buyers. "
        "What range are you planning to spend in our first interaction?",
        reply_markup=build_budget_keyboard(),
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state = load_state()
    user = update.effective_user
    record = get_user_record(state, user.id)
    record["telegram_username"] = user.username
    record["first_name"] = user.first_name
    record["last_name"] = user.last_name
    mark_expired_if_needed(record)

    admin_chat_id = resolve_admin_chat_id(state, user)
    if admin_chat_id == update.effective_chat.id:
        state["admin_chat_id"] = admin_chat_id
        save_state(state)
        await update.message.reply_text(
            "Admin chat registered. New requests will be sent here.\n\n"
            "Commands:\n"
            "/pending [all|low|normal|priority|expired]\n"
            "/approve <user_id>\n"
            "/approverelay <user_id>\n"
            "/reject <user_id>\n"
            "/priority <user_id>\n"
            "/lowpriority <user_id>\n"
            "/renew <user_id>\n"
            "/senddirect <user_id>\n"
            "/status <user_id>\n"
            "/expiring\n"
            "/syncsubs\n"
            "/verifyof <onlyfans_username>\n"
            "/ofdiag"
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
    await update.message.reply_text("Please state your OF-username to continue.")


async def complete_application(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    state: dict[str, Any],
    record: dict[str, Any],
) -> None:
    user = update.effective_user
    if user is None or update.message is None:
        return

    admin_chat_id = resolve_admin_chat_id(state, user)
    exact_match_note = ""
    if ofauth_is_configured():
        try:
            verification_result = await asyncio.to_thread(
                verify_onlyfans_username,
                str(record.get("of_username") or ""),
            )
        except Exception as exc:
            exact_match_note = f"OFAuth check: error ({exc})"
        else:
            if verification_result.get("verified"):
                record["subscription_status"] = "active"
                record["onlyfans_user_id"] = verification_result.get("id")
                record["subscription_expires_at"] = verification_result.get("expired_at")
            else:
                record["subscription_status"] = "inactive"
                record["subscription_expires_at"] = None

    record["queued_at"] = to_iso(utc_now())
    if classify_low_priority(record):
        record["status"] = "low_priority"
        record["review_priority"] = "low"
        save_state(state)
        await update.message.reply_text(
            "Thanks. Based on your stated budget, your request has been placed in a slower review queue. "
            "I check that queue weekly."
        )
        return

    record["status"] = "pending"
    save_state(state)
    await update.message.reply_text("Thanks. Your request is pending manual review.")

    if not admin_chat_id:
        LOGGER.warning("No admin chat configured yet. Request stored but not delivered.")
        return

    admin_text = format_review_card(user.id, record, "New gatekeeper request")
    if exact_match_note:
        admin_text = f"{admin_text}\n{exact_match_note}"
    await context.bot.send_message(
        chat_id=admin_chat_id,
        text=admin_text,
        reply_markup=build_admin_review_keyboard(user.id),
    )


async def text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state = load_state()
    user = update.effective_user
    record = get_user_record(state, user.id)
    record["telegram_username"] = user.username
    record["first_name"] = user.first_name
    record["last_name"] = user.last_name
    mark_expired_if_needed(record)

    admin_chat_id = resolve_admin_chat_id(state, user)
    if admin_chat_id == update.effective_chat.id:
        await update.message.reply_text(
            "Use /pending [all|low|normal|priority|expired], /approve <user_id>, /approverelay <user_id>, /reject <user_id>, "
            "/priority <user_id>, /lowpriority <user_id>, /renew <user_id>, /senddirect <user_id>, /status <user_id>, "
            "/expiring, /syncsubs, /verifyof <onlyfans_username> or /ofdiag here."
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
        await update.message.reply_text("Please state your OF-username to continue.")
        return

    if status == "awaiting_of_username":
        record["of_username"] = update.message.text.strip()
        record["status"] = "awaiting_budget_range"
        save_state(state)
        await ask_budget_question(update.message)
        return

    if status == "awaiting_budget_range":
        await update.message.reply_text("Please choose a budget range using the buttons above.")
        return

    if status == "awaiting_purchase_intent":
        record["purchase_intent"] = update.message.text.strip()
        await complete_application(update, context, state, record)
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

    if data.startswith("budget:"):
        record = get_user_record(state, query.from_user.id)
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
        await query.answer("Budget range saved.")
        if query.message is not None:
            await query.edit_message_text(
                text=(
                    "My time is limited and I prioritize serious buyers. "
                    f"You selected: {option['label']}"
                )
            )
            await context.bot.send_message(
                chat_id=query.message.chat.id,
                text="What are you looking to purchase in our first interaction?",
            )
        return

    admin_chat_id = resolve_admin_chat_id(state, query.from_user)
    if not admin_chat_id or query.message is None or query.message.chat.id != admin_chat_id:
        await query.answer("Not allowed.", show_alert=True)
        return

    action, _, user_id_text = data.partition(":")
    if not user_id_text.isdigit():
        await query.answer("Invalid action.", show_alert=True)
        return

    user_id = int(user_id_text)
    record = get_user_record(state, user_id)

    if action == "st":
        markup = None
        if record.get("status") in {"pending", "low_priority"}:
            markup = build_admin_review_keyboard(user_id)
        elif record.get("status") == "approved":
            markup = build_post_approval_keyboard(user_id)
        await context.bot.send_message(
            chat_id=query.message.chat.id,
            text=format_status_message(user_id, record),
            reply_markup=markup,
        )
        await query.answer("Status sent.")
        return

    if action == "ban":
        current_time = utc_now()
        record["status"] = "banned"
        record["banned_at"] = to_iso(current_time)
        record["ban_reason"] = "Admin button"
        save_state(state)
        try:
            await context.bot.send_message(chat_id=user_id, text=template("banned"))
        except Exception:
            LOGGER.exception("Could not notify banned user %s.", user_id)
        await query.edit_message_text(format_review_card(user_id, record, "Banned"))
        await query.answer("Banned.")
        return

    if action == "nf":
        record["status"] = "not_fit"
        record["not_fit_at"] = to_iso(utc_now())
        save_state(state)
        try:
            await context.bot.send_message(chat_id=user_id, text=template("not_fit"))
        except Exception:
            LOGGER.exception("Could not notify not-fit user %s.", user_id)
        await query.edit_message_text(format_review_card(user_id, record, "Closed as not a fit"))
        await query.answer("Closed as not a fit.")
        return

    if action == "paid":
        if record.get("status") != "approved":
            await query.answer("Only approved buyers can be marked paid.", show_alert=True)
            return
        record["payment_status"] = "paid"
        record["payment_confirmed_at"] = to_iso(utc_now())
        save_state(state)
        await context.bot.send_message(chat_id=user_id, text=template("payment_confirmed"))
        await query.edit_message_text(
            format_review_card(user_id, record, "Payment confirmed"),
            reply_markup=build_post_approval_keyboard(user_id),
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
        await context.bot.send_message(chat_id=user_id, text=template("payment_reminder"))
        await query.edit_message_text(
            format_review_card(user_id, record, "Payment reminder sent"),
            reply_markup=build_post_approval_keyboard(user_id),
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
                topic_id, topic_name = await send_relay_contact(
                    context.bot,
                    state,
                    user_id,
                    record,
                    now=current_time,
                )
                save_state(state)
                await query.edit_message_text(
                    format_review_card(user_id, record, f"Approved in relay mode\nTopic: {topic_name}"),
                    reply_markup=build_post_approval_keyboard(user_id),
                )
                await query.answer(f"Relay approved in topic {topic_id}.")
            else:
                await send_direct_contact(context.bot, user_id, record, now=current_time)
                save_state(state)
                await query.edit_message_text(
                    format_review_card(user_id, record, "Approved direct"),
                    reply_markup=build_post_approval_keyboard(user_id),
                )
                await query.answer("Approved direct.")
        except Exception as exc:
            LOGGER.exception("Approval flow failed for user %s.", user_id)
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
        await context.bot.send_message(
            chat_id=user_id,
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
        await query.edit_message_text(
            format_review_card(user_id, record, "Marked priority"),
            reply_markup=build_admin_review_keyboard(user_id),
        )
        await query.answer("Marked priority.")
        return

    if action == "l":
        record["review_priority"] = "low"
        record["status"] = "low_priority"
        save_state(state)
        await query.edit_message_text(format_review_card(user_id, record, "Moved to low-priority queue"))
        await query.answer("Moved to low-priority queue.")
        return

    await query.answer("Unknown action.", show_alert=True)


async def pending(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state = load_state()
    admin_chat_id = resolve_admin_chat_id(state, update.effective_user)
    if admin_chat_id != update.effective_chat.id:
        return

    mode = normalize_username(context.args[0]) if context.args else "all"
    items = []
    for user_id_text, record in state.get("users", {}).items():
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

    if not items:
        await update.message.reply_text(f"No requests found for filter '{mode}'.")
        return

    await update.message.reply_text("\n".join(items[:50]))


async def expiring(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state = load_state()
    admin_chat_id = resolve_admin_chat_id(state, update.effective_user)
    if admin_chat_id != update.effective_chat.id:
        return

    now = utc_now()
    soon = now + timedelta(days=7)
    items = []
    for user_id_text, record in state.get("users", {}).items():
        expires_at = parse_iso(record.get("expires_at"))
        if record.get("status") == "approved" and expires_at and expires_at <= soon:
            items.append(
                f"{int(user_id_text)} | {display_name(record)} | expires {format_date_for_user(record.get('expires_at'))} | {budget_line(record)}"
            )
        elif record.get("status") == "expired":
            items.append(
                f"{int(user_id_text)} | {display_name(record)} | expired"
            )

    if not items:
        await update.message.reply_text("No users expiring soon.")
        return

    await update.message.reply_text("\n".join(items[:50]))


async def sync_subs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state = load_state()
    admin_chat_id = resolve_admin_chat_id(state, update.effective_user)
    if admin_chat_id != update.effective_chat.id:
        return

    if not ofauth_is_configured():
        await update.message.reply_text(
            "OFAuth is not configured. Set OFAUTH_API_KEY and OFAUTH_CONNECTION_ID first."
        )
        return

    await update.message.reply_text("Syncing OnlyFans...")
    try:
        summary = await asyncio.to_thread(sync_subscribers, state)
    except Exception as exc:
        LOGGER.exception("OFAuth sync failed.")
        await update.message.reply_text(f"OnlyFans sync failed: {exc}")
        return

    save_state(state)
    await update.message.reply_text(format_sync_summary(summary))
    expired_alert = format_expired_access_alert(summary)
    if expired_alert:
        await update.message.reply_text(expired_alert)


async def verifyof(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state = load_state()
    admin_chat_id = resolve_admin_chat_id(state, update.effective_user)
    if admin_chat_id != update.effective_chat.id:
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
        await update.message.reply_text(f"OFAuth verification failed: {exc}")
        return

    if verification_result.get("verified"):
        lines = [
            "Verified",
            "",
            f"OF username: {verification_result.get('username') or claimed_username}",
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

    lines = [
        "Unverified",
        "",
        f"OF username: {verification_result.get('username') or claimed_username}",
        "No active subscription found.",
    ]
    if show_debug:
        lines.append(f"Source: {verification_result.get('source')}")
        if verification_result.get("reason"):
            lines.append(f"Reason: {verification_result['reason']}")
    await update.message.reply_text("\n".join(lines))


async def ofdiag(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state = load_state()
    admin_chat_id = resolve_admin_chat_id(state, update.effective_user)
    if admin_chat_id != update.effective_chat.id:
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

    state = load_state()
    admin_chat_id = resolve_admin_chat_id(state, update.effective_user)
    if admin_chat_id != update.effective_chat.id:
        return

    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: /status <user_id>")
        return

    user_id = int(context.args[0])
    record = get_user_record(state, user_id)
    await update.message.reply_text(format_status_message(user_id, record))


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


async def reprioritize(update: Update, context: ContextTypes.DEFAULT_TYPE, new_priority: str) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state = load_state()
    admin_chat_id = resolve_admin_chat_id(state, update.effective_user)
    if admin_chat_id != update.effective_chat.id:
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
    await update.message.reply_text(f"Updated queue to {new_priority}.")


async def renew_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state = load_state()
    admin_chat_id = resolve_admin_chat_id(state, update.effective_user)
    if admin_chat_id != update.effective_chat.id:
        return

    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: /renew <user_id>")
        return

    user_id = int(context.args[0])
    record = get_user_record(state, user_id)
    grant_access(record)
    save_state(state)
    await update.message.reply_text(
        f"Renewed. Access now ends {format_date_for_user(record.get('expires_at'))}."
    )


async def senddirect_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state = load_state()
    admin_chat_id = resolve_admin_chat_id(state, update.effective_user)
    if admin_chat_id != update.effective_chat.id:
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
    await update.message.reply_text("Direct handle sent.")


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

    state = load_state()
    admin_chat_id = resolve_admin_chat_id(state, update.effective_user)
    if admin_chat_id != update.effective_chat.id:
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
            if approval_mode == "relay":
                await send_relay_contact(context.bot, state, user_id, record, now=current_time)
                save_state(state)
                await update.message.reply_text("Approved in relay mode.")
            else:
                await send_direct_contact(context.bot, user_id, record, now=current_time)
                save_state(state)
                await update.message.reply_text("Approved and sent.")
        except Exception as exc:
            LOGGER.exception("Manual approval failed for user %s.", user_id)
            record["status"] = "pending"
            save_state(state)
            await update.message.reply_text(f"Approval failed: {exc}")
        return

    record["status"] = "rejected"
    save_state(state)
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
    record = get_user_record(state, update.effective_user.id)
    mark_expired_if_needed(record)
    if is_closed_record(record):
        save_state(state)
        return
    if relay_mode_enabled(record):
        await relay_buyer_message(update, context, state, record)
        return
    if record.get("status") == "awaiting_of_username":
        await update.message.reply_text("Please send your OF-username as text.")
    elif record.get("status") == "awaiting_budget_range":
        await update.message.reply_text("Please choose a budget range using the buttons above.")
    elif record.get("status") == "awaiting_purchase_intent":
        await update.message.reply_text("Please tell me what you are looking to purchase in text.")


def main() -> None:
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
    app.add_handler(CommandHandler("renew", renew_manual))
    app.add_handler(CommandHandler("senddirect", senddirect_manual))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("expiring", expiring))
    app.add_handler(CommandHandler("syncsubs", sync_subs))
    app.add_handler(CommandHandler("verifyof", verifyof))
    app.add_handler(CommandHandler("ofdiag", ofdiag))
    app.add_handler(CallbackQueryHandler(button_click))
    app.add_handler(
        MessageHandler(filters.ChatType.SUPERGROUP & ~filters.COMMAND, relay_admin_group_message),
        group=-1,
    )
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_message))
    app.add_handler(MessageHandler(~filters.TEXT & ~filters.COMMAND, non_text_message))

    LOGGER.info("Bot is running.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
