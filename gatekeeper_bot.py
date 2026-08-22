"""Telegram gatekeeper bot.

Customers redeem a one-time access link, then talk to the operator through
this bot. Their messages are copied into a forum topic inside a private admin
group, and replies in that topic are copied back to them. The operator's
personal Telegram handle is never exposed.

Access runs on a timer that the operator refreshes by tapping a button. The
bot never contacts Fansly or any other platform; it only tracks how long it
has been since access was last confirmed, and fails closed when that lapses.
"""

import asyncio
import json
import logging
import os
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
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

# Ambiguous characters (0/O, 1/I/L) are left out so a code survives being read
# aloud, retyped, or copied out of a screenshot.
CODE_ALPHABET = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"
CODE_LENGTH = 6

STATUS_ACTIVE = "active"
STATUS_PAUSED = "paused"
STATUS_REVOKED = "revoked"

# A paused customer is told once per this window, no matter how much they send.
PAUSE_NOTICE_INTERVAL_HOURS = 24


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


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


def get_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_optional_env(name: str) -> str | None:
    value = os.getenv(name, "").strip()
    return value or None


def get_access_duration_days() -> int:
    return int(os.getenv("ACCESS_DURATION_DAYS", "30"))


def get_expiry_warning_days() -> int:
    return int(os.getenv("EXPIRY_WARNING_DAYS", "7"))


def get_relay_group_id() -> int | None:
    value = get_optional_env("RELAY_ADMIN_GROUP_ID")
    if not value:
        return None
    return int(value)


def get_data_dir() -> Path:
    return Path(
        os.getenv("BOT_DATA_DIR", "").strip()
        or os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "").strip()
        or Path(__file__).resolve().parent
    )


def get_state_path() -> Path:
    return get_data_dir() / "bot_state.json"


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------


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


def format_date(value: str | datetime | None, *, empty: str = "Not set") -> str:
    parsed = value if isinstance(value, datetime) else parse_iso(value)
    if parsed is None:
        return empty
    return parsed.astimezone(DISPLAY_TIMEZONE).strftime("%d %b %Y")


def days_until(value: str | None, now: datetime | None = None) -> int | None:
    expires_at = parse_iso(value)
    if expires_at is None:
        return None
    delta = expires_at - (now or utc_now())
    return int(delta.total_seconds() // 86400)


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------


def empty_state() -> dict[str, Any]:
    return {"admin_chat_id": None, "codes": {}, "users": {}, "topics": {}}


def load_state() -> dict[str, Any]:
    state_path = get_state_path()
    if not state_path.exists():
        return empty_state()
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        LOGGER.warning("State file was invalid JSON, starting fresh.")
        return empty_state()
    for key, value in empty_state().items():
        state.setdefault(key, value)
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


def default_user_record() -> dict[str, Any]:
    return {
        "status": STATUS_ACTIVE,
        "fansly_handle": None,
        "code": None,
        "telegram_username": None,
        "first_name": None,
        "last_name": None,
        "granted_at": None,
        "expires_at": None,
        "last_extended_at": None,
        "paused_at": None,
        "pause_notice_sent_at": None,
        "topic_id": None,
        "topic_name": None,
    }


KNOWN_STATUSES = {STATUS_ACTIVE, STATUS_PAUSED, STATUS_REVOKED}


def get_user_record(state: dict[str, Any], user_id: int) -> dict[str, Any] | None:
    """Return a stored customer, or None if this user has never redeemed a code."""
    record = state.get("users", {}).get(str(user_id))
    if record is None:
        return None
    for key, value in default_user_record().items():
        record.setdefault(key, value)
    # A record written by an older version carries a status this model doesn't
    # know. Fail closed rather than leave it as a customer who never expires.
    if record.get("status") not in KNOWN_STATUSES:
        record["status"] = STATUS_PAUSED
        record.setdefault("paused_at", to_iso(utc_now()))
    return record


def create_user_record(state: dict[str, Any], user_id: int) -> dict[str, Any]:
    record = default_user_record()
    state.setdefault("users", {})[str(user_id)] = record
    return record


def remember_identity(record: dict[str, Any], user: Any) -> None:
    record["telegram_username"] = user.username
    record["first_name"] = user.first_name
    record["last_name"] = user.last_name


# ---------------------------------------------------------------------------
# Labels
# ---------------------------------------------------------------------------


def clean_text(value: Any, *, empty: str = "Not provided") -> str:
    text = str(value or "").strip()
    return text or empty


def normalize_handle(value: str) -> str:
    return value.strip().lstrip("@")


def display_name(record: dict[str, Any]) -> str:
    parts = [
        str(record.get("first_name") or "").strip(),
        str(record.get("last_name") or "").strip(),
    ]
    full_name = " ".join(part for part in parts if part).strip()
    if full_name:
        return full_name
    username = str(record.get("telegram_username") or "").strip()
    if username:
        return f"@{username}"
    return "Unknown"


def person_label(record: dict[str, Any]) -> str:
    name = display_name(record)
    handle = str(record.get("fansly_handle") or "").strip()
    if handle:
        return f"{name} ({handle})"
    return name


def truncate_text(value: str, limit: int) -> str:
    text = value.strip()
    if len(text) <= limit:
        return text
    return text[: max(limit - 1, 1)].rstrip() + "…"


def build_topic_name(record: dict[str, Any]) -> str:
    parts = [display_name(record)]
    handle = str(record.get("fansly_handle") or "").strip()
    if handle:
        parts.append(handle)
    return truncate_text(" | ".join(part for part in parts if part) or "Customer", 120)


def status_line(record: dict[str, Any]) -> str:
    status = record.get("status")
    if status == STATUS_ACTIVE:
        remaining = days_until(record.get("expires_at"))
        if remaining is None:
            return "Active"
        return f"Active until {format_date(record.get('expires_at'))} ({remaining}d left)"
    if status == STATUS_PAUSED:
        return f"Paused since {format_date(record.get('paused_at'))}"
    if status == STATUS_REVOKED:
        return "Revoked"
    return "Unknown"


# ---------------------------------------------------------------------------
# Access codes
# ---------------------------------------------------------------------------


def generate_code(state: dict[str, Any]) -> str:
    codes = state.setdefault("codes", {})
    while True:
        code = "".join(secrets.choice(CODE_ALPHABET) for _ in range(CODE_LENGTH))
        if code not in codes:
            return code


def create_code(state: dict[str, Any], fansly_handle: str) -> dict[str, Any]:
    code = generate_code(state)
    entry = {
        "code": code,
        "fansly_handle": fansly_handle,
        "created_at": to_iso(utc_now()),
        "redeemed_at": None,
        "redeemed_by": None,
    }
    state.setdefault("codes", {})[code] = entry
    return entry


def normalize_code(value: str) -> str:
    return "".join(char for char in value.strip().upper() if char.isalnum())


def find_code(state: dict[str, Any], value: str) -> dict[str, Any] | None:
    return state.get("codes", {}).get(normalize_code(value))


def build_access_link(bot_username: str, code: str) -> str:
    return f"https://t.me/{bot_username}?start={code}"


# ---------------------------------------------------------------------------
# Access lifecycle
# ---------------------------------------------------------------------------


def grant_access(record: dict[str, Any], now: datetime | None = None) -> None:
    current_time = now or utc_now()
    record["status"] = STATUS_ACTIVE
    record["granted_at"] = to_iso(current_time)
    record["expires_at"] = to_iso(current_time + timedelta(days=get_access_duration_days()))
    record["paused_at"] = None
    record["pause_notice_sent_at"] = None


def extend_access(
    record: dict[str, Any], days: int | None = None, now: datetime | None = None
) -> None:
    """Add time to the current expiry rather than to today.

    Extending a week early therefore costs the customer nothing, so the whole
    digest can be cleared in one sitting without having to time each tap.
    """
    current_time = now or utc_now()
    duration = days if days is not None else get_access_duration_days()
    current_expiry = parse_iso(record.get("expires_at"))
    anchor = max(current_expiry, current_time) if current_expiry else current_time
    record["status"] = STATUS_ACTIVE
    record["expires_at"] = to_iso(anchor + timedelta(days=duration))
    record["last_extended_at"] = to_iso(current_time)
    record["paused_at"] = None
    record["pause_notice_sent_at"] = None


def is_access_active(record: dict[str, Any], now: datetime | None = None) -> bool:
    if record.get("status") != STATUS_ACTIVE:
        return False
    expires_at = parse_iso(record.get("expires_at"))
    if expires_at is None:
        return True
    return expires_at > (now or utc_now())


def has_lapsed(record: dict[str, Any], now: datetime | None = None) -> bool:
    return record.get("status") == STATUS_ACTIVE and not is_access_active(record, now=now)


def mark_paused(record: dict[str, Any], now: datetime | None = None) -> None:
    record["status"] = STATUS_PAUSED
    record["paused_at"] = to_iso(now or utc_now())


def should_send_pause_notice(record: dict[str, Any], now: datetime | None = None) -> bool:
    last_sent = parse_iso(record.get("pause_notice_sent_at"))
    if last_sent is None:
        return True
    return (now or utc_now()) - last_sent >= timedelta(hours=PAUSE_NOTICE_INTERVAL_HOURS)


def get_expiring_soon(
    state: dict[str, Any], now: datetime | None = None
) -> list[tuple[int, dict[str, Any]]]:
    current_time = now or utc_now()
    cutoff = current_time + timedelta(days=get_expiry_warning_days())
    items: list[tuple[int, dict[str, Any]]] = []
    for user_id_text, record in state.get("users", {}).items():
        if record.get("status") != STATUS_ACTIVE:
            continue
        expires_at = parse_iso(record.get("expires_at"))
        if expires_at is not None and expires_at <= cutoff:
            items.append((int(user_id_text), record))
    items.sort(key=lambda item: item[1].get("expires_at") or "")
    return items


def get_paused(state: dict[str, Any]) -> list[tuple[int, dict[str, Any]]]:
    items = [
        (int(user_id_text), record)
        for user_id_text, record in state.get("users", {}).items()
        if record.get("status") == STATUS_PAUSED
    ]
    items.sort(key=lambda item: item[1].get("paused_at") or "")
    return items


# ---------------------------------------------------------------------------
# Forum topics
# ---------------------------------------------------------------------------


def get_topics(state: dict[str, Any]) -> dict[str, Any]:
    return state.setdefault("topics", {})


def get_topic_user_id(state: dict[str, Any], topic_id: int | None) -> int | None:
    if topic_id is None:
        return None
    raw = get_topics(state).get(str(topic_id))
    return None if raw is None else int(raw)


def topic_intro_text(user_id: int, record: dict[str, Any]) -> str:
    return "\n".join(
        [
            "Access granted",
            person_label(record),
            f"ID: {user_id}",
            f"Code: {clean_text(record.get('code'))}",
            status_line(record),
            "",
            "Reply here to message this customer.",
            "Messages starting with // stay in this topic only.",
        ]
    )


async def ensure_topic(
    bot: Any, state: dict[str, Any], user_id: int, record: dict[str, Any]
) -> int:
    relay_group_id = get_relay_group_id()
    if relay_group_id is None:
        raise RuntimeError("Relay is not configured. Set RELAY_ADMIN_GROUP_ID first.")

    existing_topic_id = record.get("topic_id")
    if isinstance(existing_topic_id, int):
        get_topics(state)[str(existing_topic_id)] = user_id
        return existing_topic_id

    topic_name = build_topic_name(record)
    topic = await bot.create_forum_topic(chat_id=relay_group_id, name=topic_name)
    topic_id = int(topic.message_thread_id)
    record["topic_id"] = topic_id
    record["topic_name"] = topic_name
    get_topics(state)[str(topic_id)] = user_id
    await bot.send_message(
        chat_id=relay_group_id,
        message_thread_id=topic_id,
        text=topic_intro_text(user_id, record),
    )
    return topic_id


async def close_topic(bot: Any, record: dict[str, Any], note: str | None = None) -> None:
    relay_group_id = get_relay_group_id()
    topic_id = record.get("topic_id")
    if relay_group_id is None or not isinstance(topic_id, int):
        return
    if note:
        try:
            await bot.send_message(chat_id=relay_group_id, message_thread_id=topic_id, text=note)
        except Exception:
            LOGGER.exception("Could not post a note before closing topic %s.", topic_id)
    try:
        await bot.close_forum_topic(chat_id=relay_group_id, message_thread_id=topic_id)
    except Exception:
        LOGGER.exception("Could not close topic %s.", topic_id)


async def reopen_topic(bot: Any, record: dict[str, Any], note: str | None = None) -> None:
    relay_group_id = get_relay_group_id()
    topic_id = record.get("topic_id")
    if relay_group_id is None or not isinstance(topic_id, int):
        return
    try:
        await bot.reopen_forum_topic(chat_id=relay_group_id, message_thread_id=topic_id)
    except Exception:
        LOGGER.exception("Could not reopen topic %s.", topic_id)
    if note:
        try:
            await bot.send_message(chat_id=relay_group_id, message_thread_id=topic_id, text=note)
        except Exception:
            LOGGER.exception("Could not post a note after reopening topic %s.", topic_id)


def is_internal_note(message: Any) -> bool:
    text = str(message.text or message.caption or "").strip()
    return text.startswith("//")


# ---------------------------------------------------------------------------
# Customer-facing copy
# ---------------------------------------------------------------------------


def welcome_text(record: dict[str, Any]) -> str:
    return (
        "You're in. Send me a message here whenever you like and I'll reply personally.\n\n"
        f"Your access runs until {format_date(record.get('expires_at'))} and keeps "
        "renewing while your subscription is active."
    )


def paused_text() -> str:
    return (
        "Your access has paused because your subscription is no longer showing as active.\n\n"
        "Resubscribe and send me a message here and I'll switch it back on. "
        "You don't need a new link."
    )


def no_code_text() -> str:
    return (
        "This bot is for subscribers only. You'll get a personal access link in a "
        "direct message once you subscribe to the Telegram tier.\n\n"
        "Already have a code? Send it to me here."
    )


def bad_code_text() -> str:
    return "That code isn't valid. Check it and try again, or ask me for a new link."


def used_code_text() -> str:
    return "That code has already been used. If that wasn't you, message me and I'll sort it out."


# ---------------------------------------------------------------------------
# Admin plumbing
# ---------------------------------------------------------------------------


def resolve_admin_chat_id(state: dict[str, Any], user: Any) -> int | None:
    env_admin_chat_id = os.getenv("ADMIN_CHAT_ID", "").strip()
    if env_admin_chat_id:
        return int(env_admin_chat_id)

    configured_username = normalize_handle(os.getenv("ADMIN_USERNAME", "")).lower()
    current_username = normalize_handle(user.username or "").lower()
    saved_admin_chat_id = state.get("admin_chat_id")

    if saved_admin_chat_id:
        return int(saved_admin_chat_id)
    if configured_username and configured_username == current_username:
        return user.id
    return None


def is_admin_chat(state: dict[str, Any], update: Update) -> bool:
    if not update.effective_user or not update.effective_chat:
        return False
    if update.effective_chat.type != "private":
        return False
    admin_chat_id = resolve_admin_chat_id(state, update.effective_user)
    return admin_chat_id is not None and admin_chat_id == update.effective_chat.id


ADMIN_HELP = "\n".join(
    [
        "Commands:",
        "/newcode <fansly_handle> - create a one-time access link",
        "/codes - unused access links",
        "/customers - everyone with access",
        "/who <user_id> - one customer's details",
        "/extend <user_id> [days] - add time",
        "/revoke <user_id> - cut access now",
        "/expiring - who lapses soon, with buttons",
    ]
)


def build_renewal_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Extend", callback_data=f"ext:{user_id}"),
                InlineKeyboardButton("❌ Cut", callback_data=f"cut:{user_id}"),
            ]
        ]
    )


def format_customer_line(user_id: int, record: dict[str, Any]) -> str:
    return f"{user_id} | {person_label(record)} | {status_line(record)}"


def format_customer_details(user_id: int, record: dict[str, Any]) -> str:
    username = record.get("telegram_username")
    return "\n".join(
        [
            person_label(record),
            f"ID: {user_id}",
            f"Telegram: @{username}" if username else "Telegram: Not set",
            f"Fansly: {clean_text(record.get('fansly_handle'))}",
            f"Code: {clean_text(record.get('code'))}",
            status_line(record),
            f"First granted: {format_date(record.get('granted_at'))}",
            f"Last extended: {format_date(record.get('last_extended_at'), empty='Never')}",
        ]
    )


# ---------------------------------------------------------------------------
# Customer handlers
# ---------------------------------------------------------------------------


async def redeem_code(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    state: dict[str, Any],
    raw_code: str,
) -> None:
    user = update.effective_user
    if user is None or update.message is None:
        return

    entry = find_code(state, raw_code)
    if entry is None:
        await update.message.reply_text(bad_code_text())
        return

    if entry.get("redeemed_by") is not None:
        record = get_user_record(state, user.id)
        if int(entry["redeemed_by"]) == user.id and record is not None:
            # Re-clicking your own link is harmless, so just restate the status.
            await update.message.reply_text(
                welcome_text(record) if record.get("status") == STATUS_ACTIVE else paused_text()
            )
            return
        await update.message.reply_text(used_code_text())
        return

    record = get_user_record(state, user.id) or create_user_record(state, user.id)
    remember_identity(record, user)
    record["fansly_handle"] = entry.get("fansly_handle")
    record["code"] = entry["code"]
    grant_access(record)

    entry["redeemed_at"] = to_iso(utc_now())
    entry["redeemed_by"] = user.id

    try:
        await ensure_topic(context.bot, state, user.id, record)
    except Exception as exc:
        LOGGER.exception("Could not open a topic for user %s.", user.id)
        save_state(state)
        await update.message.reply_text(
            "You're in, but I couldn't open our chat thread just yet. "
            "Send me a message and I'll pick it up."
        )
        admin_chat_id = state.get("admin_chat_id")
        if admin_chat_id:
            await context.bot.send_message(
                chat_id=int(admin_chat_id),
                text=f"Topic creation failed for {user.id} ({person_label(record)}): {exc}",
            )
        return

    save_state(state)
    await update.message.reply_text(welcome_text(record))


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state = load_state()
    user = update.effective_user

    if is_admin_chat(state, update):
        state["admin_chat_id"] = update.effective_chat.id
        save_state(state)
        await update.message.reply_text(f"Admin chat registered.\n\n{ADMIN_HELP}")
        return

    # Deep link: t.me/<bot>?start=<code>
    if context.args:
        await redeem_code(update, context, state, context.args[0])
        return

    record = get_user_record(state, user.id)
    if record is None:
        await update.message.reply_text(no_code_text())
        return

    remember_identity(record, user)
    if has_lapsed(record):
        mark_paused(record)
        await close_topic(context.bot, record, "Access lapsed. Relay paused.")
    save_state(state)

    if record.get("status") == STATUS_ACTIVE:
        await update.message.reply_text(welcome_text(record))
    else:
        await update.message.reply_text(paused_text())


async def relay_to_topic(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    state: dict[str, Any],
    record: dict[str, Any],
) -> bool:
    relay_group_id = get_relay_group_id()
    if relay_group_id is None or update.effective_user is None or update.message is None:
        return False
    if update.effective_chat is None:
        return False

    user_id = update.effective_user.id
    try:
        topic_id = await ensure_topic(context.bot, state, user_id, record)
    except Exception:
        LOGGER.exception("Could not resolve a topic for user %s.", user_id)
        return False

    get_topics(state)[str(topic_id)] = user_id

    try:
        await context.bot.copy_message(
            chat_id=relay_group_id,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
            message_thread_id=topic_id,
        )
    except Exception as exc:
        LOGGER.exception("Could not relay a message from user %s.", user_id)
        admin_chat_id = state.get("admin_chat_id")
        if admin_chat_id:
            try:
                await context.bot.send_message(
                    chat_id=int(admin_chat_id),
                    text=f"Relay failed for {user_id} ({person_label(record)}): {exc}",
                )
            except Exception:
                LOGGER.exception("Could not alert the admin about the relay failure.")
        return False
    return True


async def customer_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle anything a customer sends in a private chat with the bot."""
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state = load_state()
    user = update.effective_user

    if is_admin_chat(state, update):
        state["admin_chat_id"] = update.effective_chat.id
        save_state(state)
        await update.message.reply_text(ADMIN_HELP)
        return

    record = get_user_record(state, user.id)

    if record is None:
        # Never redeemed anything, so their message may itself be a pasted code.
        text = str(update.message.text or "").strip()
        if text and len(normalize_code(text)) == CODE_LENGTH:
            await redeem_code(update, context, state, text)
            return
        await update.message.reply_text(no_code_text())
        return

    remember_identity(record, user)

    if record.get("status") == STATUS_REVOKED:
        save_state(state)
        return

    if has_lapsed(record):
        mark_paused(record)
        await close_topic(context.bot, record, "Access lapsed. Relay paused.")

    paused = record.get("status") == STATUS_PAUSED

    # Paused customers still reach the operator: someone messaging after a lapse
    # is usually about to resubscribe, and the closed topic already flags them.
    delivered = await relay_to_topic(update, context, state, record)

    if paused and should_send_pause_notice(record):
        record["pause_notice_sent_at"] = to_iso(utc_now())
        await update.message.reply_text(paused_text())

    save_state(state)

    if not delivered:
        await update.message.reply_text(
            "I couldn't get that through just now. Please try again in a moment."
        )


async def relay_group_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Copy an operator's reply in a forum topic back to the matching customer."""
    if not update.effective_chat or not update.message or not update.effective_user:
        return
    relay_group_id = get_relay_group_id()
    if relay_group_id is None or update.effective_chat.id != relay_group_id:
        return
    if update.effective_user.is_bot:
        return
    if is_internal_note(update.message):
        return

    state = load_state()
    user_id = get_topic_user_id(state, update.message.message_thread_id)
    if user_id is None:
        return

    record = get_user_record(state, user_id)
    if record is None:
        return

    if record.get("status") == STATUS_REVOKED:
        await context.bot.send_message(
            chat_id=relay_group_id,
            message_thread_id=update.message.message_thread_id,
            text="This customer's access was revoked. Extend them first if you want to reply.",
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
        LOGGER.exception("Could not deliver an operator reply to user %s.", user_id)
        await context.bot.send_message(
            chat_id=relay_group_id,
            message_thread_id=update.message.message_thread_id,
            text=f"Delivery failed: {exc}",
        )


# ---------------------------------------------------------------------------
# Admin commands
# ---------------------------------------------------------------------------


async def newcode(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    state = load_state()
    if not is_admin_chat(state, update) or update.message is None:
        return

    if not context.args:
        await update.message.reply_text("Usage: /newcode <fansly_handle>")
        return

    fansly_handle = normalize_handle(" ".join(context.args))
    entry = create_code(state, fansly_handle)
    save_state(state)

    bot_username = (await context.bot.get_me()).username
    link = build_access_link(bot_username, entry["code"])
    await update.message.reply_text(
        f"Access link for {fansly_handle}:\n\n{link}\n\n"
        f"Code: {entry['code']}\nWorks once. Send it in a Fansly DM."
    )


async def codes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    state = load_state()
    if not is_admin_chat(state, update) or update.message is None:
        return

    unused = [entry for entry in state.get("codes", {}).values() if entry.get("redeemed_by") is None]
    unused.sort(key=lambda entry: entry.get("created_at") or "")
    if not unused:
        await update.message.reply_text("No unused access links.")
        return

    bot_username = (await context.bot.get_me()).username
    lines = ["Unused access links:", ""]
    for entry in unused[:50]:
        lines.append(
            f"{clean_text(entry.get('fansly_handle'), empty='(no handle)')} - "
            f"{build_access_link(bot_username, entry['code'])} "
            f"(made {format_date(entry.get('created_at'))})"
        )
    await update.message.reply_text("\n".join(lines))


async def customers(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    state = load_state()
    if not is_admin_chat(state, update) or update.message is None:
        return

    items = [
        (int(user_id_text), record)
        for user_id_text, record in state.get("users", {}).items()
        if record.get("status") != STATUS_REVOKED
    ]
    if not items:
        await update.message.reply_text("No customers yet.")
        return

    items.sort(key=lambda item: item[1].get("expires_at") or "")
    lines = [format_customer_line(user_id, record) for user_id, record in items[:50]]
    await update.message.reply_text("\n".join(lines))


async def who(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    state = load_state()
    if not is_admin_chat(state, update) or update.message is None:
        return

    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: /who <user_id>")
        return

    user_id = int(context.args[0])
    record = get_user_record(state, user_id)
    if record is None:
        await update.message.reply_text("No customer with that ID.")
        return

    await update.message.reply_text(format_customer_details(user_id, record))


async def extend(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    state = load_state()
    if not is_admin_chat(state, update) or update.message is None:
        return

    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: /extend <user_id> [days]")
        return

    user_id = int(context.args[0])
    days = int(context.args[1]) if len(context.args) > 1 and context.args[1].isdigit() else None
    record = get_user_record(state, user_id)
    if record is None:
        await update.message.reply_text("No customer with that ID.")
        return

    was_inactive = record.get("status") != STATUS_ACTIVE
    extend_access(record, days=days)
    if was_inactive:
        await reopen_topic(context.bot, record, "Access restored.")
    save_state(state)
    await update.message.reply_text(
        f"{person_label(record)} extended to {format_date(record.get('expires_at'))}."
    )


async def revoke(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    state = load_state()
    if not is_admin_chat(state, update) or update.message is None:
        return

    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: /revoke <user_id>")
        return

    user_id = int(context.args[0])
    record = get_user_record(state, user_id)
    if record is None:
        await update.message.reply_text("No customer with that ID.")
        return

    record["status"] = STATUS_REVOKED
    record["paused_at"] = to_iso(utc_now())
    await close_topic(context.bot, record, "Access revoked.")
    save_state(state)
    await update.message.reply_text(f"{person_label(record)} revoked.")


async def expiring(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    state = load_state()
    if not is_admin_chat(state, update) or update.message is None:
        return

    soon = get_expiring_soon(state)
    paused = get_paused(state)

    if not soon and not paused:
        await update.message.reply_text(
            f"Nobody lapses in the next {get_expiry_warning_days()} days."
        )
        return

    for user_id, record in soon:
        remaining = days_until(record.get("expires_at"))
        await update.message.reply_text(
            f"{person_label(record)}\nID: {user_id}\n"
            f"Lapses {format_date(record.get('expires_at'))} ({remaining}d)",
            reply_markup=build_renewal_keyboard(user_id),
        )

    for user_id, record in paused:
        await update.message.reply_text(
            f"{person_label(record)}\nID: {user_id}\n"
            f"Paused since {format_date(record.get('paused_at'))}",
            reply_markup=build_renewal_keyboard(user_id),
        )


async def button_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.from_user:
        return

    state = load_state()
    admin_chat_id = resolve_admin_chat_id(state, query.from_user)
    if not admin_chat_id or query.message is None or query.message.chat.id != admin_chat_id:
        await query.answer("Not allowed.", show_alert=True)
        return

    action, _, user_id_text = (query.data or "").partition(":")
    if not user_id_text.isdigit():
        await query.answer("Invalid action.", show_alert=True)
        return

    user_id = int(user_id_text)
    record = get_user_record(state, user_id)
    if record is None:
        await query.answer("No customer with that ID.", show_alert=True)
        return

    if action == "ext":
        was_inactive = record.get("status") != STATUS_ACTIVE
        extend_access(record)
        if was_inactive:
            await reopen_topic(context.bot, record, "Access restored.")
        save_state(state)
        await query.edit_message_text(
            f"{person_label(record)} extended to {format_date(record.get('expires_at'))}."
        )
        await query.answer("Extended.")
        return

    if action == "cut":
        mark_paused(record)
        await close_topic(context.bot, record, "Access cut. Relay paused.")
        save_state(state)
        await query.edit_message_text(f"{person_label(record)} paused.")
        await query.answer("Paused.")
        return

    await query.answer("Unknown action.", show_alert=True)


# ---------------------------------------------------------------------------
# Daily sweep, shared with access_digest.py
# ---------------------------------------------------------------------------


async def sweep_lapsed(bot: Any, state: dict[str, Any]) -> list[tuple[int, dict[str, Any]]]:
    """Pause everyone whose access ran out and close their topics."""
    newly_paused: list[tuple[int, dict[str, Any]]] = []
    now = utc_now()
    for user_id_text, record in state.get("users", {}).items():
        if has_lapsed(record, now=now):
            mark_paused(record, now=now)
            await close_topic(bot, record, "Access lapsed. Relay paused.")
            newly_paused.append((int(user_id_text), record))
    return newly_paused


def format_digest(
    state: dict[str, Any], newly_paused: list[tuple[int, dict[str, Any]]]
) -> str | None:
    soon = get_expiring_soon(state)
    if not soon and not newly_paused:
        return None

    lines = ["Access check"]
    if newly_paused:
        lines.extend(["", "Paused today:"])
        for user_id, record in newly_paused:
            lines.append(f"{user_id} | {person_label(record)}")
    if soon:
        lines.extend(["", f"Lapsing within {get_expiry_warning_days()} days:"])
        for user_id, record in soon:
            remaining = days_until(record.get("expires_at"))
            lines.append(
                f"{user_id} | {person_label(record)} | "
                f"{format_date(record.get('expires_at'))} ({remaining}d)"
            )
    lines.extend(["", "Run /expiring to extend or cut with buttons."])
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def main() -> None:
    load_dotenv_file()
    token = get_required_env("BOT_TOKEN")
    if get_relay_group_id() is None:
        LOGGER.warning("RELAY_ADMIN_GROUP_ID is not set. Customer messages cannot be relayed.")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    app = Application.builder().token(token).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("newcode", newcode))
    app.add_handler(CommandHandler("codes", codes))
    app.add_handler(CommandHandler("customers", customers))
    app.add_handler(CommandHandler("who", who))
    app.add_handler(CommandHandler("extend", extend))
    app.add_handler(CommandHandler("revoke", revoke))
    app.add_handler(CommandHandler("expiring", expiring))
    app.add_handler(CallbackQueryHandler(button_click))
    app.add_handler(
        MessageHandler(filters.ChatType.SUPERGROUP & ~filters.COMMAND, relay_group_message),
        group=-1,
    )
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, customer_message))

    LOGGER.info("Bot is running.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
