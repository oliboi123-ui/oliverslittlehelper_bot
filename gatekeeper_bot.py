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
from telegram.error import Forbidden, RetryAfter

import v1_migration
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

BROADCAST_AUDIENCES = {
    "customers": "Bought at least once",
    "active": "Access still running",
    "paused": "Bought before, lapsed since",
    "revoked": "Access cut",
    "leads": "Imported, never bought",
    "all": "Everyone, customers and leads",
}

BROADCAST_SEND_DELAY_SECONDS = 0.05
BROADCAST_MAX_MESSAGE_LENGTH = 3500
BROADCAST_PREVIEW_RECIPIENTS = 10

V1_IMPORT_MAX_BYTES = 20 * 1024 * 1024
V1_IMPORT_PREVIEW_ROWS = 15
V1_IMPORT_FILENAME = "v1_import_pending.json"


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
    return {
        "admin_chat_id": None,
        "codes": {},
        "users": {},
        "topics": {},
        "delete_permission_warned": False,
        "broadcast_drafts": {},
        "pending_v1_import": None,
        "pending_removal": None,
    }


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
        "awaiting_fansly_handle": False,
        "last_broadcast_at": None,
        "broadcast_blocked_at": None,
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
    """Telegram writes the per-message "Forwarded from" header itself and won't
    take a custom name, so the Fansly handle lives in the topic title instead,
    where it stays on screen for the whole conversation.
    """
    name = display_name(record)
    handle = str(record.get("fansly_handle") or "").strip()
    return truncate_text(f"{name} ({handle})" if handle else name or "Customer", 120)


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


def get_v1_import_path() -> Path:
    return get_data_dir() / V1_IMPORT_FILENAME


def build_v1_import_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("\u2705 Import customers", callback_data="v1imp:customers")],
            [InlineKeyboardButton("\U0001f465 Import everyone, leads included", callback_data="v1imp:everyone")],
            [InlineKeyboardButton("\u274c Cancel", callback_data="v1imp:cancel")],
        ]
    )


def format_v1_import_preview(plan: dict[str, Any], include_leads: bool) -> str:
    counts = plan["counts"]
    lines = [
        "v1 state file read",
        "",
        f"Records in the file: {plan['source_total']}",
        f"  Approved and still in date: {counts['active']}",
        f"  Lapsed, expired, or revoked: {counts['paused']}",
        f"  Never approved (leads): {counts['lead']}",
        f"  Banned, trashed, rejected, sandbox: {counts['drop']} (never imported)",
    ]
    if plan["skipped_existing"]:
        lines.append(
            f"  Already here: {plan['skipped_existing']}"
            " (access untouched, their old notes refreshed)"
        )
    lines.extend(
        [
            "",
            f"Would import now: {len(plan['planned'])}"
            + (" (leads included)" if include_leads else ""),
            "",
        ]
    )
    for user_id_text, verdict, record in plan["planned"][:V1_IMPORT_PREVIEW_ROWS]:
        target_status = "active" if verdict == "active" else "paused"
        handle = str(record.get("of_username") or "").strip() or "(no handle)"
        lines.append(f"{user_id_text} | {v1_migration.label(record)} | {handle} | -> {target_status}")
    remaining = len(plan["planned"]) - V1_IMPORT_PREVIEW_ROWS
    if remaining > 0:
        lines.append(f"...and {remaining} more.")
    lines.extend(
        [
            "",
            "Nothing is written until you tap a button.",
            "Customers land without an access code and can message you straight away.",
        ]
    )
    return "\n".join(lines)


def format_v1_import_result(result: dict[str, int], plan: dict[str, Any]) -> str:
    return "\n".join(
        [
            "v1 import finished",
            "",
            f"Added: {result['added']}",
            f"Already here, notes refreshed, access untouched: {result['refreshed']}",
            "",
            "Check them with /customers and /leads.",
            "Reach them with /broadcast customers <message>.",
            "",
            "A customer only receives that message if they pressed Start on this",
            "same bot before. Reissuing a token keeps the same bot; a bot made",
            "with /newbot is a different one and reaches nobody until they start it.",
        ]
    )


def find_user_traces(state: dict[str, Any], user_id: int) -> dict[str, Any]:
    """Everything the state file holds about one person."""
    record = state.get("users", {}).get(str(user_id))
    topic_ids = [
        topic_id
        for topic_id, owner in get_topics(state).items()
        if str(owner) == str(user_id)
    ]
    codes = [
        code
        for code, entry in state.get("codes", {}).items()
        if isinstance(entry, dict) and str(entry.get("redeemed_by")) == str(user_id)
    ]
    return {"record": record, "topic_ids": topic_ids, "codes": codes}


def build_removal_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("\U0001f5d1 Delete everything, topic included", callback_data=f"rm:all:{user_id}")],
            [InlineKeyboardButton("\U0001f4c1 Delete record, keep the topic", callback_data=f"rm:record:{user_id}")],
            [InlineKeyboardButton("\u274c Cancel", callback_data=f"rm:cancel:{user_id}")],
        ]
    )


def format_removal_preview(user_id: int, traces: dict[str, Any]) -> str:
    record = traces["record"]
    lines = [
        "Remove this person?",
        "",
        format_customer_details(user_id, record),
        "",
        "This would delete:",
        "  Their customer record",
    ]
    if traces["topic_ids"]:
        lines.append(f"  Their topic mapping ({len(traces['topic_ids'])})")
    if traces["codes"]:
        lines.append(f"  The access code they redeemed ({', '.join(traces['codes'])})")
    lines.extend(
        [
            "",
            "Deleting the topic removes its whole message history from the group.",
            "Keeping it leaves the conversation there, unlinked from anyone.",
            "",
            "This cannot be undone. Nothing happens until you tap a button.",
            "They keep whatever access they already have until you delete them,",
            "and afterwards the bot treats them as a stranger with no code.",
        ]
    )
    return "\n".join(lines)


def get_broadcast_drafts(state: dict[str, Any]) -> dict[str, Any]:
    return state.setdefault("broadcast_drafts", {})


def broadcast_audience_matches(audience: str, record: dict[str, Any]) -> bool:
    status = record.get("status")
    if audience == "all":
        return True
    if audience == "leads":
        return is_v1_lead(record)
    # Leads are stored as paused, so every customer-shaped audience has to
    # exclude them or it quietly picks up people who never bought.
    if audience == "active":
        return is_access_active(record) and not is_v1_lead(record)
    if audience == "paused":
        return status == STATUS_PAUSED and not is_v1_lead(record)
    if audience == "revoked":
        return status == STATUS_REVOKED
    if audience == "customers":
        return bool(record.get("granted_at"))
    return False


def get_broadcast_recipients(
    state: dict[str, Any],
    audience: str,
) -> list[tuple[int, dict[str, Any]]]:
    recipients: list[tuple[int, dict[str, Any]]] = []
    for user_id_text, record in state.get("users", {}).items():
        if not broadcast_audience_matches(audience, record):
            continue
        recipients.append((int(user_id_text), record))
    recipients.sort(key=lambda item: str(item[1].get("granted_at") or ""))
    return recipients


def build_broadcast_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("\u2705 Send broadcast", callback_data="bcast:send"),
                InlineKeyboardButton("\u274c Cancel", callback_data="bcast:cancel"),
            ]
        ]
    )


def format_broadcast_usage(state: dict[str, Any]) -> str:
    lines = [
        "Broadcast",
        "",
        "Sends one message to every customer in an audience, in their private chat with the bot.",
        "",
        "Usage:",
        "/broadcast <audience> <message>",
        "",
        "Audiences:",
    ]
    for key, description in BROADCAST_AUDIENCES.items():
        lines.append(f"{key} - {description} ({len(get_broadcast_recipients(state, key))})")
    lines.extend(
        [
            "",
            "Example:",
            "/broadcast customers The new group is open. Reply here for the invite link.",
            "",
            "leads never bought anything, so keep that message different from the one",
            "your paying customers get. all sends to both at once.",
            "",
            "You get a preview and a confirm button before anything is sent.",
        ]
    )
    return "\n".join(lines)


def format_broadcast_preview(
    audience: str,
    message: str,
    recipients: list[tuple[int, dict[str, Any]]],
) -> str:
    lines = [
        "Broadcast preview",
        "",
        f"Audience: {audience} - {BROADCAST_AUDIENCES.get(audience, 'Unknown')}",
        f"Recipients: {len(recipients)}",
        "",
        "Message customers will see:",
        "",
        message,
        "",
        "Going to:",
    ]
    for user_id, record in recipients[:BROADCAST_PREVIEW_RECIPIENTS]:
        lines.append(f"{user_id} | {person_label(record)}")
    remaining = len(recipients) - BROADCAST_PREVIEW_RECIPIENTS
    if remaining > 0:
        lines.append(f"...and {remaining} more.")
    lines.extend(["", "Nothing is sent until you tap Send broadcast."])
    return "\n".join(lines)


def format_broadcast_summary(summary: dict[str, Any]) -> str:
    return (
        "Broadcast finished\n\n"
        f"Audience: {summary['audience']}\n"
        f"Matched: {summary['recipients']}\n"
        f"Sent: {summary['sent']}\n"
        f"Blocked the bot: {summary['blocked']}\n"
        f"Failed: {summary['failed']}"
    )


async def send_broadcast(
    bot: Any,
    state: dict[str, Any],
    audience: str,
    message: str,
) -> dict[str, Any]:
    recipients = get_broadcast_recipients(state, audience)
    sent = 0
    blocked = 0
    failed = 0
    stamp = to_iso(utc_now())

    for user_id, record in recipients:
        try:
            await bot.send_message(chat_id=user_id, text=message)
        except RetryAfter as exc:
            await asyncio.sleep(float(getattr(exc, "retry_after", 5)) + 1)
            try:
                await bot.send_message(chat_id=user_id, text=message)
            except Forbidden:
                record["broadcast_blocked_at"] = stamp
                blocked += 1
                continue
            except Exception:
                LOGGER.exception("Broadcast retry failed for %s.", user_id)
                failed += 1
                continue
        except Forbidden:
            record["broadcast_blocked_at"] = stamp
            blocked += 1
            continue
        except Exception:
            LOGGER.exception("Broadcast failed for %s.", user_id)
            failed += 1
            continue

        record["last_broadcast_at"] = stamp
        record["broadcast_blocked_at"] = None
        sent += 1
        await asyncio.sleep(BROADCAST_SEND_DELAY_SECONDS)

    return {
        "audience": audience,
        "recipients": len(recipients),
        "sent": sent,
        "blocked": blocked,
        "failed": failed,
    }


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


async def rename_topic(bot: Any, record: dict[str, Any]) -> None:
    """Re-title a topic after the customer's details change."""
    relay_group_id = get_relay_group_id()
    topic_id = record.get("topic_id")
    if relay_group_id is None or not isinstance(topic_id, int):
        return
    topic_name = build_topic_name(record)
    if topic_name == record.get("topic_name"):
        return
    try:
        await bot.edit_forum_topic(
            chat_id=relay_group_id, message_thread_id=topic_id, name=topic_name
        )
    except Exception:
        LOGGER.exception("Could not rename topic %s.", topic_id)
        return
    record["topic_name"] = topic_name


def is_internal_note(message: Any) -> bool:
    text = str(message.text or message.caption or "").strip()
    return text.startswith("//")


# ---------------------------------------------------------------------------
# Customer-facing copy
# ---------------------------------------------------------------------------


# Everything the customer sees is lowercase, so the bot reads the way the
# operator actually types rather than like an automated system.


def welcome_text() -> str:
    return "hey, you made it :)"


def ask_fansly_handle_text() -> str:
    # Sent straight after the welcome, so it doesn't repeat the greeting.
    return "what's your fansly username, so i know who i'm talking to?"


def fansly_handle_saved_text() -> str:
    return "got it, thanks. message me whenever."


def paused_text() -> str:
    return (
        "your access has paused because your subscription isn't showing as active anymore.\n\n"
        "resubscribe and message me here and i'll switch it back on. "
        "you don't need a new link."
    )


def no_code_text() -> str:
    return (
        "this bot is for subscribers only. you'll get a personal access link in a "
        "direct message once you subscribe to the telegram tier.\n\n"
        "already have a code? send it to me here."
    )


def bad_code_text() -> str:
    return "that code isn't valid. check it and try again, or ask me for a new link."


def used_code_text() -> str:
    return "that code has already been used. if that wasn't you, message me and i'll sort it out."


def relay_failed_text() -> str:
    return "i couldn't get that through just now. try again in a moment?"


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
        "/customers - people who actually bought, leads excluded",
        "/leads - imported people who never bought, with budget and request",
        "/who <user_id> - one customer's details",
        "/extend <user_id> [days] - add time",
        "/revoke <user_id> - cut access now, keeping their record",
        "/removeuser <user_id> - delete them from the bot entirely",
        "/expiring - who lapses soon, with buttons",
        "/broadcast <audience> <message> - message every customer in an audience",
        "Send the old bot's bot_state.json here as a file to import its customers",
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


TELEGRAM_MESSAGE_LIMIT = 4096


def chunk_lines(lines: list[str], limit: int = TELEGRAM_MESSAGE_LIMIT) -> list[str]:
    """Group lines into messages Telegram will accept."""
    chunks: list[str] = []
    current: list[str] = []
    length = 0
    for line in lines:
        addition = len(line) + 1
        if current and length + addition > limit:
            chunks.append("\n".join(current))
            current = []
            length = 0
        current.append(line)
        length += addition
    if current:
        chunks.append("\n".join(current))
    return chunks


async def reply_in_chunks(message: Any, lines: list[str]) -> None:
    for chunk in chunk_lines(lines):
        await message.reply_text(chunk)


def v1_notes(record: dict[str, Any]) -> dict[str, Any]:
    notes = record.get("v1")
    return notes if isinstance(notes, dict) else {}


def is_v1_lead(record: dict[str, Any]) -> bool:
    """Came over from v1 having never been approved there."""
    notes = v1_notes(record)
    if not notes:
        return False
    return not notes.get("was_customer")


def format_v1_lines(record: dict[str, Any]) -> list[str]:
    notes = v1_notes(record)
    if not notes:
        return []
    lines = ["", "From the old bot:"]
    lines.append(f"  Their status there: {clean_text(notes.get('status'), empty='Unknown')}")
    lines.append(f"  OnlyFans: {clean_text(notes.get('of_username'))}")
    lines.append(f"  Budget: {clean_text(notes.get('budget_label'), empty='Never answered')}")
    priority = str(notes.get("review_priority") or "").strip()
    if priority:
        lines.append(f"  Priority: {priority}")
    lines.append(f"  Wanted: {clean_text(notes.get('purchase_intent'), empty='Never answered')}")
    payment = str(notes.get("payment_status") or "").strip()
    if payment and payment != "not_requested":
        lines.append(f"  Payment: {payment}")
    subscription = str(notes.get("subscription_status") or "").strip()
    if subscription and subscription != "unknown":
        lines.append(f"  Subscription: {subscription}")
    if notes.get("queued_at"):
        lines.append(f"  First asked: {format_date(notes.get('queued_at'))}")
    if notes.get("approved_at"):
        lines.append(f"  Approved there: {format_date(notes.get('approved_at'))}")
    return lines


def format_lead_line(user_id: int, record: dict[str, Any]) -> str:
    notes = v1_notes(record)
    username = str(record.get("telegram_username") or "").strip()
    parts = [
        str(user_id),
        person_label(record),
        f"@{username}" if username else "no handle",
        clean_text(notes.get("budget_label"), empty="no budget"),
        truncate_text(clean_text(notes.get("purchase_intent"), empty="no request"), 60),
    ]
    return " | ".join(parts)


def format_customer_details(user_id: int, record: dict[str, Any]) -> str:
    username = record.get("telegram_username")
    lines = [
        person_label(record),
        f"ID: {user_id}",
        f"Telegram: @{username}" if username else "Telegram: Not set",
        f"Fansly: {clean_text(record.get('fansly_handle'))}",
        f"Code: {clean_text(record.get('code'))}",
        status_line(record),
        f"First granted: {format_date(record.get('granted_at'))}",
        f"Last extended: {format_date(record.get('last_extended_at'), empty='Never')}",
    ]
    lines.extend(format_v1_lines(record))
    return "\n".join(lines)


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
                welcome_text() if record.get("status") == STATUS_ACTIVE else paused_text()
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
            "you're in, but i couldn't open our chat thread just yet. "
            "message me and i'll pick it up."
        )
        admin_chat_id = state.get("admin_chat_id")
        if admin_chat_id:
            await context.bot.send_message(
                chat_id=int(admin_chat_id),
                text=f"Topic creation failed for {user.id} ({person_label(record)}): {exc}",
            )
        return

    # The handle normally arrives with the code, so this only fires when a code
    # was issued without one.
    if not str(record.get("fansly_handle") or "").strip():
        record["awaiting_fansly_handle"] = True
        save_state(state)
        await update.message.reply_text(welcome_text())
        await update.message.reply_text(ask_fansly_handle_text())
        return

    save_state(state)
    await update.message.reply_text(welcome_text())


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
        await update.message.reply_text(welcome_text())
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
        # Forwarded rather than copied so Telegram labels it with the customer's
        # name. A copy would arrive wearing the bot's identity, which makes the
        # two sides of the conversation indistinguishable in the topic.
        try:
            await context.bot.forward_message(
                chat_id=relay_group_id,
                from_chat_id=update.effective_chat.id,
                message_id=update.message.message_id,
                message_thread_id=topic_id,
            )
        except Exception:
            LOGGER.warning(
                "Could not forward message from user %s, falling back to a copy.", user_id
            )
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

    # Their first reply answers the handle question rather than starting the
    # conversation, so it is captured instead of relayed.
    if record.get("awaiting_fansly_handle"):
        handle = normalize_handle(str(update.message.text or ""))
        if handle:
            record["fansly_handle"] = handle
            record["awaiting_fansly_handle"] = False
            await rename_topic(context.bot, record)
            save_state(state)
            await update.message.reply_text(fansly_handle_saved_text())
            return
        await update.message.reply_text(ask_fansly_handle_text())
        save_state(state)
        return

    paused = record.get("status") == STATUS_PAUSED

    # Paused customers still reach the operator: someone messaging after a lapse
    # is usually about to resubscribe, and the closed topic already flags them.
    delivered = await relay_to_topic(update, context, state, record)

    if paused and should_send_pause_notice(record):
        record["pause_notice_sent_at"] = to_iso(utc_now())
        await update.message.reply_text(paused_text())

    save_state(state)

    if not delivered:
        await update.message.reply_text(relay_failed_text())


async def repost_as_bot(bot: Any, state: dict[str, Any], message: Any) -> None:
    """Replace an operator's message in a topic with an identical bot message.

    Telegram gives no way to post into a group as a bot, so the message is
    copied under the bot's identity and the original is deleted. If the delete
    is refused the copy is removed again, because a duplicate reads worse than
    the original message simply staying put.
    """
    relay_group_id = get_relay_group_id()
    if relay_group_id is None:
        return

    try:
        copy = await bot.copy_message(
            chat_id=relay_group_id,
            from_chat_id=relay_group_id,
            message_id=message.message_id,
            message_thread_id=message.message_thread_id,
        )
    except Exception:
        LOGGER.exception("Could not repost an operator message as the bot.")
        return

    try:
        await bot.delete_message(chat_id=relay_group_id, message_id=message.message_id)
    except Exception:
        LOGGER.warning("Could not delete the operator's original message; removing the copy.")
        try:
            await bot.delete_message(chat_id=relay_group_id, message_id=copy.message_id)
        except Exception:
            LOGGER.exception("Could not remove the duplicate copy either.")
        await warn_missing_delete_permission(bot, state, message.message_thread_id)


async def warn_missing_delete_permission(
    bot: Any, state: dict[str, Any], topic_id: int | None
) -> None:
    """Explain the missing permission once rather than on every message."""
    if state.get("delete_permission_warned"):
        return
    state["delete_permission_warned"] = True
    save_state(state)
    relay_group_id = get_relay_group_id()
    if relay_group_id is None:
        return
    try:
        await bot.send_message(
            chat_id=relay_group_id,
            message_thread_id=topic_id,
            text=(
                "Your messages are being delivered, but I can't repost them under my own "
                "name because I don't have the Delete Messages permission in this group. "
                "Grant it in the group's admin settings and they'll post as me from then on."
            ),
        )
    except Exception:
        LOGGER.exception("Could not warn about the missing delete permission.")


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
        return

    await repost_as_bot(context.bot, state, update.message)


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

    # Imported leads are stored as paused, so they would otherwise be counted
    # here as customers. They never bought anything, so /leads owns them.
    items = []
    lead_count = 0
    for user_id_text, record in state.get("users", {}).items():
        if record.get("status") == STATUS_REVOKED:
            continue
        if is_v1_lead(record):
            lead_count += 1
            continue
        items.append((int(user_id_text), record))

    if not items:
        message = "No customers yet."
        if lead_count:
            message += f"\n\n{lead_count} imported leads are waiting in /leads."
        await update.message.reply_text(message)
        return

    items.sort(key=lambda item: item[1].get("expires_at") or "")
    active = sum(1 for _, record in items if is_access_active(record))
    lines = [
        f"Customers: {len(items)} ({active} with access running)",
        "",
    ]
    lines.extend(format_customer_line(user_id, record) for user_id, record in items)
    lines.append("")
    if lead_count:
        lines.append(f"Plus {lead_count} imported leads who never bought: /leads")
    lines.append("Full detail on one of them: /who <id>")
    await reply_in_chunks(update.message, lines)


async def leads(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Everyone imported from v1 who never became a customer there."""
    state = load_state()
    if not is_admin_chat(state, update) or update.message is None:
        return

    items = [
        (int(user_id_text), record)
        for user_id_text, record in state.get("users", {}).items()
        if is_v1_lead(record)
    ]
    if not items:
        await update.message.reply_text(
            "No leads stored.\n\n"
            "Leads only arrive if you tapped Import everyone when sending the old "
            "bot's state file. Send that file again and tap Import everyone to add them."
        )
        return

    items.sort(key=lambda item: str(v1_notes(item[1]).get("queued_at") or ""))
    lines = [
        f"Leads from the old bot: {len(items)}",
        "id | name | handle | budget | wanted",
        "",
    ]
    lines.extend(format_lead_line(user_id, record) for user_id, record in items)
    lines.extend(["", "Full detail on one of them: /who <id>"])
    await reply_in_chunks(update.message, lines)


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


async def v1_state_upload(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin drops the old bot's bot_state.json into the chat to import it."""
    state = load_state()
    if not is_admin_chat(state, update) or update.message is None or update.effective_user is None:
        return

    document = update.message.document
    if document is None:
        return

    file_size = int(document.file_size or 0)
    if file_size > V1_IMPORT_MAX_BYTES:
        await update.message.reply_text(
            f"That file is {file_size} bytes. Telegram caps bot downloads at "
            f"{V1_IMPORT_MAX_BYTES} bytes, so send it to a machine and run "
            "migrate_v1_state.py there instead."
        )
        return

    try:
        telegram_file = await context.bot.get_file(document.file_id)
        raw = bytes(await telegram_file.download_as_bytearray())
    except Exception:
        LOGGER.exception("Could not download the uploaded v1 state file.")
        await update.message.reply_text("I could not download that file. Try sending it again.")
        return

    try:
        source = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        await update.message.reply_text(
            "That file is not readable JSON. Send the old bot's bot_state.json as a file."
        )
        return

    if not v1_migration.looks_like_v1_state(source):
        await update.message.reply_text(
            "That JSON has no users object, so it is not a v1 bot_state.json."
        )
        return

    plan = v1_migration.plan_migration(source, state.get("users", {}))
    if not plan["planned"] and plan["counts"]["lead"] == 0:
        await update.message.reply_text(
            f"Nothing to import from that file.\n\n{format_v1_import_preview(plan, False)}"
        )
        return

    import_path = get_v1_import_path()
    import_path.parent.mkdir(parents=True, exist_ok=True)
    import_path.write_text(json.dumps(source, ensure_ascii=False), encoding="utf-8")

    state["pending_v1_import"] = {
        "admin_user_id": update.effective_user.id,
        "file_name": document.file_name or "bot_state.json",
        "uploaded_at": to_iso(utc_now()),
    }
    save_state(state)

    await update.message.reply_text(
        format_v1_import_preview(plan, False),
        reply_markup=build_v1_import_keyboard(),
    )


async def run_v1_import(
    query: Any,
    context: ContextTypes.DEFAULT_TYPE,
    state: dict[str, Any],
    *,
    include_leads: bool,
) -> None:
    pending = state.get("pending_v1_import")
    import_path = get_v1_import_path()
    if not pending or not import_path.exists():
        await query.answer("That upload is gone. Send the file again.", show_alert=True)
        return

    try:
        source = json.loads(import_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        LOGGER.exception("Stored v1 import file could not be read.")
        state["pending_v1_import"] = None
        save_state(state)
        await query.answer("The stored upload is unreadable. Send it again.", show_alert=True)
        return

    users = state.setdefault("users", {})
    plan = v1_migration.plan_migration(source, users, include_leads=include_leads)
    result = v1_migration.apply_migration(users, plan)

    state["pending_v1_import"] = None
    save_state(state)
    import_path.unlink(missing_ok=True)

    LOGGER.info(
        "v1 import: added %s, refreshed %s (include_leads=%s).",
        result["added"],
        result["refreshed"],
        include_leads,
    )
    await query.answer(f"Added {result['added']}, refreshed {result['refreshed']}.")
    await query.edit_message_text(format_v1_import_result(result, plan))


async def removeuser(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Delete every trace of one person from the state file."""
    state = load_state()
    if not is_admin_chat(state, update) or update.message is None or update.effective_user is None:
        return

    if not context.args or not context.args[0].lstrip("-").isdigit():
        await update.message.reply_text(
            "Usage: /removeuser <user_id>\n\n"
            "Find the id with /customers, /leads, or /who."
        )
        return

    user_id = int(context.args[0])
    traces = find_user_traces(state, user_id)
    if traces["record"] is None:
        extra = ""
        if traces["topic_ids"] or traces["codes"]:
            extra = (
                "\n\nThere is leftover data under that id "
                f"(topics: {len(traces['topic_ids'])}, codes: {len(traces['codes'])}). "
                "Run the command again to clear it."
            )
            state["pending_removal"] = {
                "user_id": user_id,
                "admin_user_id": update.effective_user.id,
                "requested_at": to_iso(utc_now()),
            }
            save_state(state)
            await update.message.reply_text(
                f"No record for {user_id}.{extra}",
                reply_markup=build_removal_keyboard(user_id),
            )
            return
        await update.message.reply_text(f"Nothing stored under {user_id}.")
        return

    state["pending_removal"] = {
        "user_id": user_id,
        "admin_user_id": update.effective_user.id,
        "requested_at": to_iso(utc_now()),
    }
    save_state(state)
    await update.message.reply_text(
        format_removal_preview(user_id, traces),
        reply_markup=build_removal_keyboard(user_id),
    )


async def run_removal(
    query: Any,
    context: ContextTypes.DEFAULT_TYPE,
    state: dict[str, Any],
    user_id: int,
    *,
    delete_topic: bool,
) -> None:
    pending = state.get("pending_removal") or {}
    if int(pending.get("user_id") or 0) != user_id:
        await query.answer("That removal is no longer pending. Run /removeuser again.", show_alert=True)
        return

    traces = find_user_traces(state, user_id)
    record = traces["record"]
    label = person_label(record) if record else str(user_id)

    removed_topics = 0
    if delete_topic:
        relay_group_id = get_relay_group_id()
        topic_id = record.get("topic_id") if record else None
        if relay_group_id is not None and isinstance(topic_id, int):
            try:
                await context.bot.delete_forum_topic(
                    chat_id=relay_group_id, message_thread_id=topic_id
                )
                removed_topics = 1
            except Exception:
                LOGGER.exception("Could not delete topic %s for user %s.", topic_id, user_id)

    state.get("users", {}).pop(str(user_id), None)

    topics = get_topics(state)
    for topic_id_text in traces["topic_ids"]:
        topics.pop(topic_id_text, None)

    codes = state.get("codes", {})
    for code in traces["codes"]:
        codes.pop(code, None)

    state["pending_removal"] = None
    save_state(state)

    LOGGER.info(
        "Removed user %s (record=%s, topics=%s, codes=%s, topic_deleted=%s).",
        user_id,
        record is not None,
        len(traces["topic_ids"]),
        len(traces["codes"]),
        bool(removed_topics),
    )

    lines = [
        f"{label} removed.",
        "",
        f"Record deleted: {'yes' if record is not None else 'none was stored'}",
        f"Topic mappings cleared: {len(traces['topic_ids'])}",
        f"Access codes deleted: {len(traces['codes'])}",
    ]
    if delete_topic:
        lines.append(
            "Forum topic deleted: yes"
            if removed_topics
            else "Forum topic deleted: no (none linked, or the group refused)"
        )
    else:
        lines.append("Forum topic: left in place")
    lines.extend(
        [
            "",
            "If they message the bot again they are treated as a stranger with no code.",
        ]
    )
    await query.answer("Removed.")
    await query.edit_message_text("\n".join(lines))


async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    state = load_state()
    if not is_admin_chat(state, update) or update.message is None or update.effective_user is None:
        return

    command_parts = (update.message.text or "").split(None, 1)
    remainder = command_parts[1] if len(command_parts) > 1 else ""
    audience_parts = remainder.split(None, 1)
    audience = audience_parts[0].strip().lower() if audience_parts else ""
    message = audience_parts[1].strip() if len(audience_parts) > 1 else ""

    if audience not in BROADCAST_AUDIENCES:
        await update.message.reply_text(format_broadcast_usage(state))
        return

    if not message:
        await update.message.reply_text(
            f"Add the message after the audience.\n\n"
            f"/broadcast {audience} The new group is open. Reply here for the invite link."
        )
        return

    if len(message) > BROADCAST_MAX_MESSAGE_LENGTH:
        await update.message.reply_text(
            f"That message is {len(message)} characters. "
            f"Keep it under {BROADCAST_MAX_MESSAGE_LENGTH}."
        )
        return

    recipients = get_broadcast_recipients(state, audience)
    if not recipients:
        await update.message.reply_text(f"Nobody matches the {audience} audience right now.")
        return

    get_broadcast_drafts(state)[str(update.effective_user.id)] = {
        "audience": audience,
        "message": message,
        "created_at": to_iso(utc_now()),
    }
    save_state(state)
    await update.message.reply_text(
        format_broadcast_preview(audience, message, recipients),
        reply_markup=build_broadcast_keyboard(),
    )


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

    data = query.data or ""

    if data.startswith("rm:"):
        parts = data.split(":")
        if len(parts) != 3 or not parts[2].lstrip("-").isdigit():
            await query.answer("That removal button is outdated.", show_alert=True)
            return
        removal_action = parts[1]
        target_user_id = int(parts[2])

        if removal_action == "cancel":
            state["pending_removal"] = None
            save_state(state)
            await query.edit_message_text("Removal cancelled. Nothing was deleted.")
            await query.answer("Cancelled.")
            return

        if removal_action not in {"all", "record"}:
            await query.answer("Unknown removal action.", show_alert=True)
            return

        await run_removal(
            query,
            context,
            state,
            target_user_id,
            delete_topic=removal_action == "all",
        )
        return

    if data.startswith("v1imp:"):
        import_action = data.partition(":")[2]

        if import_action == "cancel":
            state["pending_v1_import"] = None
            save_state(state)
            get_v1_import_path().unlink(missing_ok=True)
            await query.edit_message_text("Import cancelled. Nothing was written.")
            await query.answer("Cancelled.")
            return

        if import_action not in {"customers", "everyone"}:
            await query.answer("Unknown import action.", show_alert=True)
            return

        await run_v1_import(query, context, state, include_leads=import_action == "everyone")
        return

    if data.startswith("bcast:"):
        drafts = get_broadcast_drafts(state)
        draft_key = str(query.from_user.id)
        bcast_action = data.partition(":")[2]

        if bcast_action == "cancel":
            drafts.pop(draft_key, None)
            save_state(state)
            await query.edit_message_text("Broadcast cancelled. Nothing was sent.")
            await query.answer("Cancelled.")
            return

        if bcast_action != "send":
            await query.answer("Unknown broadcast action.", show_alert=True)
            return

        draft = drafts.pop(draft_key, None)
        if draft is None:
            await query.answer(
                "That broadcast draft is gone. Run /broadcast again.",
                show_alert=True,
            )
            return

        save_state(state)
        await query.answer("Sending.")
        await query.edit_message_text("Sending broadcast...")

        summary = await send_broadcast(
            context.bot,
            state,
            str(draft.get("audience") or ""),
            str(draft.get("message") or ""),
        )
        save_state(state)
        await context.bot.send_message(
            chat_id=admin_chat_id,
            text=format_broadcast_summary(summary),
        )
        return

    action, _, user_id_text = data.partition(":")
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
    app.add_handler(CommandHandler("leads", leads))
    app.add_handler(CommandHandler("who", who))
    app.add_handler(CommandHandler("extend", extend))
    app.add_handler(CommandHandler("revoke", revoke))
    app.add_handler(CommandHandler("removeuser", removeuser))
    app.add_handler(CommandHandler("expiring", expiring))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CallbackQueryHandler(button_click))
    app.add_handler(
        MessageHandler(filters.ChatType.SUPERGROUP & ~filters.COMMAND, relay_group_message),
        group=-1,
    )
    app.add_handler(
        MessageHandler(filters.ChatType.PRIVATE & filters.Document.ALL, v1_state_upload)
    )
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, customer_message))

    LOGGER.info("Bot is running.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
