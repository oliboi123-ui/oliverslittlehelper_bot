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


def load_state() -> dict[str, Any]:
    state_path = get_state_path()
    if not state_path.exists():
        return {"admin_chat_id": None, "users": {}}
    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        LOGGER.warning("State file was invalid JSON, starting fresh.")
        return {"admin_chat_id": None, "users": {}}


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
    }


def get_user_record(state: dict[str, Any], user_id: int) -> dict[str, Any]:
    users = state.setdefault("users", {})
    record = users.setdefault(str(user_id), default_user_record())
    for key, value in default_user_record().items():
        record.setdefault(key, value)
    return record


def user_label(user_id: int, user_data: dict[str, Any]) -> str:
    parts = [
        str(user_data.get("first_name") or "").strip(),
        str(user_data.get("last_name") or "").strip(),
    ]
    full_name = " ".join(part for part in parts if part).strip()
    tg_username = user_data.get("telegram_username")
    if tg_username:
        return f"{full_name or 'Unknown'} (@{tg_username}, id={user_id})"
    return f"{full_name or 'Unknown'} (id={user_id})"


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


def subscription_status_line(record: dict[str, Any]) -> str:
    status = record.get("subscription_status") or "unknown"
    expires_at = record.get("subscription_expires_at")
    if expires_at:
        return f"{status} until {expires_at}"
    return status


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
        if isinstance(exc.reason, TimeoutError):
            raise RuntimeError(f"OFAuth request timed out after {timeout:g}s.") from exc
        if isinstance(exc.reason, socket.timeout):
            raise RuntimeError(f"OFAuth request timed out after {timeout:g}s.") from exc
        raise RuntimeError(f"OFAuth request failed: {exc.reason}") from exc
    try:
        return json.loads(payload)
    except json.JSONDecodeError as exc:
        raise RuntimeError("OFAuth returned invalid JSON.") from exc


def fetch_active_subscribers(limit: int = 100) -> list[dict[str, Any]]:
    subscribers: list[dict[str, Any]] = []
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
            limit,
            timeout,
        )
        payload = ofauth_request_json(
            "/v2/access/subscribers",
            {"type": "active", "limit": limit, "offset": offset},
            timeout_seconds=timeout,
        )
        batch = payload.get("list") or []
        if not isinstance(batch, list):
            raise RuntimeError("OFAuth returned an unexpected subscriber payload.")
        page_fingerprint = json.dumps(
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
        if previous_page_fingerprint is not None and page_fingerprint == previous_page_fingerprint:
            raise RuntimeError(
                "OFAuth returned the same subscriber page twice in a row. "
                "Pagination appears stuck or the offset may be ignored."
            )
        previous_page_fingerprint = page_fingerprint
        LOGGER.info(
            "OFAuth sync received page %s with %s subscribers (hasMore=%s).",
            page_number,
            len(batch),
            bool(payload.get("hasMore")),
        )
        subscribers.extend(batch)
        if not payload.get("hasMore") or not batch:
            break
        offset += len(batch)
    return subscribers


def find_active_subscriber_by_username(claimed_username: str) -> dict[str, Any] | None:
    normalized = normalize_of_username(claimed_username)
    if not normalized:
        return None
    for subscriber in fetch_active_subscribers():
        if normalize_of_username(str(subscriber.get("username") or "")) == normalized:
            return subscriber
    return None


def sync_subscribers(state: dict[str, Any]) -> dict[str, Any]:
    now = utc_now()
    started_at = time.monotonic()
    subscribers = fetch_active_subscribers()
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
    }

    for user_id_text, record in state.get("users", {}).items():
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

        record["last_checked_at"] = summary["checked_at"]

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

        if record.get("status") == "approved":
            record["status"] = "expired"
            summary["expired"] += 1
        record["subscription_status"] = "inactive"
        record["subscription_expires_at"] = None
        summary["inactive"] += 1
        LOGGER.info("Marked user %s inactive after OFAuth sync.", user_id_text)

    summary["duration_seconds"] = round(time.monotonic() - started_at, 2)
    return summary


def format_sync_summary(summary: dict[str, Any]) -> str:
    return (
        "OnlyFans sync complete.\n\n"
        f"Checked at: {summary['checked_at']}\n"
        f"Active subscribers seen: {summary['active_subscribers_seen']}\n"
        f"Matched users: {summary['matched']}\n"
        f"Renewed access: {summary['renewed']}\n"
        f"Expired access: {summary['expired']}\n"
        f"Inactive claims: {summary['inactive']}\n"
        f"Duration: {summary['duration_seconds']}s"
    )


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
            "/pending - show pending requests\n"
            "/approve <user_id> - approve manually\n"
            "/reject <user_id> - reject manually\n"
            "/renew <user_id> - extend access 30 days\n"
            "/status <user_id> - show one user\n"
            "/expiring - show access that is expiring soon\n"
            "/syncsubs - sync active subscribers from OFAuth"
        )
        return

    if is_access_active(record):
        private_username = get_required_env("PRIVATE_TELEGRAM_USERNAME")
        save_state(state)
        await update.message.reply_text(
            f"You are approved until {record['expires_at']}. You can contact me at {private_username}"
        )
        return

    if record.get("status") == "expired":
        record["status"] = "awaiting_of_username"
        save_state(state)
        await update.message.reply_text(
            "Your access period has ended. Please send your OF-username again to request renewed access."
        )
        return

    record["status"] = "awaiting_of_username"
    save_state(state)
    await update.message.reply_text("Please state your OF-username to continue")


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
            "Use /pending, /approve <user_id>, /reject <user_id>, /renew <user_id>, /status <user_id>, /expiring or /syncsubs here."
        )
        state["admin_chat_id"] = admin_chat_id
        save_state(state)
        return

    if is_access_active(record):
        private_username = get_required_env("PRIVATE_TELEGRAM_USERNAME")
        save_state(state)
        await update.message.reply_text(
            f"You are approved until {record['expires_at']}. You can contact me at {private_username}"
        )
        return

    if record["status"] in {"new", "rejected", "expired"}:
        record["status"] = "awaiting_of_username"
        save_state(state)
        await update.message.reply_text("Please state your OF-username to continue")
        return

    if record["status"] != "awaiting_of_username":
        await update.message.reply_text(
            "Your request is already pending review. Please wait for a decision."
        )
        save_state(state)
        return

    claimed_username = update.message.text.strip()
    record["status"] = "pending"
    record["of_username"] = claimed_username
    record["subscription_status"] = "unknown"
    record["subscription_expires_at"] = None
    save_state(state)

    await update.message.reply_text(
        "Thanks. Your request is pending review."
    )

    if not admin_chat_id:
        LOGGER.warning("No admin chat configured yet. Request stored but not delivered.")
        return

    exact_match_note = ""
    if ofauth_is_configured():
        try:
            subscriber = await asyncio.to_thread(find_active_subscriber_by_username, claimed_username)
        except Exception as exc:
            exact_match_note = f"\nOFAuth check: error ({exc})"
        else:
            if subscriber:
                record["subscription_status"] = "active"
                record["onlyfans_user_id"] = subscriber.get("id")
                record["subscription_expires_at"] = subscriber.get("expiredAt")
                save_state(state)
                exact_match_note = (
                    "\nOFAuth check: exact active username match found"
                    f" (expires {subscriber.get('expiredAt') or 'unknown'})"
                )
            else:
                record["subscription_status"] = "inactive"
                save_state(state)
                exact_match_note = "\nOFAuth check: no exact active username match found"

    try:
        await context.bot.forward_message(
            chat_id=admin_chat_id,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
        )
    except Exception:
        LOGGER.exception("Failed to forward applicant message to admin.")

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Approve", callback_data=f"a:{user.id}"),
                InlineKeyboardButton("Reject", callback_data=f"r:{user.id}"),
            ]
        ]
    )

    await context.bot.send_message(
        chat_id=admin_chat_id,
        text=(
            "New gatekeeper request\n\n"
            f"Applicant: {user_label(user.id, record)}\n"
            f"OF username: {record['of_username']}"
            f"{exact_match_note}"
        ),
        reply_markup=keyboard,
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
    if record["status"] != "pending":
        await query.answer("This request is no longer pending.", show_alert=True)
        return

    if action == "a":
        grant_access(record)
        save_state(state)
        private_username = get_required_env("PRIVATE_TELEGRAM_USERNAME")
        await context.bot.send_message(
            chat_id=user_id,
            text=(
                "Approved. You can contact me here: "
                f"{private_username}\n\n"
                f"Access is valid until {record['expires_at']}."
            ),
        )
        await query.edit_message_text(
            text=(
                "Approved\n\n"
                f"Applicant: {user_label(user_id, record)}\n"
                f"OF username: {record['of_username']}\n"
                f"Access until: {record['expires_at']}\n"
                f"Subscriber status: {subscription_status_line(record)}"
            )
        )
        await query.answer("Approved.")
        return

    if action == "r":
        record["status"] = "rejected"
        save_state(state)
        await context.bot.send_message(
            chat_id=user_id,
            text="Sorry, this request was not approved.",
        )
        await query.edit_message_text(
            text=(
                "Rejected\n\n"
                f"Applicant: {user_label(user_id, record)}\n"
                f"OF username: {record['of_username']}"
            )
        )
        await query.answer("Rejected.")
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

    items = []
    for user_id_text, record in state.get("users", {}).items():
        if record.get("status") == "pending":
            items.append(
                f"{user_label(int(user_id_text), record)} | OF={record.get('of_username')} | OFAuth={record.get('subscription_status')}"
            )

    if not items:
        await update.message.reply_text("No pending requests.")
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
                f"{user_label(int(user_id_text), record)} | expires={record.get('expires_at')} | OF={record.get('of_username')}"
            )
        elif record.get("status") == "expired":
            items.append(
                f"{user_label(int(user_id_text), record)} | expired | OF={record.get('of_username')}"
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

    await update.message.reply_text("Running OnlyFans sync...")
    try:
        summary = await asyncio.to_thread(sync_subscribers, state)
    except Exception as exc:
        LOGGER.exception("OFAuth sync failed.")
        await update.message.reply_text(f"OnlyFans sync failed: {exc}")
        return

    save_state(state)
    await update.message.reply_text(format_sync_summary(summary))


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
    await update.message.reply_text(
        f"{user_label(user_id, record)}\n"
        f"Status: {record.get('status')}\n"
        f"OF username: {record.get('of_username')}\n"
        f"Access until: {record.get('expires_at')}\n"
        f"Last checked: {record.get('last_checked_at')}\n"
        f"Subscription: {subscription_status_line(record)}\n"
        f"OnlyFans user id: {record.get('onlyfans_user_id')}"
    )


async def approve_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await manual_decision(update, context, approved=True)


async def reject_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await manual_decision(update, context, approved=False)


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
    await update.message.reply_text(f"Renewed through {record['expires_at']}.")


async def manual_decision(
    update: Update, context: ContextTypes.DEFAULT_TYPE, approved: bool
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
        command_name = "approve" if approved else "reject"
        await update.message.reply_text(f"Usage: /{command_name} <user_id>")
        return

    user_id = int(context.args[0])
    record = get_user_record(state, user_id)
    if record["status"] != "pending":
        await update.message.reply_text("That request is not pending.")
        return

    if approved:
        grant_access(record)
        save_state(state)
        private_username = get_required_env("PRIVATE_TELEGRAM_USERNAME")
        await context.bot.send_message(
            chat_id=user_id,
            text=(
                "Approved. You can contact me here: "
                f"{private_username}\n\n"
                f"Access is valid until {record['expires_at']}."
            ),
        )
        await update.message.reply_text("Approved.")
        return

    record["status"] = "rejected"
    save_state(state)
    await context.bot.send_message(
        chat_id=user_id,
        text="Sorry, this request was not approved.",
    )
    await update.message.reply_text("Rejected.")


async def non_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    state = load_state()
    record = get_user_record(state, update.effective_user.id)
    if record["status"] == "awaiting_of_username":
        await update.message.reply_text("Please send your OF-username as text.")


def main() -> None:
    load_dotenv_file()
    token = get_required_env("BOT_TOKEN")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    app = Application.builder().token(token).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("pending", pending))
    app.add_handler(CommandHandler("approve", approve_manual))
    app.add_handler(CommandHandler("reject", reject_manual))
    app.add_handler(CommandHandler("renew", renew_manual))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("expiring", expiring))
    app.add_handler(CommandHandler("syncsubs", sync_subs))
    app.add_handler(CallbackQueryHandler(button_click))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_message))
    app.add_handler(MessageHandler(~filters.TEXT & ~filters.COMMAND, non_text_message))

    LOGGER.info("Bot is running.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
