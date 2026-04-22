import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any

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
# Keep dependency request logs quiet so the bot token never appears in URLs.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

DATA_DIR = Path(
    os.getenv("BOT_DATA_DIR", "").strip()
    or os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "").strip()
    or Path(__file__).resolve().parent
)
STATE_PATH = DATA_DIR / "bot_state.json"
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


def load_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return {"admin_chat_id": None, "users": {}}
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        LOGGER.warning("State file was invalid JSON, starting fresh.")
        return {"admin_chat_id": None, "users": {}}


def save_state(state: dict[str, Any]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def get_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def normalize_username(value: str) -> str:
    return value.strip().lstrip("@").lower()


def get_user_record(state: dict[str, Any], user_id: int) -> dict[str, Any]:
    users = state.setdefault("users", {})
    return users.setdefault(
        str(user_id),
        {
            "status": "new",
            "of_username": None,
            "telegram_username": None,
            "first_name": None,
            "last_name": None,
        },
    )


def user_label(user_id: int, user_data: dict[str, Any]) -> str:
    parts = [str(user_data.get("first_name") or "").strip(), str(user_data.get("last_name") or "").strip()]
    full_name = " ".join(part for part in parts if part).strip()
    tg_username = user_data.get("telegram_username")
    if tg_username:
        return f"{full_name or 'Unknown'} (@{tg_username}, id={user_id})"
    return f"{full_name or 'Unknown'} (id={user_id})"


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat:
        return
    if update.effective_chat.type != "private":
        return

    state = load_state()
    user = update.effective_user
    record = get_user_record(state, user.id)
    record["telegram_username"] = user.username
    record["first_name"] = user.first_name
    record["last_name"] = user.last_name

    admin_chat_id = resolve_admin_chat_id(state, user)
    if admin_chat_id == update.effective_chat.id:
        state["admin_chat_id"] = admin_chat_id
        save_state(state)
        await update.message.reply_text(
            "Admin chat registered. New requests will be sent here.\n\n"
            "Commands:\n"
            "/pending - show pending requests\n"
            "/approve <user_id> - approve manually\n"
            "/reject <user_id> - reject manually"
        )
        return

    if record["status"] == "approved":
        private_username = get_required_env("PRIVATE_TELEGRAM_USERNAME")
        await update.message.reply_text(
            f"You are already approved. You can contact me at {private_username}"
        )
        save_state(state)
        return

    record["status"] = "awaiting_of_username"
    save_state(state)
    await update.message.reply_text("Please state your OF-username to continue")


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

    admin_chat_id = resolve_admin_chat_id(state, user)
    if admin_chat_id == update.effective_chat.id:
        await update.message.reply_text(
            "Use /pending, /approve <user_id> or /reject <user_id> here."
        )
        state["admin_chat_id"] = admin_chat_id
        save_state(state)
        return

    if record["status"] == "approved":
        private_username = get_required_env("PRIVATE_TELEGRAM_USERNAME")
        await update.message.reply_text(
            f"You are already approved. You can contact me at {private_username}"
        )
        save_state(state)
        return

    if record["status"] in {"new", "rejected"}:
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

    record["status"] = "pending"
    record["of_username"] = update.message.text.strip()
    save_state(state)

    await update.message.reply_text(
        "Thanks. Your request is pending manual review."
    )

    if not admin_chat_id:
        LOGGER.warning("No admin chat configured yet. Request stored but not delivered.")
        return

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
        record["status"] = "approved"
        save_state(state)
        private_username = get_required_env("PRIVATE_TELEGRAM_USERNAME")
        await context.bot.send_message(
            chat_id=user_id,
            text=(
                "Approved. You can contact me here: "
                f"{private_username}"
            ),
        )
        await query.edit_message_text(
            text=(
                "Approved\n\n"
                f"Applicant: {user_label(user_id, record)}\n"
                f"OF username: {record['of_username']}"
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
                f"{user_label(int(user_id_text), record)} | OF={record.get('of_username')}"
            )

    if not items:
        await update.message.reply_text("No pending requests.")
        return

    await update.message.reply_text("\n".join(items[:50]))


async def approve_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await manual_decision(update, context, approved=True)


async def reject_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await manual_decision(update, context, approved=False)


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
        record["status"] = "approved"
        save_state(state)
        private_username = get_required_env("PRIVATE_TELEGRAM_USERNAME")
        await context.bot.send_message(
            chat_id=user_id,
            text=(
                "Approved. You can contact me here: "
                f"{private_username}"
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
    app.add_handler(CallbackQueryHandler(button_click))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_message))
    app.add_handler(MessageHandler(~filters.TEXT & ~filters.COMMAND, non_text_message))

    LOGGER.info("Bot is running.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
