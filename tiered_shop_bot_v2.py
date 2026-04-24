from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes


logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    level=logging.INFO,
)
LOGGER = logging.getLogger("tiered_shop_bot_v2")


@dataclass(frozen=True)
class Settings:
    bot_token: str
    admin_user_ids: frozenset[int]
    test_only_mode: bool


def parse_admin_ids(raw_value: str) -> frozenset[int]:
    admin_ids: set[int] = set()
    for part in raw_value.split(","):
        part = part.strip()
        if not part:
            continue
        admin_ids.add(int(part))
    return frozenset(admin_ids)


def load_settings() -> Settings:
    bot_token = os.environ["BOT_TOKEN"].strip()
    admin_ids = parse_admin_ids(os.environ.get("ADMIN_USER_IDS", ""))
    if not admin_ids:
        raise RuntimeError("ADMIN_USER_IDS must contain at least one Telegram user id.")
    test_only_mode = os.environ.get("TEST_ONLY_MODE", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    return Settings(
        bot_token=bot_token,
        admin_user_ids=admin_ids,
        test_only_mode=test_only_mode,
    )


def is_admin(update: Update, settings: Settings) -> bool:
    user = update.effective_user
    return bool(user and user.id in settings.admin_user_ids)


async def require_admin(
    update: Update,
    settings: Settings,
    message: str = "this test bot is private for now.",
) -> bool:
    if is_admin(update, settings):
        return True
    if update.effective_message:
        await update.effective_message.reply_text(message)
    return False


def build_home_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("starter unlock \U0001f5dd\ufe0f", callback_data="menu:starter")],
        [InlineKeyboardButton("plus menu \U0001f48e", callback_data="menu:plus")],
        [InlineKeyboardButton("pro menu \U0001f525", callback_data="menu:pro")],
        [InlineKeyboardButton("my access \U0001f464", callback_data="menu:access")],
        [InlineKeyboardButton("my purchases \U0001f4e6", callback_data="menu:purchases")],
        [InlineKeyboardButton("rules & boundaries \u26a0\ufe0f", callback_data="menu:rules")],
    ]
    return InlineKeyboardMarkup(rows)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    if not await require_admin(update, settings):
        return
    text = (
        "v2 test bot is live.\n\n"
        "this is the clean tiered-shop build lane.\n"
        "the full commerce flow is being rebuilt around:\n"
        "OnlyFans Verified -> Starter -> Plus -> Pro"
    )
    await update.effective_message.reply_text(text, reply_markup=build_home_keyboard())


async def testmode_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    if not await require_admin(update, settings):
        return
    await update.effective_message.reply_text(
        "testmode placeholder: this will seed an approved buyer directly into relay-side buyer testing."
    )


async def testmodefull_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    if not await require_admin(update, settings):
        return
    await update.effective_message.reply_text(
        "testmodefull placeholder: this will start from OnlyFans verification, budget, and approval flow."
    )


async def menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    query = update.callback_query
    if query is None:
        return
    if not is_admin(update, settings):
        await query.answer("private test bot only", show_alert=True)
        return
    await query.answer()
    destination = query.data.partition(":")[2]
    await query.edit_message_text(
        f"menu placeholder: {destination}\n\n"
        "the tiered buyer experience will be built here next.",
        reply_markup=build_home_keyboard(),
    )


def build_application(settings: Settings) -> Application:
    application = Application.builder().token(settings.bot_token).build()
    application.bot_data["settings"] = settings

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("testmode", testmode_command))
    application.add_handler(CommandHandler("testmodefull", testmodefull_command))
    application.add_handler(CallbackQueryHandler(menu_callback, pattern=r"^menu:"))
    return application


def main() -> None:
    settings = load_settings()
    LOGGER.info("starting v2 bot; test_only_mode=%s", settings.test_only_mode)
    asyncio.set_event_loop(asyncio.new_event_loop())
    application = build_application(settings)
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
