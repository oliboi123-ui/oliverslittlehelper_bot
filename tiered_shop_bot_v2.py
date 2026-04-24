from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
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
LOGGER = logging.getLogger("tiered_shop_bot_v2")

TEST_SESSION_ID_OFFSET = 9_000_000_000_000_000
TIER_VERIFIED = "onlyfans_verified"
TIER_STARTER = "starter"
TIER_PLUS = "plus"
TIER_PRO = "pro"

TIER_LABELS = {
    TIER_VERIFIED: "OnlyFans Verified",
    TIER_STARTER: "Starter",
    TIER_PLUS: "Plus",
    TIER_PRO: "Pro",
}

TIER_ORDER = {
    TIER_VERIFIED: 0,
    TIER_STARTER: 1,
    TIER_PLUS: 2,
    TIER_PRO: 3,
}

BUDGET_OPTIONS = [
    {"key": "under_50", "label": "Under $50"},
    {"key": "50_99", "label": "$50-$99"},
    {"key": "100_199", "label": "$100-$199"},
    {"key": "200_249", "label": "$200-$249"},
    {"key": "250_499", "label": "$250-$499"},
    {"key": "500_plus", "label": "$500+"},
]

CATALOG: dict[str, dict[str, Any]] = {
    "starter_unlock": {
        "title": "starter unlock",
        "price_cents": 3700,
        "kind": "starter_unlock",
        "delivery_mode": "instant",
        "button_label": "unlock starter \U0001f5dd\ufe0f",
    },
    "plus_bundle": {
        "title": "best value bundle",
        "price_cents": 9700,
        "kind": "plus_bundle",
        "delivery_mode": "instant",
        "button_label": "unlock bundle \U0001f48e",
    },
    "plus_ppv_stroking": {
        "title": "stroking ppv",
        "price_cents": 6700,
        "kind": "plus_ppv",
        "line_key": "stroking",
        "delivery_mode": "instant",
        "button_label": "stroking ppv \U0001f525",
    },
    "plus_ppv_ass": {
        "title": "ass ppv",
        "price_cents": 6700,
        "kind": "plus_ppv",
        "line_key": "ass_noboxers",
        "delivery_mode": "instant",
        "button_label": "ass ppv \U0001f351",
    },
    "plus_ppv_strip": {
        "title": "strip ppv",
        "price_cents": 6700,
        "kind": "plus_ppv",
        "line_key": "strip",
        "delivery_mode": "instant",
        "button_label": "strip ppv \U0001f455",
    },
    "pro_video": {
        "title": "5-10 min video",
        "price_cents": 17000,
        "kind": "pro_manual",
        "delivery_mode": "manual",
        "button_label": "unlock video \U0001f4a6",
    },
    "pro_voice_note": {
        "title": "voice note",
        "price_cents": 8000,
        "kind": "pro_manual",
        "delivery_mode": "manual",
        "button_label": "unlock voice note \U0001f399\ufe0f",
    },
}

PERSONAS = [
    ("Noah Vale", "nvale"),
    ("Milo Hart", "mhart"),
    ("Alec North", "anorth"),
    ("Luca Stone", "lstone"),
    ("Evan Shore", "eshore"),
    ("Theo Vale", "tvale"),
]


@dataclass(frozen=True)
class Settings:
    bot_token: str
    admin_user_ids: frozenset[int]
    relay_admin_group_id: int | None
    payment_url: str | None
    test_only_mode: bool
    state_path: Path


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(value: datetime | None = None) -> str | None:
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
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def format_dt(value: str | None) -> str:
    parsed = parse_iso(value)
    if parsed is None:
        return "unknown"
    return parsed.astimezone().strftime("%d %b %Y, %H:%M")


def money_text(cents: int) -> str:
    return f"${cents / 100:.2f}"


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
    relay_group_raw = os.environ.get("RELAY_ADMIN_GROUP_ID", "").strip()
    relay_group_id = int(relay_group_raw) if relay_group_raw else None
    payment_url = os.environ.get("PAYMENT_URL", "").strip() or None
    test_only_mode = os.environ.get("TEST_ONLY_MODE", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    state_path_raw = os.environ.get("STATE_PATH", "data/v2_bot_state.json").strip()
    state_path = Path(state_path_raw)
    if not state_path.is_absolute():
        state_path = Path(__file__).resolve().parent / state_path
    return Settings(
        bot_token=bot_token,
        admin_user_ids=admin_ids,
        relay_admin_group_id=relay_group_id,
        payment_url=payment_url,
        test_only_mode=test_only_mode,
        state_path=state_path,
    )


def load_state(settings: Settings) -> dict[str, Any]:
    if not settings.state_path.exists():
        return {
            "users": {},
            "test_sessions": {},
            "vault_chat_id": None,
            "vault_lines": {},
        }
    try:
        state = json.loads(settings.state_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        LOGGER.warning("State file is invalid JSON. Starting with a new state.")
        return {
            "users": {},
            "test_sessions": {},
            "vault_chat_id": None,
            "vault_lines": {},
        }
    state.setdefault("users", {})
    state.setdefault("test_sessions", {})
    state.setdefault("vault_chat_id", None)
    state.setdefault("vault_lines", {})
    return state


def save_state(settings: Settings, state: dict[str, Any]) -> None:
    settings.state_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = settings.state_path.with_suffix(".tmp")
    temp_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(settings.state_path)


def is_admin_user_id(user_id: int, settings: Settings) -> bool:
    return user_id in settings.admin_user_ids


def is_admin_update(update: Update, settings: Settings) -> bool:
    user = update.effective_user
    return bool(user and is_admin_user_id(user.id, settings))


def make_test_user_id(owner_user_id: int) -> int:
    return TEST_SESSION_ID_OFFSET + abs(owner_user_id)


def active_test_user_id(state: dict[str, Any], owner_user_id: int) -> int | None:
    raw_value = state.get("test_sessions", {}).get(str(owner_user_id))
    if raw_value is None:
        return None
    return int(raw_value)


def persona_for(owner_user_id: int) -> tuple[str, str]:
    return PERSONAS[abs(owner_user_id) % len(PERSONAS)]


def tier_rank(tier: str | None) -> int:
    if not tier:
        return -1
    return TIER_ORDER.get(tier, -1)


def tier_label(tier: str | None) -> str:
    if not tier:
        return "Not set"
    return TIER_LABELS.get(tier, tier)


def new_record(
    synthetic_user_id: int,
    owner_user_id: int,
    owner_chat_id: int,
    *,
    mode: str,
) -> dict[str, Any]:
    display_name, of_username = persona_for(owner_user_id)
    record = {
        "user_id": synthetic_user_id,
        "test_mode": True,
        "test_mode_owner_user_id": owner_user_id,
        "buyer_chat_id": owner_chat_id,
        "mode": mode,
        "display_name": display_name,
        "telegram_username": None,
        "of_username": of_username if mode == "seeded" else None,
        "budget_key": None,
        "budget_label": None,
        "purchase_intent": None,
        "review_status": "approved" if mode == "seeded" else "draft",
        "tier": TIER_VERIFIED if mode == "seeded" else None,
        "intake_state": "buyer_active" if mode == "seeded" else "awaiting_of_username",
        "topic_id": None,
        "topic_name": None,
        "flagged": False,
        "notes": [],
        "purchases": [],
        "delivery_counters": {},
        "created_at": to_iso(utc_now()),
        "updated_at": to_iso(utc_now()),
    }
    return record


def get_user_record(state: dict[str, Any], user_id: int) -> dict[str, Any]:
    users = state.setdefault("users", {})
    key = str(user_id)
    if key not in users:
        users[key] = {
            "user_id": user_id,
            "test_mode": False,
            "buyer_chat_id": user_id,
            "display_name": "Unknown",
            "telegram_username": None,
            "of_username": None,
            "budget_key": None,
            "budget_label": None,
            "purchase_intent": None,
            "review_status": "draft",
            "tier": None,
            "intake_state": "awaiting_of_username",
            "topic_id": None,
            "topic_name": None,
            "flagged": False,
            "notes": [],
            "purchases": [],
            "delivery_counters": {},
            "created_at": to_iso(utc_now()),
            "updated_at": to_iso(utc_now()),
        }
    return users[key]


def get_active_private_record(state: dict[str, Any], user_id: int) -> dict[str, Any] | None:
    synthetic_user_id = active_test_user_id(state, user_id)
    if synthetic_user_id is None:
        return None
    return state.get("users", {}).get(str(synthetic_user_id))


def sync_record_identity(record: dict[str, Any], user: Any, chat_id: int) -> None:
    record["telegram_username"] = user.username
    record["buyer_chat_id"] = chat_id
    record["updated_at"] = to_iso(utc_now())


def budget_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(option["label"], callback_data=f"budget:{option['key']}")]
        for option in BUDGET_OPTIONS
    ]
    return InlineKeyboardMarkup(rows)


def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("starter unlock \U0001f5dd\ufe0f", callback_data="menu:starter")],
            [InlineKeyboardButton("plus menu \U0001f48e", callback_data="menu:plus")],
            [InlineKeyboardButton("pro menu \U0001f525", callback_data="menu:pro")],
            [InlineKeyboardButton("my access \U0001f464", callback_data="menu:access")],
            [InlineKeyboardButton("my purchases \U0001f4e6", callback_data="menu:purchases")],
            [InlineKeyboardButton("rules & boundaries \u26a0\ufe0f", callback_data="menu:rules")],
        ]
    )


def simple_back_keyboard(target: str = "home") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("back", callback_data=f"nav:{target}")]])


def starter_keyboard(record: dict[str, Any]) -> InlineKeyboardMarkup:
    buttons = []
    if tier_rank(record.get("tier")) < tier_rank(TIER_STARTER):
        buttons.append([InlineKeyboardButton(CATALOG["starter_unlock"]["button_label"], callback_data="buy:starter_unlock")])
    buttons.append([InlineKeyboardButton("what comes next \U0001f440", callback_data="menu:starter_next")])
    buttons.append([InlineKeyboardButton("view my access \U0001f464", callback_data="menu:access")])
    buttons.append([InlineKeyboardButton("back", callback_data="nav:home")])
    return InlineKeyboardMarkup(buttons)


def plus_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("best value bundle \U0001f48e", callback_data="menu:plus_bundle")],
            [InlineKeyboardButton("premium ppvs \U0001f525", callback_data="menu:plus_ppvs")],
            [InlineKeyboardButton("back", callback_data="nav:home")],
        ]
    )


def plus_bundle_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(CATALOG["plus_bundle"]["button_label"], callback_data="buy:plus_bundle")],
            [InlineKeyboardButton("back", callback_data="menu:plus")],
        ]
    )


def plus_ppv_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(CATALOG["plus_ppv_stroking"]["button_label"], callback_data="buy:plus_ppv_stroking")],
            [InlineKeyboardButton(CATALOG["plus_ppv_ass"]["button_label"], callback_data="buy:plus_ppv_ass")],
            [InlineKeyboardButton(CATALOG["plus_ppv_strip"]["button_label"], callback_data="buy:plus_ppv_strip")],
            [InlineKeyboardButton("back", callback_data="menu:plus")],
        ]
    )


def pro_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("5-10 min video \U0001f4a6", callback_data="menu:pro_video")],
            [InlineKeyboardButton("voice note \U0001f399\ufe0f", callback_data="menu:pro_voice")],
            [InlineKeyboardButton("chat access \U0001f4ac", callback_data="menu:pro_chat")],
            [InlineKeyboardButton("my purchases \U0001f4e6", callback_data="menu:purchases")],
            [InlineKeyboardButton("back", callback_data="nav:home")],
        ]
    )


def pro_video_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(CATALOG["pro_video"]["button_label"], callback_data="buy:pro_video")],
            [InlineKeyboardButton("back", callback_data="menu:pro")],
        ]
    )


def pro_voice_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(CATALOG["pro_voice_note"]["button_label"], callback_data="buy:pro_voice_note")],
            [InlineKeyboardButton("back", callback_data="menu:pro")],
        ]
    )


def payment_keyboard(settings: Settings, purchase_id: str) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if settings.payment_url:
        rows.append([InlineKeyboardButton("open paypal", url=settings.payment_url)])
    rows.append([InlineKeyboardButton("i've paid \u2705", callback_data=f"paid:{purchase_id}")])
    rows.append([InlineKeyboardButton("my purchases \U0001f4e6", callback_data="menu:purchases")])
    rows.append([InlineKeyboardButton("back", callback_data="nav:home")])
    return InlineKeyboardMarkup(rows)


def admin_review_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("OnlyFans Verified \u2705", callback_data=f"adm:tier:{user_id}:{TIER_VERIFIED}"),
                InlineKeyboardButton("Starter \U0001f5dd\ufe0f", callback_data=f"adm:tier:{user_id}:{TIER_STARTER}"),
            ],
            [
                InlineKeyboardButton("Plus \U0001f48e", callback_data=f"adm:tier:{user_id}:{TIER_PLUS}"),
                InlineKeyboardButton("Pro \U0001f525", callback_data=f"adm:tier:{user_id}:{TIER_PRO}"),
            ],
            [
                InlineKeyboardButton("reject", callback_data=f"adm:reject:{user_id}"),
                InlineKeyboardButton("flag", callback_data=f"adm:flag:{user_id}"),
            ],
            [InlineKeyboardButton("summary", callback_data=f"adm:summary:{user_id}")],
        ]
    )


def admin_payment_keyboard(user_id: int, purchase_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("mark paid \u2705", callback_data=f"adm:pay:{user_id}:{purchase_id}"),
                InlineKeyboardButton("reject payment", callback_data=f"adm:deny:{user_id}:{purchase_id}"),
            ],
            [InlineKeyboardButton("summary", callback_data=f"adm:summary:{user_id}")],
        ]
    )


def admin_manual_fulfillment_keyboard(user_id: int, purchase_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("mark fulfilled \u2705", callback_data=f"adm:fulfill:{user_id}:{purchase_id}")],
            [InlineKeyboardButton("summary", callback_data=f"adm:summary:{user_id}")],
        ]
    )


def get_budget_label(key: str | None) -> str:
    for option in BUDGET_OPTIONS:
        if option["key"] == key:
            return option["label"]
    return "Not set"


def current_home_text(record: dict[str, Any]) -> str:
    tier = record.get("tier")
    if tier == TIER_VERIFIED:
        return (
            "you’re verified \u2705\n"
            "you’re inside the private menu now.\n\n"
            "start with Starter to open the first unlock.\n"
            "Plus and Pro are visible too, but they open as you move through the tiers. "
            "as you progress, PPV-prices decrease by 25% per tier. buy more, save more :D"
        )
    if tier == TIER_STARTER:
        return (
            "you’re in Starter \U0001f5dd\ufe0f\n\n"
            "the first private unlock is open.\n"
            "Plus comes next with bundles and individual PPVs.\n"
            "Pro stays visible too, so you can see where the path opens next."
        )
    if tier == TIER_PLUS:
        return (
            "you’re in Plus \U0001f48e\n\n"
            "this is the mid-tier layer.\n"
            "you can unlock bundles and Premium PPVs here.\n"
            "one more Plus purchase opens Pro."
        )
    if tier == TIER_PRO:
        return (
            "you’re in Pro \U0001f525\n\n"
            "you have access to the most personal unlocks now.\n"
            "chat access is open here too, as long as things stay respectful and easygoing."
        )
    return (
        "welcome.\n\n"
        "this private shop opens step by step.\n"
        "use /testmode or /testmodefull from the admin side to enter the buyer flow."
    )


def access_text(record: dict[str, Any]) -> str:
    tier = record.get("tier")
    if tier == TIER_VERIFIED:
        return (
            "your access \U0001f464\n\n"
            "current access: OnlyFans Verified\n\n"
            "Starter is your first private unlock.\n"
            "Plus and Pro are visible, but locked for now. PPVs become cheaper as you progress."
        )
    if tier == TIER_STARTER:
        return (
            "your access \U0001f464\n\n"
            "current access: Starter\n\n"
            "you’ve opened the first tier.\n"
            "Plus comes next with bundles and individual PPVs.\n"
            "Pro opens after Plus."
        )
    if tier == TIER_PLUS:
        return (
            "your access \U0001f464\n\n"
            "current access: Plus\n\n"
            "you can unlock Plus items now.\n"
            "any Plus purchase opens Pro."
        )
    if tier == TIER_PRO:
        return (
            "your access \U0001f464\n\n"
            "current access: Pro\n\n"
            "you have access to Pro products and chat."
        )
    return "your access \U0001f464\n\nno active access yet."


def rules_text() -> str:
    return (
        "rules & boundaries \u26a0\ufe0f\n\n"
        "this is a structured private shop.\n"
        "purchases are delivered through the bot.\n"
        "chatting is only available at Pro.\n"
        "be respectful and easygoing.\n"
        "boundaries apply at every tier."
    )


def tier_locked_text(target: str, current_tier: str | None) -> str:
    if target == "plus":
        return "Plus is locked for now \U0001f512\nstart with Starter first, or wait for manual approval if you’ve bought before."
    if target == "pro" and current_tier == TIER_STARTER:
        return "this is a Pro unlock \U0001f512\nPro opens after a Plus purchase."
    if target == "pro" and current_tier == TIER_PLUS:
        return "this opens at Pro \U0001f512\ncomplete any Plus unlock to open Pro access."
    if target == "chat":
        return "chat opens at Pro \U0001f4ac\U0001f512\n\nchatting isn’t included yet.\nit unlocks once you reach Pro."
    return "this area is locked right now."


def what_comes_next_text() -> str:
    return (
        "what comes next \U0001f440\n\n"
        "Plus opens the next layer: bundles and individual PPVs.\n"
        "Pro is the highest tier, with the most personal products and chat access.\n\n"
        "you’ll see more as your access grows."
    )


def starter_text(record: dict[str, Any]) -> str:
    if tier_rank(record.get("tier")) >= tier_rank(TIER_STARTER):
        return (
            "starter unlock \U0001f5dd\ufe0f\n\n"
            "you’ve already opened Starter.\n"
            "it stays part of your path, but the next real step is Plus."
        )
    return (
        "starter unlock \U0001f5dd\ufe0f\n\n"
        "a first private unlock before the higher tiers.\n"
        "you’ll get 3 randomly selected dickpics from the vault. 2 hard, 1 soft. expect abs :)"
    )


def plus_text(record: dict[str, Any]) -> str:
    ppv_price = money_text(display_price_cents(record, "plus_ppv_stroking"))
    bundle_price = money_text(display_price_cents(record, "plus_bundle"))
    return (
        "plus menu \U0001f48e\n\n"
        "this is the mid-tier layer.\n"
        "best value bundle comes first, then Premium PPVs.\n\n"
        f"best value bundle: {bundle_price}\n"
        f"Premium PPVs: {ppv_price} each"
    )


def plus_bundle_text(record: dict[str, Any]) -> str:
    price = money_text(display_price_cents(record, "plus_bundle"))
    return (
        "Best Value Bundle \U0001f48e\n\n"
        "the strongest Plus option.\n"
        "includes 1 stroking video and 1 strip tease.\n\n"
        "simple bundle, better value, instant unlock.\n\n"
        f"price: {price}"
    )


def plus_ppv_text(record: dict[str, Any]) -> str:
    price = money_text(display_price_cents(record, "plus_ppv_stroking"))
    if tier_rank(record.get("tier")) >= tier_rank(TIER_PRO):
        suffix = "\n\nas Pro, your Plus PPVs are 25% cheaper."
    else:
        suffix = ""
    return (
        "Premium PPVs \U0001f525\n\n"
        "individual Plus unlocks.\n"
        "pick the type you want and unlock it instantly.\n\n"
        f"current price: {price} each{suffix}"
    )


def pro_text() -> str:
    return (
        "pro menu \U0001f525\n\n"
        "this is the highest tier.\n"
        "you can unlock the most personal items here, and chat access opens at this level."
    )


def pro_video_text(record: dict[str, Any]) -> str:
    return (
        "Pro unlock \U0001f4a6\n\n"
        "5-10 min jerkoff + cum video.\n"
        "clear, direct, and made for Pro.\n\n"
        f"price: {money_text(display_price_cents(record, 'pro_video'))}"
    )


def pro_voice_text(record: dict[str, Any]) -> str:
    return (
        "personal voice note \U0001f399\ufe0f\n\n"
        "up to around 1 minute, depending on the topic.\n"
        "available only for Pro members.\n\n"
        f"price: {money_text(display_price_cents(record, 'pro_voice_note'))}"
    )


def pro_chat_text() -> str:
    return (
        "Pro chat access \U0001f4ac\n\n"
        "since you’ve reached Pro, you can chat directly.\n"
        "keep it respectful and easygoing."
    )


def purchase_lines(record: dict[str, Any]) -> list[str]:
    lines = [f"access tier: {tier_label(record.get('tier'))}"]
    purchases = record.get("purchases", [])
    if not purchases:
        lines.append("no purchases yet.")
        return lines
    for purchase in reversed(purchases[-12:]):
        status = purchase.get("status", "unknown")
        title = purchase.get("title", "item")
        amount = money_text(int(purchase.get("price_cents", 0)))
        requested_at = format_dt(purchase.get("requested_at"))
        lines.append(f"- {title} | {amount} | {status} | {requested_at}")
    return lines


def purchases_text(record: dict[str, Any]) -> str:
    return "my purchases \U0001f4e6\n\n" + "\n".join(purchase_lines(record))


def format_admin_review_card(record: dict[str, Any], title: str) -> str:
    return (
        f"{title}\n\n"
        f"{record.get('display_name', 'Unknown')}\n"
        f"user id: {record['user_id']}\n"
        f"OnlyFans: {record.get('of_username') or 'Not set'}\n"
        f"budget: {record.get('budget_label') or 'Not set'}\n"
        f"looking for: {record.get('purchase_intent') or 'Not set'}\n"
        f"tier: {tier_label(record.get('tier'))}\n"
        f"flagged: {'yes' if record.get('flagged') else 'no'}"
    )


def format_admin_purchase_card(record: dict[str, Any], purchase: dict[str, Any], title: str) -> str:
    return (
        f"{title}\n\n"
        f"{record.get('display_name', 'Unknown')}\n"
        f"user id: {record['user_id']}\n"
        f"tier: {tier_label(record.get('tier'))}\n"
        f"item: {purchase.get('title')}\n"
        f"amount: {money_text(int(purchase.get('price_cents', 0)))}\n"
        f"status: {purchase.get('status')}\n"
        f"requested: {format_dt(purchase.get('requested_at'))}"
    )


def display_price_cents(record: dict[str, Any], sku: str) -> int:
    product = CATALOG[sku]
    price = int(product["price_cents"])
    tier = record.get("tier")
    if tier == TIER_PRO and product["kind"] == "plus_ppv":
        return int(round(price * 0.75))
    return price


def can_open_plus(record: dict[str, Any]) -> bool:
    return tier_rank(record.get("tier")) >= tier_rank(TIER_STARTER)


def can_open_pro(record: dict[str, Any]) -> bool:
    return tier_rank(record.get("tier")) >= tier_rank(TIER_PRO)


def find_purchase(record: dict[str, Any], purchase_id: str) -> dict[str, Any] | None:
    for purchase in record.get("purchases", []):
        if purchase.get("purchase_id") == purchase_id:
            return purchase
    return None


def create_purchase(record: dict[str, Any], sku: str) -> dict[str, Any]:
    purchase_id = uuid.uuid4().hex[:10]
    price_cents = display_price_cents(record, sku)
    product = CATALOG[sku]
    purchase = {
        "purchase_id": purchase_id,
        "sku": sku,
        "title": product["title"],
        "price_cents": price_cents,
        "status": "awaiting_payment",
        "requested_at": to_iso(utc_now()),
        "paid_at": None,
        "fulfilled_at": None,
        "delivery_summary": None,
    }
    record.setdefault("purchases", []).append(purchase)
    record["updated_at"] = to_iso(utc_now())
    return purchase


def apply_tier_progression(record: dict[str, Any], sku: str) -> str | None:
    current_tier = record.get("tier")
    new_tier = None
    if sku == "starter_unlock" and tier_rank(current_tier) < tier_rank(TIER_STARTER):
        new_tier = TIER_STARTER
    elif sku == "plus_bundle":
        if tier_rank(current_tier) < tier_rank(TIER_PRO):
            new_tier = TIER_PRO
    elif sku.startswith("plus_ppv_"):
        if current_tier == TIER_STARTER:
            new_tier = TIER_PLUS
        elif current_tier == TIER_PLUS:
            new_tier = TIER_PRO
    elif sku.startswith("pro_") and tier_rank(current_tier) < tier_rank(TIER_PRO):
        new_tier = TIER_PRO
    if new_tier and new_tier != current_tier:
        record["tier"] = new_tier
        record["review_status"] = "approved"
        record["intake_state"] = "buyer_active"
        record["updated_at"] = to_iso(utc_now())
    return new_tier


def delivery_items_for_line(record: dict[str, Any], state: dict[str, Any], line_key: str, amount: int) -> list[dict[str, Any]]:
    line_items = state.get("vault_lines", {}).get(line_key, [])
    if len(line_items) < amount:
        raise RuntimeError(f"line '{line_key}' needs at least {amount} item(s), has {len(line_items)}.")
    counters = record.setdefault("delivery_counters", {})
    current_index = int(counters.get(line_key, 0))
    chosen: list[dict[str, Any]] = []
    for offset in range(amount):
        chosen.append(line_items[(current_index + offset) % len(line_items)])
    counters[line_key] = current_index + amount
    return chosen


def random_items_from_line(state: dict[str, Any], line_key: str, amount: int) -> list[dict[str, Any]]:
    line_items = state.get("vault_lines", {}).get(line_key, [])
    if len(line_items) < amount:
        raise RuntimeError(f"line '{line_key}' needs at least {amount} item(s), has {len(line_items)}.")
    return random.sample(line_items, k=amount)


async def deliver_purchase(
    bot: Any,
    settings: Settings,
    state: dict[str, Any],
    record: dict[str, Any],
    purchase: dict[str, Any],
) -> tuple[bool, str]:
    buyer_chat_id = int(record.get("buyer_chat_id") or 0)
    if buyer_chat_id <= 0:
        return False, "buyer chat id is missing."

    sku = purchase["sku"]
    if sku == "starter_unlock":
        try:
            items = random_items_from_line(state, "dickpic_hard", 2) + random_items_from_line(state, "dickpic_soft", 1)
        except RuntimeError as exc:
            return False, str(exc)
        for item in items:
            await bot.copy_message(
                chat_id=buyer_chat_id,
                from_chat_id=int(item["chat_id"]),
                message_id=int(item["message_id"]),
                protect_content=True,
            )
        purchase["delivery_summary"] = "2 hard, 1 soft"
        return True, "starter content delivered"

    if sku == "plus_bundle":
        try:
            items = delivery_items_for_line(record, state, "stroking", 1) + delivery_items_for_line(record, state, "strip", 1)
        except RuntimeError as exc:
            return False, str(exc)
        for item in items:
            await bot.copy_message(
                chat_id=buyer_chat_id,
                from_chat_id=int(item["chat_id"]),
                message_id=int(item["message_id"]),
                protect_content=True,
            )
        purchase["delivery_summary"] = "1 stroking, 1 strip"
        return True, "bundle delivered"

    if sku.startswith("plus_ppv_"):
        line_key = str(CATALOG[sku]["line_key"])
        try:
            items = delivery_items_for_line(record, state, line_key, 1)
        except RuntimeError as exc:
            return False, str(exc)
        item = items[0]
        await bot.copy_message(
            chat_id=buyer_chat_id,
            from_chat_id=int(item["chat_id"]),
            message_id=int(item["message_id"]),
            protect_content=True,
        )
        purchase["delivery_summary"] = line_key
        return True, f"{line_key} delivered"

    if sku.startswith("pro_"):
        return False, "manual fulfillment required"

    return False, "unknown product"


async def ensure_topic(
    bot: Any,
    settings: Settings,
    state: dict[str, Any],
    record: dict[str, Any],
) -> int | None:
    if settings.relay_admin_group_id is None:
        return None
    existing_topic = record.get("topic_id")
    if existing_topic:
        return int(existing_topic)
    topic_name = f"{record.get('display_name', 'buyer')} | {tier_label(record.get('tier'))}"
    topic = await bot.create_forum_topic(chat_id=settings.relay_admin_group_id, name=topic_name)
    topic_id = int(topic.message_thread_id)
    record["topic_id"] = topic_id
    record["topic_name"] = topic_name
    save_state(settings, state)
    return topic_id


async def post_to_admin_topic(
    bot: Any,
    settings: Settings,
    state: dict[str, Any],
    record: dict[str, Any],
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:
    if settings.relay_admin_group_id is None:
        return
    topic_id = await ensure_topic(bot, settings, state, record)
    if topic_id is None:
        return
    await bot.send_message(
        chat_id=settings.relay_admin_group_id,
        message_thread_id=topic_id,
        text=text,
        reply_markup=reply_markup,
    )


async def send_buyer_home(message_target: Any, record: dict[str, Any]) -> None:
    await message_target.reply_text(current_home_text(record), reply_markup=main_menu_keyboard())


async def edit_buyer_view(query: Any, text: str, reply_markup: InlineKeyboardMarkup) -> None:
    await query.edit_message_text(text=text, reply_markup=reply_markup)


async def handle_buyer_menu(query: Any, record: dict[str, Any]) -> None:
    destination = query.data.partition(":")[2]
    tier = record.get("tier")

    if destination == "starter":
        await edit_buyer_view(query, starter_text(record), starter_keyboard(record))
        return

    if destination == "starter_next":
        await edit_buyer_view(query, what_comes_next_text(), simple_back_keyboard("starter"))
        return

    if destination == "plus":
        if not can_open_plus(record):
            await edit_buyer_view(query, tier_locked_text("plus", tier), simple_back_keyboard("home"))
            return
        await edit_buyer_view(query, plus_text(record), plus_keyboard())
        return

    if destination == "plus_bundle":
        if not can_open_plus(record):
            await edit_buyer_view(query, tier_locked_text("plus", tier), simple_back_keyboard("home"))
            return
        await edit_buyer_view(query, plus_bundle_text(record), plus_bundle_keyboard())
        return

    if destination == "plus_ppvs":
        if not can_open_plus(record):
            await edit_buyer_view(query, tier_locked_text("plus", tier), simple_back_keyboard("home"))
            return
        await edit_buyer_view(query, plus_ppv_text(record), plus_ppv_keyboard())
        return

    if destination == "pro":
        if not can_open_pro(record):
            await edit_buyer_view(query, tier_locked_text("pro", tier), simple_back_keyboard("home"))
            return
        await edit_buyer_view(query, pro_text(), pro_keyboard())
        return

    if destination == "pro_video":
        if not can_open_pro(record):
            await edit_buyer_view(query, tier_locked_text("pro", tier), simple_back_keyboard("home"))
            return
        await edit_buyer_view(query, pro_video_text(record), pro_video_keyboard())
        return

    if destination == "pro_voice":
        if not can_open_pro(record):
            await edit_buyer_view(query, tier_locked_text("pro", tier), simple_back_keyboard("home"))
            return
        await edit_buyer_view(query, pro_voice_text(record), pro_voice_keyboard())
        return

    if destination == "pro_chat":
        if not can_open_pro(record):
            await edit_buyer_view(query, tier_locked_text("chat", tier), simple_back_keyboard("home"))
            return
        await edit_buyer_view(query, pro_chat_text(), simple_back_keyboard("pro"))
        return

    if destination == "access":
        await edit_buyer_view(query, access_text(record), simple_back_keyboard("home"))
        return

    if destination == "purchases":
        await edit_buyer_view(query, purchases_text(record), simple_back_keyboard("home"))
        return

    if destination == "rules":
        await edit_buyer_view(query, rules_text(), simple_back_keyboard("home"))
        return

    await edit_buyer_view(query, current_home_text(record), main_menu_keyboard())


async def begin_test_session(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    settings: Settings,
    state: dict[str, Any],
    *,
    mode: str,
) -> None:
    if update.effective_user is None or update.effective_chat is None or update.message is None:
        return
    owner_user_id = update.effective_user.id
    synthetic_user_id = make_test_user_id(owner_user_id)
    record = new_record(synthetic_user_id, owner_user_id, update.effective_chat.id, mode=mode)
    sync_record_identity(record, update.effective_user, update.effective_chat.id)
    state.setdefault("users", {})[str(synthetic_user_id)] = record
    state.setdefault("test_sessions", {})[str(owner_user_id)] = synthetic_user_id
    save_state(settings, state)

    if mode == "seeded":
        await post_to_admin_topic(
            context.bot,
            settings,
            state,
            record,
            format_admin_review_card(record, "test buyer seeded"),
            reply_markup=admin_review_keyboard(synthetic_user_id),
        )
        await send_buyer_home(update.message, record)
        return

    await post_to_admin_topic(
        context.bot,
        settings,
        state,
        record,
        format_admin_review_card(record, "test buyer started full intake"),
        reply_markup=admin_review_keyboard(synthetic_user_id),
    )
    await update.message.reply_text(
        "welcome.\n\nsend your OnlyFans username to begin.",
    )


def end_test_session(settings: Settings, state: dict[str, Any], owner_user_id: int) -> bool:
    synthetic_user_id = active_test_user_id(state, owner_user_id)
    if synthetic_user_id is None:
        return False
    state.get("test_sessions", {}).pop(str(owner_user_id), None)
    state.get("users", {}).pop(str(synthetic_user_id), None)
    save_state(settings, state)
    return True


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    state = load_state(settings)
    if update.effective_user is None or update.effective_chat is None or update.message is None:
        return
    if update.effective_chat.type != "private":
        return

    active_record = get_active_private_record(state, update.effective_user.id)
    if active_record is not None:
        sync_record_identity(active_record, update.effective_user, update.effective_chat.id)
        save_state(settings, state)
        if active_record.get("intake_state") == "awaiting_of_username":
            await update.message.reply_text("welcome.\n\nsend your OnlyFans username to begin.")
            return
        await send_buyer_home(update.message, active_record)
        return

    if settings.test_only_mode and not is_admin_update(update, settings):
        await update.message.reply_text("this test bot is private for now.")
        return

    await update.message.reply_text(
        "v2 admin lane is live.\n\n"
        "/testmode -> jump straight into the buyer side after approval\n"
        "/testmodefull -> begin from username + budget + request\n"
        "/helpadmin -> admin command help"
    )


async def helpadmin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    if not is_admin_update(update, settings) or update.message is None:
        return
    await update.message.reply_text(
        "admin help\n\n"
        "/testmode - seed an approved buyer straight into the private menu\n"
        "/testmodefull - start from username, budget, and request\n"
        "/testreset - end the current buyer test session\n"
        "/setvault - run in the vault chat to register that chat\n"
        "/addcontent <line_key> [title] - reply to a vault post to add it\n"
        "/catalog - show stored vault line counts\n\n"
        "line keys for v2 right now:\n"
        "dickpic_hard\n"
        "dickpic_soft\n"
        "stroking\n"
        "ass_noboxers\n"
        "strip"
    )


async def testmode_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    state = load_state(settings)
    if not is_admin_update(update, settings) or update.message is None or update.effective_chat is None or update.effective_user is None:
        return
    if update.effective_chat.type != "private":
        await update.message.reply_text("open a private chat with the bot first.")
        return
    ended = end_test_session(settings, state, update.effective_user.id)
    if ended:
        state = load_state(settings)
    await begin_test_session(update, context, settings, state, mode="seeded")


async def testmodefull_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    state = load_state(settings)
    if not is_admin_update(update, settings) or update.message is None or update.effective_chat is None or update.effective_user is None:
        return
    if update.effective_chat.type != "private":
        await update.message.reply_text("open a private chat with the bot first.")
        return
    ended = end_test_session(settings, state, update.effective_user.id)
    if ended:
        state = load_state(settings)
    await begin_test_session(update, context, settings, state, mode="full")


async def testreset_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    state = load_state(settings)
    if not is_admin_update(update, settings) or update.message is None or update.effective_user is None:
        return
    if end_test_session(settings, state, update.effective_user.id):
        await update.message.reply_text("test session cleared.")
    else:
        await update.message.reply_text("no active test session.")


async def setvault_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    state = load_state(settings)
    if update.effective_chat is None or update.message is None or update.effective_user is None:
        return
    if not is_admin_user_id(update.effective_user.id, settings):
        return
    state["vault_chat_id"] = update.effective_chat.id
    save_state(settings, state)
    await update.message.reply_text(f"vault chat set to {update.effective_chat.id}.")


async def addcontent_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    state = load_state(settings)
    if update.effective_chat is None or update.message is None or update.effective_user is None:
        return
    if not is_admin_user_id(update.effective_user.id, settings):
        return
    if state.get("vault_chat_id") != update.effective_chat.id:
        await update.message.reply_text("run this in the registered vault chat.")
        return
    if update.message.reply_to_message is None:
        await update.message.reply_text("reply to a vault message with /addcontent <line_key> [title].")
        return
    if not context.args:
        await update.message.reply_text("usage: /addcontent <line_key> [title]")
        return
    line_key = context.args[0].strip().lower()
    title = " ".join(context.args[1:]).strip() or f"{line_key} item"
    line = state.setdefault("vault_lines", {}).setdefault(line_key, [])
    line.append(
        {
            "item_id": uuid.uuid4().hex[:10],
            "title": title,
            "chat_id": update.effective_chat.id,
            "message_id": update.message.reply_to_message.message_id,
        }
    )
    save_state(settings, state)
    await update.message.reply_text(f"added to {line_key}. total items: {len(line)}")


async def catalog_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    state = load_state(settings)
    if update.message is None or update.effective_user is None:
        return
    if not is_admin_user_id(update.effective_user.id, settings):
        return
    lines = ["catalog"]
    for line_key in sorted(state.get("vault_lines", {})):
        count = len(state["vault_lines"][line_key])
        lines.append(f"- {line_key}: {count}")
    if len(lines) == 1:
        lines.append("no content lines registered yet.")
    await update.message.reply_text("\n".join(lines))


async def text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    if update.effective_user is None or update.effective_chat is None or update.message is None:
        return
    if update.effective_chat.type != "private":
        return

    state = load_state(settings)
    record = get_active_private_record(state, update.effective_user.id)
    if record is None:
        if settings.test_only_mode and not is_admin_update(update, settings):
            await update.message.reply_text("this test bot is private for now.")
            return
        if is_admin_update(update, settings):
            await update.message.reply_text("use /testmode or /testmodefull to enter the buyer flow.")
        return

    sync_record_identity(record, update.effective_user, update.effective_chat.id)
    intake_state = record.get("intake_state")

    if intake_state == "awaiting_of_username":
        record["of_username"] = update.message.text.strip()
        record["intake_state"] = "awaiting_budget"
        save_state(settings, state)
        await update.message.reply_text(
            "thanks. to route the request properly, what range are you planning for the first request?",
            reply_markup=budget_keyboard(),
        )
        return

    if intake_state == "awaiting_purchase_intent":
        record["purchase_intent"] = update.message.text.strip()
        record["review_status"] = "pending"
        record["intake_state"] = "waiting_review"
        save_state(settings, state)
        await update.message.reply_text(
            "thanks. i have your request.\n\n"
            f"OnlyFans username: {record.get('of_username')}\n"
            f"looking for: {record.get('purchase_intent')}\n\n"
            "if it looks like a fit, i’ll follow up here."
        )
        await post_to_admin_topic(
            context.bot,
            settings,
            state,
            record,
            format_admin_review_card(record, "new v2 application"),
            reply_markup=admin_review_keyboard(record["user_id"]),
        )
        return

    if intake_state == "waiting_review":
        await update.message.reply_text("your application is waiting for review.")
        return

    if record.get("tier") == TIER_PRO:
        await post_to_admin_topic(
            context.bot,
            settings,
            state,
            record,
            f"pro chat message\n\n{record.get('display_name')}\n\n{update.message.text.strip()}",
        )
        await update.message.reply_text("got it \U0001f4ac")
        return

    await update.message.reply_text("use the menu below.", reply_markup=main_menu_keyboard())


async def notify_buyer_of_tier_change(bot: Any, record: dict[str, Any], tier: str) -> None:
    buyer_chat_id = int(record.get("buyer_chat_id") or 0)
    if buyer_chat_id <= 0:
        return
    if tier == TIER_VERIFIED:
        await bot.send_message(chat_id=buyer_chat_id, text=current_home_text(record), reply_markup=main_menu_keyboard())
        return
    if tier == TIER_STARTER:
        text = (
            "you’re now Starter \U0001f5dd\ufe0f\n\n"
            "next up is Plus, where better bundles and stronger unlocks become available.\n"
            "after Plus comes Pro, where the most personal access opens.\n\n"
            "open-ended chatting isn’t included yet — access opens gradually."
        )
        await bot.send_message(chat_id=buyer_chat_id, text=text, reply_markup=main_menu_keyboard())
        return
    if tier == TIER_PLUS:
        text = (
            "you’re now Plus \U0001f48e\n\n"
            "bundles and Premium PPVs are open now.\n"
            "one more Plus purchase opens Pro."
        )
        await bot.send_message(chat_id=buyer_chat_id, text=text, reply_markup=main_menu_keyboard())
        return
    if tier == TIER_PRO:
        text = (
            "unlocked \U0001f525\n"
            "you’re now Pro.\n\n"
            "Pro gives you access to the most personal items and chat access."
        )
        await bot.send_message(chat_id=buyer_chat_id, text=text, reply_markup=main_menu_keyboard())


async def mark_purchase_paid(
    query: Any,
    context: ContextTypes.DEFAULT_TYPE,
    settings: Settings,
    state: dict[str, Any],
    record: dict[str, Any],
    purchase: dict[str, Any],
) -> None:
    purchase["status"] = "paid"
    purchase["paid_at"] = to_iso(utc_now())
    tier_before = record.get("tier")
    new_tier = apply_tier_progression(record, purchase["sku"])
    save_state(settings, state)

    if CATALOG[purchase["sku"]]["delivery_mode"] == "manual":
        purchase["status"] = "pending_manual"
        save_state(settings, state)
        await context.bot.send_message(
            chat_id=int(record["buyer_chat_id"]),
            text="payment confirmed.\n\nthis one is prepared manually, so i’ll handle it from here.",
            reply_markup=main_menu_keyboard(),
        )
        await post_to_admin_topic(
            context.bot,
            settings,
            state,
            record,
            format_admin_purchase_card(record, purchase, "manual fulfillment needed"),
            reply_markup=admin_manual_fulfillment_keyboard(record["user_id"], purchase["purchase_id"]),
        )
    else:
        delivered, detail = await deliver_purchase(context.bot, settings, state, record, purchase)
        if delivered:
            purchase["status"] = "fulfilled"
            purchase["fulfilled_at"] = to_iso(utc_now())
            save_state(settings, state)
            await context.bot.send_message(
                chat_id=int(record["buyer_chat_id"]),
                text=f"payment confirmed \u2705\n\n{detail}",
                reply_markup=main_menu_keyboard(),
            )
        else:
            purchase["status"] = "pending_manual"
            purchase["delivery_summary"] = detail
            save_state(settings, state)
            await context.bot.send_message(
                chat_id=int(record["buyer_chat_id"]),
                text="payment confirmed.\n\nsomething needs a manual check before delivery, so i’m handling it from here.",
                reply_markup=main_menu_keyboard(),
            )
            await post_to_admin_topic(
                context.bot,
                settings,
                state,
                record,
                format_admin_purchase_card(record, purchase, f"delivery held up: {detail}"),
                reply_markup=admin_manual_fulfillment_keyboard(record["user_id"], purchase["purchase_id"]),
            )

    if new_tier and new_tier != tier_before:
        await notify_buyer_of_tier_change(context.bot, record, new_tier)

    await query.edit_message_text(
        text=format_admin_purchase_card(record, purchase, "payment confirmed"),
        reply_markup=admin_review_keyboard(record["user_id"]),
    )


async def button_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    query = update.callback_query
    if query is None or query.from_user is None or query.data is None:
        return
    state = load_state(settings)
    data = query.data

    if data.startswith("budget:"):
        record = get_active_private_record(state, query.from_user.id)
        if record is None:
            await query.answer("no active buyer session.", show_alert=True)
            return
        if record.get("intake_state") != "awaiting_budget":
            await query.answer("that step is no longer active.", show_alert=True)
            return
        budget_key = data.partition(":")[2]
        record["budget_key"] = budget_key
        record["budget_label"] = get_budget_label(budget_key)
        record["intake_state"] = "awaiting_purchase_intent"
        save_state(settings, state)
        await query.answer("saved")
        await query.edit_message_text(f"budget range saved: {record['budget_label']}")
        await context.bot.send_message(
            chat_id=int(record["buyer_chat_id"]),
            text="what are you looking to buy in the first interaction?",
        )
        return

    if data.startswith("menu:"):
        record = get_active_private_record(state, query.from_user.id)
        if record is None:
            await query.answer("no active buyer session.", show_alert=True)
            return
        await query.answer()
        await handle_buyer_menu(query, record)
        save_state(settings, state)
        return

    if data.startswith("nav:"):
        record = get_active_private_record(state, query.from_user.id)
        if record is None:
            await query.answer("no active buyer session.", show_alert=True)
            return
        target = data.partition(":")[2]
        await query.answer()
        if target == "home":
            await edit_buyer_view(query, current_home_text(record), main_menu_keyboard())
        else:
            await handle_buyer_menu(query, record)
        return

    if data.startswith("buy:"):
        record = get_active_private_record(state, query.from_user.id)
        if record is None:
            await query.answer("no active buyer session.", show_alert=True)
            return
        sku = data.partition(":")[2]
        if sku not in CATALOG:
            await query.answer("unknown product", show_alert=True)
            return

        if sku == "starter_unlock" and tier_rank(record.get("tier")) < tier_rank(TIER_VERIFIED):
            await query.answer("starter opens after verification.", show_alert=True)
            return
        if sku.startswith("plus_") and not can_open_plus(record):
            await query.answer("plus is locked.", show_alert=True)
            return
        if sku.startswith("pro_") and not can_open_pro(record):
            await query.answer("pro is locked.", show_alert=True)
            return

        purchase = create_purchase(record, sku)
        save_state(settings, state)
        await post_to_admin_topic(
            context.bot,
            settings,
            state,
            record,
            format_admin_purchase_card(record, purchase, "purchase requested"),
        )
        await query.answer()
        await query.edit_message_text(
            text=(
                "payment request\n\n"
                f"{purchase['title']}\n"
                f"amount due: {money_text(int(purchase['price_cents']))}\n\n"
                "simple, instant, and no pressure."
            ),
            reply_markup=payment_keyboard(settings, purchase["purchase_id"]),
        )
        return

    if data.startswith("paid:"):
        record = get_active_private_record(state, query.from_user.id)
        if record is None:
            await query.answer("no active buyer session.", show_alert=True)
            return
        purchase_id = data.partition(":")[2]
        purchase = find_purchase(record, purchase_id)
        if purchase is None:
            await query.answer("purchase not found.", show_alert=True)
            return
        if purchase["status"] not in {"awaiting_payment", "payment_claimed"}:
            await query.answer("payment is already being handled.", show_alert=True)
            return
        purchase["status"] = "payment_claimed"
        save_state(settings, state)
        await post_to_admin_topic(
            context.bot,
            settings,
            state,
            record,
            format_admin_purchase_card(record, purchase, "buyer says payment is sent"),
            reply_markup=admin_payment_keyboard(record["user_id"], purchase_id),
        )
        await query.answer("sent")
        await query.edit_message_text(
            text=(
                "got it.\n\n"
                "i’ve marked your payment as claimed and i’m checking it now."
            ),
            reply_markup=main_menu_keyboard(),
        )
        return

    if data.startswith("adm:"):
        if not is_admin_user_id(query.from_user.id, settings):
            await query.answer("admin only", show_alert=True)
            return
        parts = data.split(":")
        if len(parts) < 3:
            await query.answer("invalid admin action", show_alert=True)
            return
        action = parts[1]
        user_id = int(parts[2])
        record = state.get("users", {}).get(str(user_id))
        if record is None:
            await query.answer("buyer not found", show_alert=True)
            return

        if action == "tier" and len(parts) == 4:
            target_tier = parts[3]
            record["tier"] = target_tier
            record["review_status"] = "approved"
            record["intake_state"] = "buyer_active"
            record["updated_at"] = to_iso(utc_now())
            save_state(settings, state)
            await notify_buyer_of_tier_change(context.bot, record, target_tier)
            await query.answer("tier updated")
            await query.edit_message_text(
                text=format_admin_review_card(record, "tier updated"),
                reply_markup=admin_review_keyboard(user_id),
            )
            return

        if action == "reject":
            record["review_status"] = "rejected"
            record["intake_state"] = "rejected"
            record["updated_at"] = to_iso(utc_now())
            save_state(settings, state)
            await context.bot.send_message(
                chat_id=int(record["buyer_chat_id"]),
                text="sorry, this request wasn’t approved.",
            )
            await query.answer("rejected")
            await query.edit_message_text(
                text=format_admin_review_card(record, "buyer rejected"),
                reply_markup=admin_review_keyboard(user_id),
            )
            return

        if action == "flag":
            record["flagged"] = True
            record.setdefault("notes", []).append(f"flagged at {to_iso(utc_now())}")
            save_state(settings, state)
            await query.answer("flagged")
            await query.edit_message_text(
                text=format_admin_review_card(record, "buyer flagged"),
                reply_markup=admin_review_keyboard(user_id),
            )
            return

        if action == "summary":
            await query.answer()
            await query.edit_message_text(
                text=format_admin_review_card(record, "buyer summary"),
                reply_markup=admin_review_keyboard(user_id),
            )
            return

        if action in {"pay", "deny", "fulfill"} and len(parts) == 4:
            purchase_id = parts[3]
            purchase = find_purchase(record, purchase_id)
            if purchase is None:
                await query.answer("purchase not found", show_alert=True)
                return
            if action == "pay":
                await query.answer("payment confirmed")
                await mark_purchase_paid(query, context, settings, state, record, purchase)
                return
            if action == "deny":
                purchase["status"] = "awaiting_payment"
                save_state(settings, state)
                await context.bot.send_message(
                    chat_id=int(record["buyer_chat_id"]),
                    text="i couldn’t confirm that payment yet. try again once it has landed.",
                    reply_markup=main_menu_keyboard(),
                )
                await query.answer("reset")
                await query.edit_message_text(
                    text=format_admin_purchase_card(record, purchase, "payment not confirmed"),
                    reply_markup=admin_payment_keyboard(user_id, purchase_id),
                )
                return
            if action == "fulfill":
                purchase["status"] = "fulfilled"
                purchase["fulfilled_at"] = to_iso(utc_now())
                save_state(settings, state)
                await context.bot.send_message(
                    chat_id=int(record["buyer_chat_id"]),
                    text=f"{purchase['title']} is marked fulfilled \u2705",
                    reply_markup=main_menu_keyboard(),
                )
                await query.answer("fulfilled")
                await query.edit_message_text(
                    text=format_admin_purchase_card(record, purchase, "manual fulfillment completed"),
                    reply_markup=admin_review_keyboard(user_id),
                )
                return

        await query.answer("unknown admin action", show_alert=True)


def build_application(settings: Settings) -> Application:
    application = Application.builder().token(settings.bot_token).build()
    application.bot_data["settings"] = settings

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("helpadmin", helpadmin_command))
    application.add_handler(CommandHandler("testmode", testmode_command))
    application.add_handler(CommandHandler("testmodefull", testmodefull_command))
    application.add_handler(CommandHandler("testreset", testreset_command))
    application.add_handler(CommandHandler("setvault", setvault_command))
    application.add_handler(CommandHandler("addcontent", addcontent_command))
    application.add_handler(CommandHandler("catalog", catalog_command))
    application.add_handler(CallbackQueryHandler(button_click))
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, text_message))
    return application


def main() -> None:
    settings = load_settings()
    LOGGER.info("starting v2 bot; test_only_mode=%s", settings.test_only_mode)
    asyncio.set_event_loop(asyncio.new_event_loop())
    application = build_application(settings)
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
