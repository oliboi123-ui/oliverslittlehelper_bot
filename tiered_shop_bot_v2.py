from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import random
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
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
LOGGER = logging.getLogger("tiered_shop_bot_v2")

PAYPAL_MAIN_LOOP: asyncio.AbstractEventLoop | None = None
PAYPAL_BOT: Any | None = None
PAYPAL_SETTINGS: "Settings | None" = None
PAYPAL_WEBHOOK_SERVER: ThreadingHTTPServer | None = None
PAYPAL_WEBHOOK_THREAD: threading.Thread | None = None

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
    test_only_mode: bool
    state_path: Path
    paypal_client_id: str | None
    paypal_client_secret: str | None
    paypal_webhook_id: str | None
    paypal_public_base_url: str | None
    paypal_env: str
    paypal_webhook_port: int


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


def cents_to_paypal_value(cents: int) -> str:
    return f"{cents / 100:.2f}"


def parse_admin_ids(raw_value: str) -> frozenset[int]:
    admin_ids: set[int] = set()
    for part in raw_value.split(","):
        part = part.strip()
        if not part:
            continue
        admin_ids.add(int(part))
    return frozenset(admin_ids)


def normalize_public_base_url(raw_value: str | None) -> str | None:
    value = (raw_value or "").strip()
    if not value:
        return None
    if not value.startswith(("http://", "https://")):
        value = "https://" + value.lstrip("/")
    return value.rstrip("/")


def load_settings() -> Settings:
    bot_token = os.environ["BOT_TOKEN"].strip()
    admin_ids = parse_admin_ids(os.environ.get("ADMIN_USER_IDS", ""))
    if not admin_ids:
        raise RuntimeError("ADMIN_USER_IDS must contain at least one Telegram user id.")
    relay_group_raw = os.environ.get("RELAY_ADMIN_GROUP_ID", "").strip()
    relay_group_id = int(relay_group_raw) if relay_group_raw else None
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
        test_only_mode=test_only_mode,
        state_path=state_path,
        paypal_client_id=os.environ.get("PAYPAL_CLIENT_ID", "").strip() or None,
        paypal_client_secret=os.environ.get("PAYPAL_CLIENT_SECRET", "").strip() or None,
        paypal_webhook_id=os.environ.get("PAYPAL_WEBHOOK_ID", "").strip() or None,
        paypal_public_base_url=normalize_public_base_url(os.environ.get("PAYPAL_PUBLIC_BASE_URL")),
        paypal_env=os.environ.get("PAYPAL_ENV", "live").strip().lower() or "live",
        paypal_webhook_port=int(os.environ.get("PAYPAL_WEBHOOK_PORT", os.environ.get("PORT", "8080"))),
    )


def load_state(settings: Settings) -> dict[str, Any]:
    if not settings.state_path.exists():
        return {
            "users": {},
            "test_sessions": {},
            "vault_chat_id": None,
            "vault_lines": {},
            "pending_admin_actions": {},
            "paypal_orders": {},
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
            "pending_admin_actions": {},
            "paypal_orders": {},
        }
    state.setdefault("users", {})
    state.setdefault("test_sessions", {})
    state.setdefault("vault_chat_id", None)
    state.setdefault("vault_lines", {})
    state.setdefault("pending_admin_actions", {})
    state.setdefault("paypal_orders", {})
    return state


def save_state(settings: Settings, state: dict[str, Any]) -> None:
    settings.state_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = settings.state_path.with_suffix(".tmp")
    temp_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(settings.state_path)


def paypal_api_base(settings: Settings) -> str:
    if settings.paypal_env == "sandbox":
        return "https://api-m.sandbox.paypal.com"
    return "https://api-m.paypal.com"


def paypal_is_configured(settings: Settings) -> bool:
    return bool(
        settings.paypal_client_id
        and settings.paypal_client_secret
        and settings.paypal_webhook_id
        and settings.paypal_public_base_url
    )


def paypal_setup_failure_title(exc: Exception) -> str:
    message = str(exc)
    if "PAYEE_ACCOUNT_RESTRICTED" in message:
        return "PayPal account restricted - checkout blocked"
    if "PayPal checkout is not configured" in message or "PAYPAL_PUBLIC_BASE_URL" in message:
        return "PayPal checkout is not fully configured"
    if "PayPal client credentials are missing" in message:
        return "PayPal credentials are missing"
    if "PayPal token request failed" in message:
        return "PayPal credentials were rejected"
    return "PayPal setup failed"


def paypal_setup_failure_detail(exc: Exception) -> str:
    message = str(exc)
    if "PAYEE_ACCOUNT_RESTRICTED" in message:
        return (
            "PayPal refused to create the checkout because the receiving merchant account is restricted. "
            "Open the PayPal account Resolution Center or contact PayPal support to remove the restriction."
        )
    if "PayPal checkout is not configured" in message:
        return (
            "PayPal checkout is missing one or more Railway variables: PAYPAL_CLIENT_ID, "
            "PAYPAL_CLIENT_SECRET, PAYPAL_WEBHOOK_ID, or PAYPAL_PUBLIC_BASE_URL."
        )
    if "PAYPAL_PUBLIC_BASE_URL" in message:
        return "PAYPAL_PUBLIC_BASE_URL should be the public Railway base URL, without /paypal/webhook at the end."
    if "PayPal client credentials are missing" in message:
        return "PAYPAL_CLIENT_ID and PAYPAL_CLIENT_SECRET need to be set in Railway."
    if "PayPal token request failed" in message:
        return "PayPal rejected the client id/secret. Check that PAYPAL_ENV matches the credential type."
    return message


def buyer_payment_unavailable_text() -> str:
    return (
        "payment is paused for a moment.\n\n"
        "your request is saved, and i'll sort the PayPal side before asking you to try again."
    )


def paypal_return_url(settings: Settings) -> str:
    if not settings.paypal_public_base_url:
        raise RuntimeError("PAYPAL_PUBLIC_BASE_URL is required for PayPal checkout.")
    return settings.paypal_public_base_url.rstrip("/") + "/paypal/return"


def paypal_cancel_url(settings: Settings) -> str:
    if not settings.paypal_public_base_url:
        raise RuntimeError("PAYPAL_PUBLIC_BASE_URL is required for PayPal checkout.")
    return settings.paypal_public_base_url.rstrip("/") + "/paypal/cancel"


def paypal_get_access_token(settings: Settings) -> str:
    if not settings.paypal_client_id or not settings.paypal_client_secret:
        raise RuntimeError("PayPal client credentials are missing.")
    auth = base64.b64encode(
        f"{settings.paypal_client_id}:{settings.paypal_client_secret}".encode("utf-8")
    ).decode("ascii")
    request = urllib_request.Request(
        paypal_api_base(settings).rstrip("/") + "/v1/oauth2/token",
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
    settings: Settings,
    state: dict[str, Any],
    record: dict[str, Any],
    purchase: dict[str, Any],
) -> tuple[str, str]:
    if not paypal_is_configured(settings):
        raise RuntimeError("PayPal checkout is not configured.")

    token = paypal_get_access_token(settings)
    user_id = int(record["user_id"])
    purchase_id = str(purchase["purchase_id"])
    invoice_id = f"v2-{user_id}-{purchase_id}-{uuid.uuid4().hex[:8]}"
    payload = {
        "intent": "CAPTURE",
        "purchase_units": [
            {
                "custom_id": f"{user_id}:{purchase_id}",
                "invoice_id": invoice_id,
                "description": str(purchase.get("title") or "private unlock")[:127],
                "amount": {
                    "currency_code": "USD",
                    "value": cents_to_paypal_value(int(purchase.get("price_cents", 0))),
                },
            }
        ],
        "application_context": {
            "brand_name": "Oliver's Little Helper",
            "return_url": paypal_return_url(settings),
            "cancel_url": paypal_cancel_url(settings),
        },
    }
    request = urllib_request.Request(
        paypal_api_base(settings).rstrip("/") + "/v2/checkout/orders",
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "PayPal-Request-Id": f"{user_id}-{purchase_id}-{uuid.uuid4().hex}",
        },
    )
    try:
        with urllib_request.urlopen(request, timeout=30) as response:
            order_payload = json.loads(response.read().decode("utf-8"))
    except urllib_error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"PayPal order creation failed ({exc.code}): {detail}") from exc

    order_id = str(order_payload.get("id") or "").strip()
    if not order_id:
        raise RuntimeError("PayPal did not return an order id.")

    approval_url = ""
    for link in order_payload.get("links", []):
        if link.get("rel") in {"approve", "payer-action"} and link.get("href"):
            approval_url = str(link["href"])
            break
    if not approval_url:
        raise RuntimeError("PayPal did not return an approval URL.")

    purchase["paypal_order_id"] = order_id
    purchase["paypal_approval_url"] = approval_url
    purchase["paypal_invoice_id"] = invoice_id
    state.setdefault("paypal_orders", {})[order_id] = {
        "user_id": user_id,
        "purchase_id": purchase_id,
        "amount": cents_to_paypal_value(int(purchase.get("price_cents", 0))),
        "currency": "USD",
        "status": "created",
        "created_at": to_iso(utc_now()),
    }
    save_state(settings, state)
    return order_id, approval_url


def paypal_capture_order(settings: Settings, order_id: str) -> dict[str, Any]:
    if not paypal_is_configured(settings):
        raise RuntimeError("PayPal checkout is not configured.")
    token = paypal_get_access_token(settings)
    request = urllib_request.Request(
        paypal_api_base(settings).rstrip("/") + f"/v2/checkout/orders/{urllib_parse.quote(order_id)}/capture",
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


def paypal_verify_webhook(settings: Settings, raw_body: bytes, headers: Any) -> dict[str, Any]:
    if not paypal_is_configured(settings):
        raise RuntimeError("PayPal webhook verification is not configured.")
    event = json.loads(raw_body.decode("utf-8"))
    verification_payload = {
        "auth_algo": headers.get("PAYPAL-AUTH-ALGO") or headers.get("paypal-auth-algo"),
        "cert_url": headers.get("PAYPAL-CERT-URL") or headers.get("paypal-cert-url"),
        "transmission_id": headers.get("PAYPAL-TRANSMISSION-ID") or headers.get("paypal-transmission-id"),
        "transmission_sig": headers.get("PAYPAL-TRANSMISSION-SIG") or headers.get("paypal-transmission-sig"),
        "transmission_time": headers.get("PAYPAL-TRANSMISSION-TIME") or headers.get("paypal-transmission-time"),
        "webhook_id": settings.paypal_webhook_id,
        "webhook_event": event,
    }
    required = ("auth_algo", "cert_url", "transmission_id", "transmission_sig", "transmission_time", "webhook_id")
    if not all(verification_payload.get(key) for key in required):
        raise RuntimeError("Missing PayPal signature headers.")

    token = paypal_get_access_token(settings)
    request = urllib_request.Request(
        paypal_api_base(settings).rstrip("/") + "/v1/notifications/verify-webhook-signature",
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
    return event


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


def purchase_status_label(status: str | None) -> str:
    labels = {
        "awaiting_approval": "waiting for approval",
        "awaiting_payment": "waiting for payment",
        "payment_claimed": "payment under review",
        "paid": "paid",
        "pending_manual": "being prepared",
        "fulfilled": "delivered",
        "declined": "declined",
        "payment_setup_failed": "payment temporarily unavailable",
    }
    return labels.get(status or "", status or "unknown")


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
        "pending_buyer_action": None,
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
            "pending_buyer_action": None,
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


def clear_pending_buyer_action(record: dict[str, Any]) -> None:
    record["pending_buyer_action"] = None


def is_internal_topic_note(message: Any) -> bool:
    text = str(message.text or message.caption or "").strip()
    return text.startswith("//")


def find_record_by_topic_id(state: dict[str, Any], topic_id: int | None) -> dict[str, Any] | None:
    if not isinstance(topic_id, int):
        return None
    for record in state.get("users", {}).values():
        if int(record.get("topic_id") or 0) == topic_id:
            return record
    return None


def pending_admin_action(state: dict[str, Any], admin_user_id: int) -> dict[str, Any] | None:
    action = state.setdefault("pending_admin_actions", {}).get(str(admin_user_id))
    return action if isinstance(action, dict) else None


def set_pending_admin_action(state: dict[str, Any], admin_user_id: int, action: dict[str, Any]) -> None:
    state.setdefault("pending_admin_actions", {})[str(admin_user_id)] = action


def clear_pending_admin_action(state: dict[str, Any], admin_user_id: int) -> None:
    state.setdefault("pending_admin_actions", {}).pop(str(admin_user_id), None)


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
            [InlineKeyboardButton("payment help \U0001f4b8", callback_data="menu:payment_help")],
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


def plus_keyboard(record: dict[str, Any]) -> InlineKeyboardMarkup:
    if record.get("tier") == TIER_STARTER:
        rows = [
            [InlineKeyboardButton("step in with bundle 💎", callback_data="menu:plus_bundle")],
            [InlineKeyboardButton("step in with premium ppvs 🔥", callback_data="menu:plus_ppvs")],
            [InlineKeyboardButton("back", callback_data="nav:home")],
        ]
    else:
        rows = [
            [InlineKeyboardButton("best value bundle 💎", callback_data="menu:plus_bundle")],
            [InlineKeyboardButton("premium ppvs 🔥", callback_data="menu:plus_ppvs")],
            [InlineKeyboardButton("back", callback_data="nav:home")],
        ]
    return InlineKeyboardMarkup(rows)


def plus_bundle_keyboard(record: dict[str, Any]) -> InlineKeyboardMarkup:
    if record.get("tier") == TIER_STARTER:
        label = "buy bundle + skip to Pro 💎"
    elif record.get("tier") == TIER_PLUS:
        label = "buy bundle + open Pro 💎"
    else:
        label = CATALOG["plus_bundle"]["button_label"]
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(label, callback_data="buy:plus_bundle")],
            [InlineKeyboardButton("back", callback_data="menu:plus")],
        ]
    )


def plus_ppv_keyboard(record: dict[str, Any]) -> InlineKeyboardMarkup:
    if record.get("tier") == TIER_STARTER:
        rows = [
            [InlineKeyboardButton("enter Plus: stroking 🔥", callback_data="buy:plus_ppv_stroking")],
            [InlineKeyboardButton("enter Plus: ass 🍑", callback_data="buy:plus_ppv_ass")],
            [InlineKeyboardButton("enter Plus: strip 👕", callback_data="buy:plus_ppv_strip")],
            [InlineKeyboardButton("back", callback_data="menu:plus")],
        ]
    else:
        rows = [
            [InlineKeyboardButton(CATALOG["plus_ppv_stroking"]["button_label"], callback_data="buy:plus_ppv_stroking")],
            [InlineKeyboardButton(CATALOG["plus_ppv_ass"]["button_label"], callback_data="buy:plus_ppv_ass")],
            [InlineKeyboardButton(CATALOG["plus_ppv_strip"]["button_label"], callback_data="buy:plus_ppv_strip")],
            [InlineKeyboardButton("back", callback_data="menu:plus")],
        ]
    return InlineKeyboardMarkup(rows)


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


def payment_keyboard(approval_url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("pay with paypal \U0001f4b8", url=approval_url)],
            [InlineKeyboardButton("my purchases \U0001f4e6", callback_data="menu:purchases")],
            [InlineKeyboardButton("back", callback_data="nav:home")],
        ]
    )


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
                InlineKeyboardButton("payment not found", callback_data=f"adm:deny:{user_id}:{purchase_id}"),
            ],
            [InlineKeyboardButton("summary", callback_data=f"adm:summary:{user_id}")],
        ]
    )


def admin_payment_fallback_keyboard(user_id: int, purchase_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("manually verify payment \u2705", callback_data=f"adm:pay:{user_id}:{purchase_id}")],
            [InlineKeyboardButton("summary", callback_data=f"adm:summary:{user_id}")],
        ]
    )


def admin_voice_request_keyboard(user_id: int, purchase_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("accept request \u2705", callback_data=f"adm:voiceok:{user_id}:{purchase_id}"),
                InlineKeyboardButton("decline + explain \u274c", callback_data=f"adm:voiceno:{user_id}:{purchase_id}"),
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
            "you're verified \u2705\n"
            "you're inside the private menu now.\n\n"
            "starter is the first private unlock.\n"
            "Plus and Pro are visible too, but they open step by step.\n\n"
            "starter unlock \U0001f5dd\ufe0f\n"
            "- first paid step\n"
            "- 3 pics right away\n\n"
            "as your access grows, lower-tier PPVs get 25% cheaper."
        )
    if tier == TIER_STARTER:
        return (
            "you're in Starter \U0001f5dd\ufe0f\n\n"
            "your first private unlock is open.\n"
            "next up is Plus \U0001f48e\n\n"
            "from here:\n"
            "- a first Premium PPV moves you into Plus\n"
            "- the Best Value Bundle skips straight to Pro\n\n"
            "Pro stays visible too, so the path feels clear."
        )
    if tier == TIER_PLUS:
        return (
            "you're in Plus \U0001f48e\n\n"
            "this is the mid-tier layer.\n"
            "bundles and Premium PPVs are open here.\n\n"
            "one more Plus purchase opens Pro \U0001f525"
        )
    if tier == TIER_PRO:
        return (
            "you're in Pro \U0001f525\n\n"
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
            "unlocked now:\n"
            "- private menu\n"
            "- starter path\n\n"
            "still locked:\n"
            "- Plus\n"
            "- Pro\n\n"
            "PPVs get cheaper as your access grows."
        )
    if tier == TIER_STARTER:
        return (
            "your access \U0001f464\n\n"
            "current access: Starter\n\n"
            "unlocked now:\n"
            "- Starter\n"
            "- first-step private content\n\n"
            "next:\n"
            "- Plus with bundles and Premium PPVs\n"
            "- Pro after that"
        )
    if tier == TIER_PLUS:
        return (
            "your access \U0001f464\n\n"
            "current access: Plus\n\n"
            "unlocked now:\n"
            "- Plus menu\n"
            "- Premium PPVs\n"
            "- bundle access\n\n"
            "next:\n"
            "- one more Plus purchase opens Pro"
        )
    if tier == TIER_PRO:
        return (
            "your access \U0001f464\n\n"
            "current access: Pro\n\n"
            "unlocked now:\n"
            "- Pro products\n"
            "- chat access\n"
            "- 25% off lower-tier PPVs"
        )
    return "your access \U0001f464\n\nno active access yet."


def payment_help_text() -> str:
    return (
        "payment help \U0001f4b8\n\n"
        "when you buy something, this chat opens a fresh PayPal checkout link for that item.\n"
        "after PayPal confirms it, this chat updates automatically.\n\n"
        "if PayPal is temporarily unavailable, your request stays saved so it can be handled cleanly."
    )


def rules_text() -> str:
    return (
        "rules & boundaries \u26a0\ufe0f\n\n"
        "this is a structured private shop.\n"
        "purchases are delivered through the bot.\n"
        "chatting is only available at Pro.\n"
        "be respectful and easygoing.\n"
        "if something needs manual handling, i'll say so clearly.\n\n"
        "payment is handled through fresh PayPal checkout links made for each item."
    )


def tier_locked_text(target: str, current_tier: str | None) -> str:
    if target == "plus":
        return (
            "Plus is locked for now \U0001f512\n\n"
            "Starter is the first paid step.\n"
            "after that, Plus opens the next layer."
        )
    if target == "pro" and current_tier == TIER_STARTER:
        return "this is a Pro unlock \U0001f512\n\nPro opens after a Plus purchase."
    if target == "pro" and current_tier == TIER_PLUS:
        return "this opens at Pro \U0001f512\n\ncomplete any Plus unlock to open Pro access."
    if target == "chat":
        return "chat opens at Pro \U0001f4ac\U0001f512\n\nchatting isn't included yet.\nit unlocks once you reach Pro."
    return "this area opens later in your access path."


def what_comes_next_text() -> str:
    return (
        "what comes next \U0001f440\n\n"
        "Plus opens the next layer: bundles and individual PPVs.\n"
        "Pro is the highest tier, with the most personal products and chat access.\n\n"
        "you'll see more as your access grows."
    )


def starter_text(record: dict[str, Any]) -> str:
    if tier_rank(record.get("tier")) >= tier_rank(TIER_STARTER):
        return (
            "starter unlock \U0001f5dd\ufe0f\n\n"
            "you already opened Starter.\n"
            "that part is done.\n\n"
            "from here, the interesting step is Plus \U0001f48e"
        )
    return (
        "starter unlock \U0001f5dd\ufe0f\n\n"
        "a first private unlock before the higher tiers.\n"
        "you'll get 3 randomly selected dickpics from the vault.\n"
        "2 hard, 1 soft.\n\n"
        "price: $37.00\n"
        "instant delivery."
    )


def plus_text(record: dict[str, Any]) -> str:
    ppv_price = money_text(display_price_cents(record, "plus_ppv_stroking"))
    bundle_price = money_text(display_price_cents(record, "plus_bundle"))
    if record.get("tier") == TIER_STARTER:
        return (
            "plus preview \U0001f48e\n\n"
            "this is the next layer.\n"
            "your first Plus buy is also the step that moves you into Plus.\n\n"
            f"Best Value Bundle: {bundle_price}\n"
            "- skips straight to Pro\n\n"
            f"Premium PPVs: {ppv_price} each\n"
            "- first one moves you into Plus"
        )
    return (
        "plus menu \U0001f48e\n\n"
        "this is the mid-tier layer.\n"
        "best value bundle comes first, then Premium PPVs.\n\n"
        f"Best Value Bundle: {bundle_price}\n"
        f"Premium PPVs: {ppv_price} each"
    )


def plus_bundle_text(record: dict[str, Any]) -> str:
    price = money_text(display_price_cents(record, "plus_bundle"))
    if record.get("tier") == TIER_STARTER:
        extra = "\n\nbuying this skips straight past Plus and puts you into Pro."
    elif record.get("tier") == TIER_PLUS:
        extra = "\n\nbuying this opens Pro."
    else:
        extra = ""
    return (
        "Best Value Bundle \U0001f48e\n\n"
        "the strongest Plus option.\n"
        "includes 1 stroking video and 1 strip tease.\n\n"
        "two pieces, one cleaner unlock.\n\n"
        f"price: {price}{extra}"
    )


def plus_ppv_text(record: dict[str, Any]) -> str:
    price = money_text(display_price_cents(record, "plus_ppv_stroking"))
    if record.get("tier") == TIER_STARTER:
        suffix = "\n\nfirst Premium PPV = entry into Plus."
    elif record.get("tier") == TIER_PLUS:
        suffix = "\n\none more Plus purchase opens Pro."
    elif tier_rank(record.get("tier")) >= tier_rank(TIER_PRO):
        suffix = "\n\nas Pro, your Plus PPVs are 25% cheaper."
    else:
        suffix = ""
    return (
        "Premium PPVs \U0001f525\n\n"
        "individual Plus unlocks.\n"
        "pick the type you want.\n\n"
        f"current price: {price} each{suffix}"
    )


def pro_text() -> str:
    return (
        "pro menu \U0001f525\n\n"
        "this is the highest tier.\n"
        "the most personal items sit here.\n"
        "chat access opens at this level too."
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
        "tell me what you want first, then i approve or decline it before payment.\n"
        "available only for Pro members.\n\n"
        f"price: {money_text(display_price_cents(record, 'pro_voice_note'))}"
    )


def pro_chat_text() -> str:
    return (
        "Pro chat access \U0001f4ac\n\n"
        "since you've reached Pro, you can talk to me directly here through the bot.\n"
        "just send your message in this chat and it relays over to me.\n"
        "photos, videos, voice notes, and text all work.\n\n"
        "keep it respectful and easygoing."
    )


def purchase_lines(record: dict[str, Any]) -> list[str]:
    lines = [f"access tier: {tier_label(record.get('tier'))}"]
    purchases = record.get("purchases", [])
    if not purchases:
        lines.append("no purchases yet.")
        return lines
    status_icons = {
        "awaiting_approval": "\U0001f440",
        "awaiting_payment": "\U0001f4b8",
        "payment_claimed": "\u23f3",
        "paid": "\u2705",
        "pending_manual": "\U0001f6e0\ufe0f",
        "fulfilled": "\U0001f381",
        "declined": "\u274c",
        "payment_setup_failed": "\u26a0\ufe0f",
    }
    for purchase in reversed(purchases[-12:]):
        status = purchase.get("status", "unknown")
        title = purchase.get("title", "item")
        amount = money_text(int(purchase.get("price_cents", 0)))
        requested_at = format_dt(purchase.get("requested_at"))
        icon = status_icons.get(status, "\U0001f4cc")
        lines.append(f"{icon} {title} - {amount} - {purchase_status_label(status)} - {requested_at}")
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
    lines = [
        f"{title}\n\n"
        f"{record.get('display_name', 'Unknown')}\n"
        f"user id: {record['user_id']}\n"
        f"tier: {tier_label(record.get('tier'))}\n"
        f"item: {purchase.get('title')}\n"
        f"amount: {money_text(int(purchase.get('price_cents', 0)))}\n"
        f"status: {purchase_status_label(str(purchase.get('status') or ''))}\n"
        f"requested: {format_dt(purchase.get('requested_at'))}"
    ]
    request_text = str(purchase.get("request_text") or "").strip()
    if request_text:
        lines.append(f"request: {request_text}")
    admin_reply = str(purchase.get("admin_reply") or "").strip()
    if admin_reply:
        lines.append(f"admin reply: {admin_reply}")
    detail = str(purchase.get("delivery_summary") or "").strip()
    if detail and purchase.get("status") in {"payment_setup_failed", "pending_manual"}:
        lines.append(f"note: {detail}")
    if purchase.get("manual_payment_verified"):
        lines.append(f"manual payment verified: {format_dt(purchase.get('manual_payment_verified_at'))}")
    return "\n".join(lines)


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


def create_purchase(
    record: dict[str, Any],
    sku: str,
    *,
    status: str = "awaiting_payment",
    request_text: str | None = None,
) -> dict[str, Any]:
    purchase_id = uuid.uuid4().hex[:10]
    price_cents = display_price_cents(record, sku)
    product = CATALOG[sku]
    purchase = {
        "purchase_id": purchase_id,
        "sku": sku,
        "title": product["title"],
        "price_cents": price_cents,
        "status": status,
        "requested_at": to_iso(utc_now()),
        "paid_at": None,
        "fulfilled_at": None,
        "delivery_summary": None,
        "request_text": request_text,
        "admin_reply": None,
        "approved_at": None,
        "declined_at": None,
        "paypal_order_id": None,
        "paypal_approval_url": None,
        "paypal_invoice_id": None,
        "manual_payment_verified": False,
        "manual_payment_verified_at": None,
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


def find_purchase_by_paypal_order(state: dict[str, Any], order_id: str) -> tuple[dict[str, Any], dict[str, Any]] | None:
    order_entry = state.setdefault("paypal_orders", {}).get(order_id)
    if isinstance(order_entry, dict):
        user_id = order_entry.get("user_id")
        purchase_id = str(order_entry.get("purchase_id") or "")
        record = state.get("users", {}).get(str(user_id))
        if record is not None:
            purchase = find_purchase(record, purchase_id)
            if purchase is not None:
                return record, purchase
    for record in state.get("users", {}).values():
        for purchase in record.get("purchases", []):
            if str(purchase.get("paypal_order_id") or "") == order_id:
                return record, purchase
    return None


async def finalize_paid_purchase(
    bot: Any,
    settings: Settings,
    state: dict[str, Any],
    record: dict[str, Any],
    purchase: dict[str, Any],
) -> None:
    if purchase.get("status") in {"fulfilled", "pending_manual"}:
        return

    purchase["status"] = "paid"
    purchase["paid_at"] = purchase.get("paid_at") or to_iso(utc_now())
    order_id = str(purchase.get("paypal_order_id") or "").strip()
    if order_id:
        order_entry = state.setdefault("paypal_orders", {}).setdefault(order_id, {})
        order_entry["status"] = "completed"
        order_entry["completed_at"] = to_iso(utc_now())

    tier_before = record.get("tier")
    new_tier = apply_tier_progression(record, purchase["sku"])
    save_state(settings, state)

    if CATALOG[purchase["sku"]]["delivery_mode"] == "manual":
        purchase["status"] = "pending_manual"
        save_state(settings, state)
        await bot.send_message(
            chat_id=int(record["buyer_chat_id"]),
            text="payment confirmed.\n\nthis one is prepared manually, so i'll handle it from here.",
            reply_markup=main_menu_keyboard(),
        )
        await post_to_admin_topic(
            bot,
            settings,
            state,
            record,
            format_admin_purchase_card(record, purchase, "manual fulfillment needed"),
            reply_markup=admin_manual_fulfillment_keyboard(record["user_id"], purchase["purchase_id"]),
        )
    else:
        delivered, detail = await deliver_purchase(bot, settings, state, record, purchase)
        if delivered:
            purchase["status"] = "fulfilled"
            purchase["fulfilled_at"] = to_iso(utc_now())
            save_state(settings, state)
            await bot.send_message(
                chat_id=int(record["buyer_chat_id"]),
                text=f"payment confirmed \u2705\n\n{detail}",
                reply_markup=main_menu_keyboard(),
            )
        else:
            purchase["status"] = "pending_manual"
            purchase["delivery_summary"] = detail
            save_state(settings, state)
            await bot.send_message(
                chat_id=int(record["buyer_chat_id"]),
                text="payment confirmed.\n\nsomething needs a manual check before delivery, so i'm handling it from here.",
                reply_markup=main_menu_keyboard(),
            )
            await post_to_admin_topic(
                bot,
                settings,
                state,
                record,
                format_admin_purchase_card(record, purchase, f"delivery held up: {detail}"),
                reply_markup=admin_manual_fulfillment_keyboard(record["user_id"], purchase["purchase_id"]),
            )

    if new_tier and new_tier != tier_before:
        await notify_buyer_of_tier_change(bot, record, new_tier)


async def complete_paypal_order(settings: Settings, bot: Any, order_id: str, event: dict[str, Any]) -> str:
    state = load_state(settings)
    found = find_purchase_by_paypal_order(state, order_id)
    if found is None:
        return f"Order {order_id} not found"
    record, purchase = found
    if purchase.get("status") in {"fulfilled", "pending_manual"}:
        return f"Order {order_id} already completed"
    await finalize_paid_purchase(bot, settings, state, record, purchase)
    return f"Payment confirmed for {record.get('user_id')}"


def complete_paypal_order_from_server(order_id: str, event: dict[str, Any]) -> str:
    if PAYPAL_MAIN_LOOP is None or PAYPAL_BOT is None or PAYPAL_SETTINGS is None:
        raise RuntimeError("PayPal server is not connected to the bot.")
    future = asyncio.run_coroutine_threadsafe(
        complete_paypal_order(PAYPAL_SETTINGS, PAYPAL_BOT, order_id, event),
        PAYPAL_MAIN_LOOP,
    )
    return future.result(timeout=60)


def paypal_capture_completed(payload: dict[str, Any]) -> bool:
    if str(payload.get("status") or "").upper() == "COMPLETED":
        return True
    purchase_units = payload.get("purchase_units") or []
    for unit in purchase_units:
        payments = (unit.get("payments") or {}).get("captures") or []
        for capture in payments:
            if str(capture.get("status") or "").upper() == "COMPLETED":
                return True
    return False


def paypal_order_id_from_capture_event(event: dict[str, Any]) -> str:
    resource = event.get("resource") or {}
    related_ids = (resource.get("supplementary_data") or {}).get("related_ids") or {}
    order_id = str(related_ids.get("order_id") or "").strip()
    if not order_id:
        raise RuntimeError("PayPal event did not include an order id.")
    return order_id


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


async def relay_buyer_message(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    settings: Settings,
    state: dict[str, Any],
    record: dict[str, Any],
) -> None:
    if update.effective_chat is None or update.message is None:
        return

    topic_id = await ensure_topic(context.bot, settings, state, record)
    if topic_id is None or settings.relay_admin_group_id is None:
        await update.message.reply_text(
            "chat isn't ready on my side yet. give me a moment and try again."
        )
        return

    try:
        await context.bot.copy_message(
            chat_id=settings.relay_admin_group_id,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
            message_thread_id=topic_id,
        )
    except Exception:
        LOGGER.exception("Could not relay buyer message for user %s.", record.get("user_id"))
        await update.message.reply_text(
            "that didn't go through just now. send it one more time in a moment."
        )


async def send_voice_note_payment_prompt(
    bot: Any,
    settings: Settings,
    state: dict[str, Any],
    record: dict[str, Any],
    purchase: dict[str, Any],
) -> None:
    buyer_chat_id = int(record.get("buyer_chat_id") or 0)
    if buyer_chat_id <= 0:
        return
    _, approval_url = paypal_create_order(settings, state, record, purchase)
    request_text = str(purchase.get("request_text") or "").strip()
    await bot.send_message(
        chat_id=buyer_chat_id,
        text=(
            "voice note request approved \u2705\n\n"
            f"request: {request_text}\n"
            f"amount due: {money_text(int(purchase.get('price_cents', 0)))}\n\n"
            "if that still feels good, tap paypal. i'll update this chat when PayPal confirms it."
        ),
        reply_markup=payment_keyboard(approval_url),
    )


async def handle_pending_admin_reply(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    settings: Settings,
    state: dict[str, Any],
) -> bool:
    if update.effective_user is None or update.message is None:
        return False

    pending = pending_admin_action(state, update.effective_user.id)
    if pending is None:
        return False
    if pending.get("thread_id") != update.message.message_thread_id:
        return False

    if pending.get("kind") != "voice_decline":
        clear_pending_admin_action(state, update.effective_user.id)
        save_state(settings, state)
        return False

    decline_text = (update.message.text or "").strip()
    if not decline_text:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            message_thread_id=update.message.message_thread_id,
            text="send the decline as one text message so i can pass it on cleanly.",
        )
        return True

    record = state.get("users", {}).get(str(pending.get("user_id")))
    if record is None:
        clear_pending_admin_action(state, update.effective_user.id)
        save_state(settings, state)
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            message_thread_id=update.message.message_thread_id,
            text="that buyer record is gone, so i couldn't finish the decline.",
        )
        return True

    purchase = find_purchase(record, str(pending.get("purchase_id")))
    if purchase is None:
        clear_pending_admin_action(state, update.effective_user.id)
        save_state(settings, state)
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            message_thread_id=update.message.message_thread_id,
            text="that voice note request no longer exists.",
        )
        return True

    purchase["status"] = "declined"
    purchase["declined_at"] = to_iso(utc_now())
    purchase["admin_reply"] = decline_text
    record["updated_at"] = to_iso(utc_now())
    clear_pending_admin_action(state, update.effective_user.id)
    save_state(settings, state)

    buyer_chat_id = int(record.get("buyer_chat_id") or 0)
    if buyer_chat_id > 0:
        await context.bot.send_message(
            chat_id=buyer_chat_id,
            text=(
                "voice note request update \u274c\n\n"
                "i can't approve this one as asked.\n\n"
                f"{decline_text}"
            ),
            reply_markup=main_menu_keyboard(),
        )

    admin_chat_id = pending.get("chat_id")
    admin_message_id = pending.get("message_id")
    if isinstance(admin_chat_id, int) and isinstance(admin_message_id, int):
        await context.bot.edit_message_text(
            chat_id=admin_chat_id,
            message_id=admin_message_id,
            text=format_admin_purchase_card(record, purchase, "voice note declined"),
            reply_markup=admin_review_keyboard(record["user_id"]),
        )

    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        message_thread_id=update.message.message_thread_id,
        text="decline sent to buyer.",
    )
    return True


async def relay_admin_group_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    if update.effective_chat is None or update.effective_user is None or update.message is None:
        return
    if update.effective_chat.type != "supergroup":
        return
    if settings.relay_admin_group_id is None or update.effective_chat.id != settings.relay_admin_group_id:
        return
    if update.effective_user.is_bot:
        return

    state = load_state(settings)
    if await handle_pending_admin_reply(update, context, settings, state):
        return
    if is_internal_topic_note(update.message):
        return

    record = find_record_by_topic_id(state, update.message.message_thread_id)
    if record is None or record.get("tier") != TIER_PRO:
        return

    buyer_chat_id = int(record.get("buyer_chat_id") or 0)
    if buyer_chat_id <= 0:
        return

    try:
        await context.bot.copy_message(
            chat_id=buyer_chat_id,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
            protect_content=True,
        )
    except Exception:
        LOGGER.exception("Could not relay admin message to buyer %s.", record.get("user_id"))
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            message_thread_id=update.message.message_thread_id,
            text="that reply didn't reach the buyer. try again in a moment.",
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
        await edit_buyer_view(query, plus_text(record), plus_keyboard(record))
        return

    if destination == "plus_bundle":
        if not can_open_plus(record):
            await edit_buyer_view(query, tier_locked_text("plus", tier), simple_back_keyboard("home"))
            return
        await edit_buyer_view(query, plus_bundle_text(record), plus_bundle_keyboard(record))
        return

    if destination == "plus_ppvs":
        if not can_open_plus(record):
            await edit_buyer_view(query, tier_locked_text("plus", tier), simple_back_keyboard("home"))
            return
        await edit_buyer_view(query, plus_ppv_text(record), plus_ppv_keyboard(record))
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

    if destination == "payment_help":
        await edit_buyer_view(query, payment_help_text(), simple_back_keyboard("home"))
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
    pending_action = record.get("pending_buyer_action")

    if intake_state == "awaiting_of_username":
        record["of_username"] = update.message.text.strip()
        record["intake_state"] = "awaiting_budget"
        save_state(settings, state)
        await update.message.reply_text(
            "thanks. to route the request properly, what range are you planning for the first request?",
            reply_markup=budget_keyboard(),
        )
        return

    if isinstance(pending_action, dict) and pending_action.get("kind") == "pro_voice_request":
        request_text = update.message.text.strip()
        if not request_text:
            await update.message.reply_text("tell me what you want in the voice note first.")
            return
        clear_pending_buyer_action(record)
        purchase = create_purchase(
            record,
            "pro_voice_note",
            status="awaiting_approval",
            request_text=request_text,
        )
        save_state(settings, state)
        await update.message.reply_text(
            "got it \U0001f399\ufe0f\n\n"
            "i sent your voice note idea over for approval first.\n"
            "if it works, i'll ask for payment next.\n"
            "if not, i'll explain what i can do instead."
        )
        await post_to_admin_topic(
            context.bot,
            settings,
            state,
            record,
            format_admin_purchase_card(record, purchase, "voice note request waiting on approval"),
            reply_markup=admin_voice_request_keyboard(record["user_id"], purchase["purchase_id"]),
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
        save_state(settings, state)
        await relay_buyer_message(update, context, settings, state, record)
        return

    await update.message.reply_text("use the menu below.", reply_markup=main_menu_keyboard())


async def private_non_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    if update.effective_user is None or update.effective_chat is None or update.message is None:
        return
    if update.effective_chat.type != "private":
        return

    state = load_state(settings)
    record = get_active_private_record(state, update.effective_user.id)
    if record is None:
        return

    sync_record_identity(record, update.effective_user, update.effective_chat.id)
    pending_action = record.get("pending_buyer_action")
    if isinstance(pending_action, dict) and pending_action.get("kind") == "pro_voice_request":
        save_state(settings, state)
        await update.message.reply_text(
            "send the voice note request as text so i can review exactly what you want."
        )
        return

    save_state(settings, state)
    if record.get("tier") == TIER_PRO:
        await relay_buyer_message(update, context, settings, state, record)
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
    await finalize_paid_purchase(context.bot, settings, state, record, purchase)

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
            await query.answer("open /start to refresh your menu.", show_alert=True)
            return
        if record.get("intake_state") != "awaiting_budget":
            await query.answer("that step is already done.", show_alert=True)
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
            await query.answer("open /start to refresh your menu.", show_alert=True)
            return
        clear_pending_buyer_action(record)
        await query.answer()
        await handle_buyer_menu(query, record)
        save_state(settings, state)
        return

    if data.startswith("nav:"):
        record = get_active_private_record(state, query.from_user.id)
        if record is None:
            await query.answer("open /start to refresh your menu.", show_alert=True)
            return
        target = data.partition(":")[2]
        clear_pending_buyer_action(record)
        save_state(settings, state)
        await query.answer()
        if target == "home":
            await edit_buyer_view(query, current_home_text(record), main_menu_keyboard())
        else:
            await handle_buyer_menu(query, record)
        return

    if data.startswith("buy:"):
        record = get_active_private_record(state, query.from_user.id)
        if record is None:
            await query.answer("open /start to refresh your menu.", show_alert=True)
            return
        sku = data.partition(":")[2]
        if sku not in CATALOG:
            await query.answer("that item is no longer available.", show_alert=True)
            return

        if sku == "starter_unlock" and tier_rank(record.get("tier")) < tier_rank(TIER_VERIFIED):
            await query.answer("starter opens after verification.", show_alert=True)
            return
        if sku.startswith("plus_") and not can_open_plus(record):
            await query.answer("Plus opens after Starter.", show_alert=True)
            return
        if sku.startswith("pro_") and not can_open_pro(record):
            await query.answer("Pro opens after the Plus step.", show_alert=True)
            return
        if sku == "pro_voice_note":
            record["pending_buyer_action"] = {"kind": "pro_voice_request", "sku": sku}
            save_state(settings, state)
            await query.answer()
            await query.edit_message_text(
                text=(
                    "personal voice note \U0001f399\ufe0f\n\n"
                    "send me one message with what you want in the voice note.\n"
                    "i'll review it first before asking you to pay.\n\n"
                    "be as clear as you want about the vibe, wording, or angle."
                ),
                reply_markup=simple_back_keyboard("pro"),
            )
            return

        purchase = create_purchase(record, sku)
        try:
            _, approval_url = paypal_create_order(settings, state, record, purchase)
        except Exception as exc:
            purchase["status"] = "payment_setup_failed"
            purchase["delivery_summary"] = paypal_setup_failure_detail(exc)
            save_state(settings, state)
            await post_to_admin_topic(
                context.bot,
                settings,
                state,
                record,
                format_admin_purchase_card(record, purchase, paypal_setup_failure_title(exc)),
                reply_markup=admin_payment_fallback_keyboard(record["user_id"], purchase["purchase_id"]),
            )
            await query.answer("PayPal setup failed", show_alert=True)
            await query.edit_message_text(
                text=buyer_payment_unavailable_text(),
                reply_markup=main_menu_keyboard(),
            )
            return
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
                "tap paypal when you're ready.\n\n"
                "i'll update this chat when PayPal confirms it."
            ),
            reply_markup=payment_keyboard(approval_url),
        )
        return

    if data.startswith("paid:"):
        record = get_active_private_record(state, query.from_user.id)
        if record is None:
            await query.answer("open /start to refresh your menu.", show_alert=True)
            return
        purchase_id = data.partition(":")[2]
        purchase = find_purchase(record, purchase_id)
        if purchase is None:
            await query.answer("that purchase is no longer available.", show_alert=True)
            return
        if purchase["status"] not in {"awaiting_payment", "payment_claimed"}:
            await query.answer("that payment is already being handled.", show_alert=True)
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
                "i've marked your payment as claimed and i'm checking it now."
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
            await query.answer("that admin button is outdated.", show_alert=True)
            return
        action = parts[1]
        user_id = int(parts[2])
        record = state.get("users", {}).get(str(user_id))
        if record is None:
            await query.answer("that buyer record is no longer available.", show_alert=True)
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

        if action in {"voiceok", "voiceno"} and len(parts) == 4:
            purchase_id = parts[3]
            purchase = find_purchase(record, purchase_id)
            if purchase is None:
                await query.answer("that purchase is no longer available.", show_alert=True)
                return
            if purchase.get("sku") != "pro_voice_note":
                await query.answer("that button is for voice notes only.", show_alert=True)
                return
            if purchase["status"] not in {"awaiting_approval"}:
                await query.answer("that request was already handled.", show_alert=True)
                return
            if query.message is None or query.message.message_thread_id is None:
                await query.answer("open this from the buyer topic.", show_alert=True)
                return
            if action == "voiceok":
                purchase["status"] = "awaiting_payment"
                purchase["approved_at"] = to_iso(utc_now())
                try:
                    await send_voice_note_payment_prompt(context.bot, settings, state, record, purchase)
                except Exception as exc:
                    purchase["status"] = "payment_setup_failed"
                    purchase["delivery_summary"] = paypal_setup_failure_detail(exc)
                    save_state(settings, state)
                    await query.answer("PayPal setup failed", show_alert=True)
                    await query.edit_message_text(
                        text=format_admin_purchase_card(record, purchase, paypal_setup_failure_title(exc)),
                        reply_markup=admin_payment_fallback_keyboard(user_id, purchase_id),
                    )
                    return
                save_state(settings, state)
                await query.answer("approved")
                await query.edit_message_text(
                    text=format_admin_purchase_card(record, purchase, "voice note approved"),
                    reply_markup=admin_review_keyboard(user_id),
                )
                return
            set_pending_admin_action(
                state,
                query.from_user.id,
                {
                    "kind": "voice_decline",
                    "user_id": user_id,
                    "purchase_id": purchase_id,
                    "thread_id": query.message.message_thread_id,
                    "chat_id": query.message.chat.id,
                    "message_id": query.message.message_id,
                },
            )
            save_state(settings, state)
            await query.answer("send your decline reply")
            await context.bot.send_message(
                chat_id=query.message.chat.id,
                message_thread_id=query.message.message_thread_id,
                text=(
                    "send one text reply here with:\n"
                    "- why you're declining it\n"
                    "- what you can do instead\n\n"
                    "i'll pass that straight to the buyer."
                ),
            )
            return

        if action in {"pay", "deny", "fulfill"} and len(parts) == 4:
            purchase_id = parts[3]
            purchase = find_purchase(record, purchase_id)
            if purchase is None:
                await query.answer("that purchase is no longer available.", show_alert=True)
                return
            if action == "pay" and purchase.get("status") == "payment_setup_failed":
                purchase["manual_payment_verified"] = True
                purchase["manual_payment_verified_at"] = to_iso(utc_now())
                purchase["delivery_summary"] = "PayPal failed; admin manually verified payment."
                save_state(settings, state)
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

        await query.answer("that admin button is no longer current.", show_alert=True)


def paypal_return_page(title: str, message: str) -> str:
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


class PaypalWebhookHandler(BaseHTTPRequestHandler):
    server_version = "OliverLittleHelperV2PayPal/1.0"

    def log_message(self, format: str, *args: Any) -> None:
        LOGGER.info("paypal server %s", format % args)

    def send_text(self, status: HTTPStatus, body: str) -> None:
        data = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def send_html(self, status: HTTPStatus, body: str) -> None:
        data = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        if PAYPAL_SETTINGS is None:
            self.send_text(HTTPStatus.SERVICE_UNAVAILABLE, "PayPal server is not ready")
            return

        if self.path.startswith("/paypal/return"):
            parsed = urllib_parse.urlsplit(self.path)
            query = urllib_parse.parse_qs(parsed.query)
            order_id = str((query.get("token") or [""])[0]).strip()
            if order_id:
                try:
                    capture_payload = paypal_capture_order(PAYPAL_SETTINGS, order_id)
                    if not paypal_capture_completed(capture_payload):
                        raise RuntimeError("PayPal did not mark the capture as completed yet.")
                    complete_paypal_order_from_server(order_id, capture_payload)
                    self.send_html(
                        HTTPStatus.OK,
                        paypal_return_page(
                            "Payment confirmed",
                            "Your payment has been confirmed. You can close this page and return to Telegram.",
                        ),
                    )
                    return
                except Exception:
                    LOGGER.exception("PayPal return capture failed for order %s.", order_id)
            self.send_html(
                HTTPStatus.OK,
                paypal_return_page(
                    "Payment processing",
                    "Your payment is being confirmed. You can close this page and return to Telegram.",
                ),
            )
            return

        if self.path.startswith("/paypal/cancel"):
            self.send_html(
                HTTPStatus.OK,
                paypal_return_page(
                    "Payment cancelled",
                    "Your payment was cancelled. You can return to Telegram.",
                ),
            )
            return

        self.send_text(HTTPStatus.OK, "OK")

    def do_POST(self) -> None:
        if PAYPAL_SETTINGS is None:
            self.send_text(HTTPStatus.SERVICE_UNAVAILABLE, "PayPal server is not ready")
            return
        if not self.path.startswith("/paypal/webhook"):
            self.send_text(HTTPStatus.NOT_FOUND, "Not found")
            return
        try:
            length = int(self.headers.get("Content-Length") or "0")
        except ValueError:
            self.send_text(HTTPStatus.BAD_REQUEST, "Invalid content length")
            return
        raw_body = self.rfile.read(length)
        try:
            event = paypal_verify_webhook(PAYPAL_SETTINGS, raw_body, self.headers)
            event_type = str(event.get("event_type") or "").strip()
            if event_type != "PAYMENT.CAPTURE.COMPLETED":
                self.send_text(HTTPStatus.OK, f"Ignored {event_type or 'unknown'}")
                return
            order_id = paypal_order_id_from_capture_event(event)
            message = complete_paypal_order_from_server(order_id, event)
        except Exception as exc:
            LOGGER.exception("PayPal webhook processing failed.")
            self.send_text(HTTPStatus.BAD_REQUEST, str(exc))
            return
        self.send_text(HTTPStatus.OK, message)


def start_paypal_webhook_server(settings: Settings, loop: asyncio.AbstractEventLoop, bot: Any) -> None:
    global PAYPAL_MAIN_LOOP, PAYPAL_BOT, PAYPAL_SETTINGS, PAYPAL_WEBHOOK_SERVER, PAYPAL_WEBHOOK_THREAD
    PAYPAL_MAIN_LOOP = loop
    PAYPAL_BOT = bot
    PAYPAL_SETTINGS = settings
    if PAYPAL_WEBHOOK_SERVER is not None:
        return
    PAYPAL_WEBHOOK_SERVER = ThreadingHTTPServer(("0.0.0.0", settings.paypal_webhook_port), PaypalWebhookHandler)
    PAYPAL_WEBHOOK_THREAD = threading.Thread(
        target=PAYPAL_WEBHOOK_SERVER.serve_forever,
        name="paypal-webhook",
        daemon=True,
    )
    PAYPAL_WEBHOOK_THREAD.start()
    LOGGER.info("PayPal webhook server started on port %s", settings.paypal_webhook_port)


def stop_paypal_webhook_server() -> None:
    global PAYPAL_WEBHOOK_SERVER
    if PAYPAL_WEBHOOK_SERVER is None:
        return
    PAYPAL_WEBHOOK_SERVER.shutdown()
    PAYPAL_WEBHOOK_SERVER.server_close()
    PAYPAL_WEBHOOK_SERVER = None


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
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.TEXT & ~filters.COMMAND, private_non_text_message))
    application.add_handler(MessageHandler(filters.ChatType.SUPERGROUP & ~filters.COMMAND, relay_admin_group_message))
    return application


def main() -> None:
    settings = load_settings()
    LOGGER.info("starting v2 bot; test_only_mode=%s", settings.test_only_mode)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    application = build_application(settings)
    start_paypal_webhook_server(settings, loop, application.bot)
    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    finally:
        stop_paypal_webhook_server()


if __name__ == "__main__":
    main()
