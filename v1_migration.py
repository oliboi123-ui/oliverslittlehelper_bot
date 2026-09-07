"""Translate v1 gatekeeper records into this bot's shape.

Both bots key customers by Telegram user id, so a v1 buyer keeps working
here once their record is translated. Migrated customers need no access
code: the record already exists, so /start and anything they send relay
straight through, and their forum topic is created the first time they
write.

Telegram permission is a separate matter from the record. A bot may send a
private message only to someone who pressed Start on that bot, and that
consent belongs to the bot token's bot, identified by the digits before the
colon in the token. Reissuing a token keeps the same bot and the same
consent; a bot created with /newbot is a different bot and starts with none.

This module is plain stdlib so both the bot and migrate_v1_state.py can use
it.
"""

from datetime import datetime, timezone
from typing import Any


STATUS_ACTIVE = "active"
STATUS_PAUSED = "paused"

# v1 statuses that mean this person was never a customer.
V1_LEAD_STATUSES = {
    "new",
    "pending",
    "low_priority",
    "awaiting_of_username",
    "awaiting_budget_range",
    "awaiting_purchase_intent",
    "awaiting_clarification",
}

# v1 statuses that should never be carried over.
V1_DROP_STATUSES = {"banned", "trash", "rejected"}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat()


def parse_iso(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def looks_like_v1_state(payload: Any) -> bool:
    return isinstance(payload, dict) and isinstance(payload.get("users"), dict)


def classify(record: dict[str, Any], now: datetime) -> str:
    """Return 'active', 'paused', 'lead', or 'drop' for one v1 record."""
    if not isinstance(record, dict):
        return "drop"
    status = str(record.get("status") or "").strip()
    if record.get("test_mode"):
        return "drop"
    if status in V1_DROP_STATUSES:
        return "drop"
    if status in V1_LEAD_STATUSES:
        return "lead"
    if status == "approved":
        expires_at = parse_iso(record.get("expires_at"))
        if expires_at is None or expires_at > now:
            return "active"
        return "paused"
    if status in {"expired", "revoked"}:
        return "paused"
    # An unknown status from a newer v1 build. Fail closed.
    return "paused"


def translate(record: dict[str, Any], verdict: str, now: datetime) -> dict[str, Any]:
    stamp = to_iso(now)
    return {
        "status": STATUS_ACTIVE if verdict == "active" else STATUS_PAUSED,
        # v1 collected an OnlyFans username. This bot labels the field for
        # Fansly and only uses it for topic names and display, so the handle
        # is carried across as written.
        "fansly_handle": str(record.get("of_username") or "").strip() or None,
        "code": None,
        "telegram_username": record.get("telegram_username"),
        "first_name": record.get("first_name"),
        "last_name": record.get("last_name"),
        "granted_at": record.get("approved_at"),
        "expires_at": record.get("expires_at"),
        "last_extended_at": None,
        "paused_at": None if verdict == "active" else stamp,
        "pause_notice_sent_at": None,
        "topic_id": None,
        "topic_name": None,
        "awaiting_fansly_handle": False,
        "last_broadcast_at": None,
        "broadcast_blocked_at": None,
        "migrated_from_v1_at": stamp,
    }


def label(record: dict[str, Any]) -> str:
    name = " ".join(
        part
        for part in (record.get("first_name") or "", record.get("last_name") or "")
        if str(part).strip()
    ).strip()
    if name:
        return name
    username = str(record.get("telegram_username") or "").strip()
    if username:
        return f"@{username}"
    return "Unknown"


def plan_migration(
    source: dict[str, Any],
    target_users: dict[str, Any],
    *,
    include_leads: bool = False,
    overwrite: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Work out what would be imported, changing nothing."""
    now = now or utc_now()
    source_users = source.get("users", {}) if isinstance(source, dict) else {}
    counts = {"active": 0, "paused": 0, "lead": 0, "drop": 0}
    planned: list[tuple[str, str, dict[str, Any]]] = []
    skipped_existing = 0

    def sort_key(item: tuple[str, Any]) -> int:
        try:
            return int(item[0])
        except (TypeError, ValueError):
            return 0

    for user_id_text, record in sorted(source_users.items(), key=sort_key):
        if not str(user_id_text).lstrip("-").isdigit():
            continue
        verdict = classify(record, now)
        counts[verdict] += 1
        if verdict == "drop":
            continue
        if verdict == "lead" and not include_leads:
            continue
        if user_id_text in target_users and not overwrite:
            skipped_existing += 1
            continue
        planned.append((user_id_text, verdict, record))

    return {
        "planned": planned,
        "counts": counts,
        "skipped_existing": skipped_existing,
        "source_total": len(source_users),
        "now": now,
    }


def apply_migration(target_users: dict[str, Any], plan: dict[str, Any]) -> int:
    """Write the planned records into target_users. Returns how many landed."""
    now = plan["now"]
    for user_id_text, verdict, record in plan["planned"]:
        target_users[user_id_text] = translate(record, verdict, now)
    return len(plan["planned"])
