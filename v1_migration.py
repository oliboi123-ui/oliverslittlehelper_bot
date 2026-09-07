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


def v1_details(record: dict[str, Any]) -> dict[str, Any]:
    """The parts of a v1 record this bot has no column for, kept verbatim.

    Budget, purchase intent and payment state are the notes worth having in
    front of you when someone writes in, so they ride along instead of being
    dropped on import.
    """
    return {
        "status": str(record.get("status") or "").strip() or None,
        "of_username": str(record.get("of_username") or "").strip() or None,
        "budget_label": record.get("budget_range_label"),
        "budget_floor": record.get("budget_floor"),
        "review_priority": record.get("review_priority"),
        "purchase_intent": record.get("purchase_intent"),
        "payment_status": record.get("payment_status"),
        "subscription_status": record.get("subscription_status"),
        "queued_at": record.get("queued_at"),
        "approved_at": record.get("approved_at"),
        "expires_at": record.get("expires_at"),
        "was_customer": bool(record.get("approved_at")),
    }


def refresh_details(
    target_record: dict[str, Any],
    source_record: dict[str, Any],
    now: datetime | None = None,
) -> None:
    """Update an already-imported record with v1 detail, leaving access alone.

    Status, expiry, topic wiring and anything set here since the import are
    left untouched. Only the v1 notes and blank identity fields are filled.
    """
    now = now or utc_now()
    target_record["v1"] = v1_details(source_record)
    target_record["v1_refreshed_at"] = to_iso(now)
    for key in ("telegram_username", "first_name", "last_name"):
        if not target_record.get(key) and source_record.get(key):
            target_record[key] = source_record[key]
    if not target_record.get("fansly_handle"):
        handle = str(source_record.get("of_username") or "").strip()
        if handle:
            target_record["fansly_handle"] = handle


def translate(record: dict[str, Any], verdict: str, now: datetime) -> dict[str, Any]:
    stamp = to_iso(now)
    return {
        "v1": v1_details(record),
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
    refreshable: list[tuple[str, dict[str, Any]]] = []
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
            # Already here, so leave their access alone and refresh the notes.
            refreshable.append((user_id_text, record))
            continue
        planned.append((user_id_text, verdict, record))

    return {
        "planned": planned,
        "refreshable": refreshable,
        "counts": counts,
        "skipped_existing": skipped_existing,
        "source_total": len(source_users),
        "now": now,
    }


def apply_migration(target_users: dict[str, Any], plan: dict[str, Any]) -> dict[str, int]:
    """Write the planned records, and refresh notes on ones already here.

    Returns {"added": n, "refreshed": n}. Refreshing never touches access
    state, so re-sending the same file is safe.
    """
    now = plan["now"]
    for user_id_text, verdict, record in plan["planned"]:
        target_users[user_id_text] = translate(record, verdict, now)

    refreshed = 0
    for user_id_text, record in plan.get("refreshable", []):
        existing = target_users.get(user_id_text)
        if isinstance(existing, dict):
            refresh_details(existing, record, now)
            refreshed += 1

    return {"added": len(plan["planned"]), "refreshed": refreshed}
