"""Bring v1 gatekeeper customers into this bot's state file.

The old bot stored one record per buyer, keyed by Telegram user id. This
bot uses the same key, so a buyer keeps working here as long as their
record is translated into this bot's shape. Migrated customers need no
access code: their record already exists, so /start and any message they
send relay straight through, and their forum topic is created the first
time they write.

Telegram permission does not travel with the record. A bot may only send a
private message to someone who pressed Start on that bot, and that consent
belongs to the bot token. Migrating records into a bot running the old
token reaches everyone immediately. Migrating into a bot on a new token
gives you their history and no way to open a conversation until each one
starts the new bot.

Usage:
    python migrate_v1_state.py <v1_bot_state.json>              # preview
    python migrate_v1_state.py <v1_bot_state.json> --apply      # write
    python migrate_v1_state.py <v1_bot_state.json> --apply --include-leads
"""

import argparse
import json
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
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


def default_target_state_path() -> Path:
    data_dir = (
        os.getenv("BOT_DATA_DIR", "").strip()
        or os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "").strip()
        or str(Path(__file__).resolve().parent)
    )
    return Path(data_dir) / "bot_state.json"


def classify(record: dict[str, Any], now: datetime) -> str:
    """Return 'active', 'paused', 'lead', or 'drop' for one v1 record."""
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
    translated = {
        "status": STATUS_ACTIVE if verdict == "active" else STATUS_PAUSED,
        # v1 collected an OnlyFans username. This bot labels the field for
        # Fansly and only uses it for topic names and display, so the handle
        # is carried across as-is.
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
    return translated


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


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrate v1 customers into this bot's state.")
    parser.add_argument("source", help="Path to the v1 bot_state.json")
    parser.add_argument("--target", help="Path to this bot's bot_state.json")
    parser.add_argument("--apply", action="store_true", help="Write the changes. Without it, preview only.")
    parser.add_argument(
        "--include-leads",
        action="store_true",
        help="Also carry over people who never got approved, as paused.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace records that already exist in the target.",
    )
    args = parser.parse_args()

    source_path = Path(args.source)
    target_path = Path(args.target) if args.target else default_target_state_path()

    if not source_path.exists():
        print(f"No v1 state file at {source_path}.")
        return 1

    source = json.loads(source_path.read_text(encoding="utf-8"))
    source_users = source.get("users", {})
    if not source_users:
        print(f"{source_path} has no users to migrate.")
        return 1

    if target_path.exists():
        target = json.loads(target_path.read_text(encoding="utf-8"))
    else:
        target = {
            "admin_chat_id": None,
            "codes": {},
            "users": {},
            "topics": {},
            "delete_permission_warned": False,
            "broadcast_drafts": {},
        }
    target_users = target.setdefault("users", {})

    now = utc_now()
    planned: list[tuple[str, str, dict[str, Any]]] = []
    skipped_existing = 0
    counts = {"active": 0, "paused": 0, "lead": 0, "drop": 0}

    for user_id_text, record in sorted(source_users.items(), key=lambda item: int(item[0])):
        verdict = classify(record, now)
        counts[verdict] += 1
        if verdict == "drop":
            continue
        if verdict == "lead" and not args.include_leads:
            continue
        if user_id_text in target_users and not args.overwrite:
            skipped_existing += 1
            continue
        planned.append((user_id_text, verdict, record))

    print(f"Source: {source_path}")
    print(f"Target: {target_path}")
    print()
    print(f"v1 records read: {len(source_users)}")
    print(f"  approved and still in date: {counts['active']}")
    print(f"  approved but lapsed, expired, or revoked: {counts['paused']}")
    print(f"  never approved (leads): {counts['lead']}")
    print(f"  banned, trashed, rejected, or sandbox: {counts['drop']}")
    if skipped_existing:
        print(f"  already present in target, left alone: {skipped_existing}")
    print()
    print(f"To migrate: {len(planned)}")
    print()

    for user_id_text, verdict, record in planned:
        target_status = STATUS_ACTIVE if verdict == "active" else STATUS_PAUSED
        handle = str(record.get("of_username") or "").strip() or "(no handle)"
        print(f"  {user_id_text} | {label(record)} | {handle} | -> {target_status}")

    if not planned:
        print("Nothing to do.")
        return 0

    if not args.apply:
        print()
        print("Preview only. Re-run with --apply to write these records.")
        return 0

    for user_id_text, verdict, record in planned:
        target_users[user_id_text] = translate(record, verdict, now)

    if target_path.exists():
        backup_path = target_path.with_suffix(f".json.bak-{now.strftime('%Y%m%d%H%M%S')}")
        shutil.copy2(target_path, backup_path)
        print()
        print(f"Backed up the old target to {backup_path}")

    target_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target_path.with_suffix(".json.tmp")
    temp_path.write_text(json.dumps(target, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(target_path)

    print(f"Wrote {len(planned)} customers into {target_path}")
    print()
    print("Restart the bot so it reloads the state file.")
    print("Check the result with /customers, then reach them with /broadcast customers <message>.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
