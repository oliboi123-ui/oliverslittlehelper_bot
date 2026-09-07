"""Command-line version of the v1 import.

Most people should send the v1 `bot_state.json` to the bot as a Telegram
file attachment in the admin chat instead. The bot previews it and imports
on a button tap, with no terminal involved. This script exists for running
the same import next to the state file on a machine you already have open.

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
from pathlib import Path

import v1_migration


def default_target_state_path() -> Path:
    data_dir = (
        os.getenv("BOT_DATA_DIR", "").strip()
        or os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "").strip()
        or str(Path(__file__).resolve().parent)
    )
    return Path(data_dir) / "bot_state.json"


def empty_target() -> dict:
    return {
        "admin_chat_id": None,
        "codes": {},
        "users": {},
        "topics": {},
        "delete_permission_warned": False,
        "broadcast_drafts": {},
    }


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
    if not v1_migration.looks_like_v1_state(source):
        print(f"{source_path} has no users object. That does not look like a v1 state file.")
        return 1

    target = json.loads(target_path.read_text(encoding="utf-8")) if target_path.exists() else empty_target()
    target_users = target.setdefault("users", {})

    plan = v1_migration.plan_migration(
        source,
        target_users,
        include_leads=args.include_leads,
        overwrite=args.overwrite,
    )
    counts = plan["counts"]

    print(f"Source: {source_path}")
    print(f"Target: {target_path}")
    print()
    print(f"v1 records read: {plan['source_total']}")
    print(f"  approved and still in date: {counts['active']}")
    print(f"  approved but lapsed, expired, or revoked: {counts['paused']}")
    print(f"  never approved (leads): {counts['lead']}")
    print(f"  banned, trashed, rejected, or sandbox: {counts['drop']}")
    if plan["skipped_existing"]:
        print(f"  already present in target, left alone: {plan['skipped_existing']}")
    print()
    print(f"To migrate: {len(plan['planned'])}")
    print()

    for user_id_text, verdict, record in plan["planned"]:
        target_status = v1_migration.STATUS_ACTIVE if verdict == "active" else v1_migration.STATUS_PAUSED
        handle = str(record.get("of_username") or "").strip() or "(no handle)"
        print(f"  {user_id_text} | {v1_migration.label(record)} | {handle} | -> {target_status}")

    if not plan["planned"]:
        print("Nothing to do.")
        return 0

    if not args.apply:
        print()
        print("Preview only. Re-run with --apply to write these records.")
        return 0

    written = v1_migration.apply_migration(target_users, plan)

    if target_path.exists():
        stamp = plan["now"].strftime("%Y%m%d%H%M%S")
        backup_path = target_path.with_suffix(f".json.bak-{stamp}")
        shutil.copy2(target_path, backup_path)
        print()
        print(f"Backed up the old target to {backup_path}")

    target_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target_path.with_suffix(".json.tmp")
    temp_path.write_text(json.dumps(target, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(target_path)

    print(f"Wrote {written} customers into {target_path}")
    print()
    print("Restart the bot so it reloads the state file.")
    print("Check the result with /customers, then reach them with /broadcast customers <message>.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
