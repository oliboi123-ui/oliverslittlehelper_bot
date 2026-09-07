"""Print the buyers stored in bot_state.json.

Run this where bot_state.json lives (locally, or in a Railway shell on the
volume) to see who the bot can reach in a DM.

Usage:
    python list_users.py
    python list_users.py buyers
    python list_users.py approved --csv users.csv
"""

import argparse
import json
import os
import sys
from pathlib import Path


AUDIENCES = ("all", "buyers", "approved", "expired", "paid")
COLUMNS = (
    "user_id",
    "status",
    "telegram_username",
    "name",
    "of_username",
    "budget",
    "approved_at",
    "expires_at",
    "payment_status",
    "last_broadcast_at",
)


def get_state_path() -> Path:
    data_dir = (
        os.getenv("BOT_DATA_DIR", "").strip()
        or os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "").strip()
        or str(Path(__file__).resolve().parent)
    )
    return Path(data_dir) / "bot_state.json"


def matches(audience: str, record: dict) -> bool:
    status = record.get("status")
    if status in {"banned", "trash"}:
        return False
    if audience == "all":
        return True
    if audience == "approved":
        return status == "approved"
    if audience == "expired":
        return status == "expired"
    if audience == "paid":
        return record.get("payment_status") == "paid" or bool(record.get("payment_confirmed_at"))
    if audience == "buyers":
        return bool(record.get("approved_at")) or status in {"approved", "expired"}
    return False


def row(user_id: str, record: dict) -> list[str]:
    name = " ".join(
        part
        for part in (record.get("first_name") or "", record.get("last_name") or "")
        if str(part).strip()
    ).strip()
    username = str(record.get("telegram_username") or "").strip()
    return [
        user_id,
        str(record.get("status") or ""),
        f"@{username}" if username else "",
        name,
        str(record.get("of_username") or ""),
        str(record.get("budget_range_label") or ""),
        str(record.get("approved_at") or ""),
        str(record.get("expires_at") or ""),
        str(record.get("payment_status") or ""),
        str(record.get("last_broadcast_at") or ""),
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description="List buyers stored in bot_state.json.")
    parser.add_argument("audience", nargs="?", default="all", choices=AUDIENCES)
    parser.add_argument("--state", help="Path to bot_state.json.")
    parser.add_argument("--csv", help="Also write the rows to this CSV file.")
    args = parser.parse_args()

    state_path = Path(args.state) if args.state else get_state_path()
    if not state_path.exists():
        print(f"No state file at {state_path}.")
        print("Run this next to bot_state.json, or pass --state <path>.")
        return 1

    state = json.loads(state_path.read_text(encoding="utf-8"))
    rows = [
        row(user_id, record)
        for user_id, record in sorted(state.get("users", {}).items(), key=lambda item: int(item[0]))
        if not record.get("test_mode") and matches(args.audience, record)
    ]

    print(f"State file: {state_path}")
    print(f"Audience: {args.audience}")
    print(f"Users: {len(rows)}\n")

    widths = [
        max(len(COLUMNS[index]), *(len(r[index]) for r in rows)) if rows else len(COLUMNS[index])
        for index in range(len(COLUMNS))
    ]
    header = "  ".join(column.ljust(widths[index]) for index, column in enumerate(COLUMNS))
    print(header)
    print("-" * len(header))
    for r in rows:
        print("  ".join(value.ljust(widths[index]) for index, value in enumerate(r)))

    if args.csv:
        import csv

        with open(args.csv, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(COLUMNS)
            writer.writerows(rows)
        print(f"\nWrote {args.csv}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
