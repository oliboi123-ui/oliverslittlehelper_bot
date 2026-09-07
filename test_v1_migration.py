"""Checks on what the v1 import lets through.

Run it with: python test_v1_migration.py

No test framework, no dependencies. It exists so the exclusion rules below
stay true: banned, trashed, rejected and test-mode sandbox records must
never reach this bot, under any import button.
"""

import sys
from datetime import datetime, timezone

import v1_migration


NOW = datetime(2026, 9, 7, tzinfo=timezone.utc)

CUSTOMER = {
    "status": "approved",
    "approved_at": "2026-01-01T00:00:00+00:00",
    "expires_at": "2099-01-01T00:00:00+00:00",
    "first_name": "Customer",
}
LAPSED = {
    "status": "approved",
    "approved_at": "2025-01-01T00:00:00+00:00",
    "expires_at": "2025-02-01T00:00:00+00:00",
    "first_name": "Lapsed",
}
LEAD = {"status": "pending", "first_name": "Lead"}
BANNED = {"status": "banned", "approved_at": "2025-01-01T00:00:00+00:00"}
TRASHED = {"status": "trash", "approved_at": "2025-01-01T00:00:00+00:00"}
REJECTED = {"status": "rejected", "approved_at": "2025-01-01T00:00:00+00:00"}
SANDBOX = {"status": "approved", "test_mode": True, "approved_at": "2025-01-01T00:00:00+00:00"}

EXCLUDED_ALWAYS = {"301": BANNED, "302": TRASHED, "303": REJECTED, "304": SANDBOX}

failures: list[str] = []


def check(condition: bool, description: str) -> None:
    if condition:
        print(f"  ok   {description}")
        return
    failures.append(description)
    print(f"  FAIL {description}")


def imported_ids(source: dict, *, include_leads: bool, overwrite: bool = False) -> set[str]:
    target: dict = {}
    plan = v1_migration.plan_migration(
        source, target, include_leads=include_leads, overwrite=overwrite, now=NOW
    )
    v1_migration.apply_migration(target, plan)
    return set(target)


def main() -> int:
    source = {"users": {"101": CUSTOMER, "102": LAPSED, "201": LEAD, **EXCLUDED_ALWAYS}}

    print("classify")
    for user_id, record in EXCLUDED_ALWAYS.items():
        check(
            v1_migration.classify(record, NOW) == "drop",
            f"{user_id} ({record.get('status')}, test_mode={bool(record.get('test_mode'))}) is dropped",
        )
    check(v1_migration.classify(CUSTOMER, NOW) == "active", "an in-date customer is active")
    check(v1_migration.classify(LAPSED, NOW) == "paused", "a lapsed customer is paused")
    check(v1_migration.classify(LEAD, NOW) == "lead", "a pre-approval record is a lead")

    print("\ncustomers only")
    landed = imported_ids(source, include_leads=False)
    check(landed == {"101", "102"}, f"only the two customers land, got {sorted(landed)}")

    print("\neveryone, leads included")
    landed = imported_ids(source, include_leads=True)
    check(landed == {"101", "102", "201"}, f"customers plus the lead land, got {sorted(landed)}")
    check(
        not (landed & set(EXCLUDED_ALWAYS)),
        "no banned, trashed, rejected or sandbox record lands",
    )

    print("\neven with overwrite on")
    landed = imported_ids(source, include_leads=True, overwrite=True)
    check(
        not (landed & set(EXCLUDED_ALWAYS)),
        "overwrite does not let excluded records through",
    )

    print("\nexcluded records are never refreshed either")
    target = {user_id: {"status": "active"} for user_id in EXCLUDED_ALWAYS}
    plan = v1_migration.plan_migration(source, target, include_leads=True, now=NOW)
    refreshable_ids = {user_id for user_id, _ in plan["refreshable"]}
    check(
        not (refreshable_ids & set(EXCLUDED_ALWAYS)),
        "an excluded record already in the bot gets no v1 notes",
    )

    print()
    if failures:
        print(f"{len(failures)} check(s) failed.")
        return 1
    print("All checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
