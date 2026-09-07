"""Checks on who each broadcast audience reaches.

Run it with: python test_broadcast_audiences.py

Imported leads are stored as paused, which twice made customer-shaped
views quietly count people who never bought. These checks exist so that
stays fixed.
"""

import sys

import gatekeeper_bot as bot


def customer(name: str, *, active: bool) -> dict:
    return {
        "status": "active" if active else "paused",
        "first_name": name,
        "granted_at": "2026-01-01T00:00:00+00:00",
        "expires_at": "2099-01-01T00:00:00+00:00" if active else "2025-01-01T00:00:00+00:00",
        "paused_at": None if active else "2026-02-01T00:00:00+00:00",
        "v1": {"was_customer": True},
    }


def lead(name: str) -> dict:
    return {
        "status": "paused",
        "first_name": name,
        "paused_at": "2026-09-01T00:00:00+00:00",
        "v1": {"was_customer": False},
    }


def revoked(name: str) -> dict:
    return {
        "status": "revoked",
        "first_name": name,
        "granted_at": "2025-01-01T00:00:00+00:00",
        "v1": {"was_customer": True},
    }


STATE = {
    "users": {
        "101": customer("Active One", active=True),
        "102": customer("Active Two", active=True),
        "103": customer("Lapsed", active=False),
        "104": revoked("Cut"),
        "201": lead("Lead One"),
        "202": lead("Lead Two"),
        "203": lead("Lead Three"),
    }
}

LEAD_IDS = {201, 202, 203}

EXPECTED = {
    "customers": {101, 102, 103, 104},
    "active": {101, 102},
    "paused": {103},
    "revoked": {104},
    "leads": {201, 202, 203},
    "all": {101, 102, 103, 104, 201, 202, 203},
}

failures: list[str] = []


def check(condition: bool, description: str) -> None:
    if condition:
        print(f"  ok   {description}")
        return
    failures.append(description)
    print(f"  FAIL {description}")


def main() -> int:
    print("every audience is declared and reaches exactly who it names")
    check(
        set(bot.BROADCAST_AUDIENCES) == set(EXPECTED),
        f"declared audiences match the checks, got {sorted(bot.BROADCAST_AUDIENCES)}",
    )
    for audience, expected in EXPECTED.items():
        got = {user_id for user_id, _ in bot.get_broadcast_recipients(STATE, audience)}
        check(got == expected, f"{audience} -> {sorted(expected)}, got {sorted(got)}")

    print("\nno customer-shaped audience picks up a lead")
    for audience in ("customers", "active", "paused", "revoked"):
        got = {user_id for user_id, _ in bot.get_broadcast_recipients(STATE, audience)}
        check(not (got & LEAD_IDS), f"{audience} excludes every lead")

    print("\nleads are reachable on their own, and inside all")
    leads_only = {user_id for user_id, _ in bot.get_broadcast_recipients(STATE, "leads")}
    everyone = {user_id for user_id, _ in bot.get_broadcast_recipients(STATE, "all")}
    check(leads_only == LEAD_IDS, "leads reaches the leads")
    check(LEAD_IDS <= everyone, "all includes the leads")
    check(bool(everyone - LEAD_IDS), "all includes customers too")

    print("\nan unknown audience reaches nobody")
    got = bot.get_broadcast_recipients(STATE, "nonsense")
    check(got == [], f"nonsense -> [], got {got}")

    print()
    if failures:
        print(f"{len(failures)} check(s) failed.")
        return 1
    print("All checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
