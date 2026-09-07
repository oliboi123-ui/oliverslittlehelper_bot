"""One-shot: send this bot's bot_state.json to you on Telegram.

Getting the state file off a Railway service is awkward when the dashboard
has no file browser and the service itself is crash-looping. This script
runs in place of the bot for one deploy, finds the state file, and sends it
to you as a Telegram document. You then forward that file to the new bot,
which imports it.

Set these on the service before running it:

    EXPORT_BOT_TOKEN   a token that still works (the new bot's token)
    EXPORT_CHAT_ID     optional; your Telegram user id. Left unset, the
                       admin chat id stored inside the state file is used.

Then change the service's start command to:

    python export_state.py

It sends the file once and then holds the process open, so Railway does not
restart it and send you the same file over and over. Change the start
command back when you are done.
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path


TELEGRAM_API = "https://api.telegram.org"
# Telegram refuses documents larger than this from a bot.
MAX_UPLOAD_BYTES = 50 * 1024 * 1024


def get_state_path() -> Path:
    data_dir = (
        os.getenv("BOT_DATA_DIR", "").strip()
        or os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "").strip()
        or str(Path(__file__).resolve().parent)
    )
    return Path(data_dir) / "bot_state.json"


def describe(state: dict) -> str:
    users = state.get("users", {})
    if not isinstance(users, dict):
        return "The file has no users object."
    approved = sum(1 for r in users.values() if isinstance(r, dict) and r.get("approved_at"))
    sandbox = sum(1 for r in users.values() if isinstance(r, dict) and r.get("test_mode"))
    return (
        f"records: {len(users)}, ever approved: {approved}, test-mode sandbox: {sandbox}"
    )


def build_multipart(fields: dict[str, str], file_name: str, file_bytes: bytes) -> tuple[bytes, str]:
    boundary = f"----export{uuid.uuid4().hex}"
    parts: list[bytes] = []
    for name, value in fields.items():
        parts.append(
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="{name}"\r\n\r\n'
            f"{value}\r\n".encode("utf-8")
        )
    parts.append(
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="document"; filename="{file_name}"\r\n'
        f"Content-Type: application/json\r\n\r\n".encode("utf-8")
    )
    parts.append(file_bytes)
    parts.append(f"\r\n--{boundary}--\r\n".encode("utf-8"))
    return b"".join(parts), f"multipart/form-data; boundary={boundary}"


def send_document(token: str, chat_id: int, file_name: str, file_bytes: bytes, caption: str) -> None:
    body, content_type = build_multipart(
        {"chat_id": str(chat_id), "caption": caption},
        file_name,
        file_bytes,
    )
    request = urllib.request.Request(
        f"{TELEGRAM_API}/bot{token}/sendDocument",
        data=body,
        headers={"Content-Type": content_type},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not payload.get("ok"):
        raise RuntimeError(f"Telegram refused the upload: {payload}")


def hold() -> None:
    print("Holding the process open so Railway does not restart and resend.", flush=True)
    print("Change the start command back when you have the file.", flush=True)
    while True:
        time.sleep(3600)


def main() -> int:
    state_path = get_state_path()
    print(f"Looking for the state file at {state_path}", flush=True)

    if not state_path.exists():
        print("", flush=True)
        print("There is no state file at that path.", flush=True)
        print("Nothing was ever saved to a volume here, so there are no buyers to export.", flush=True)
        listing = state_path.parent
        if listing.exists():
            print(f"What is in {listing}: {sorted(p.name for p in listing.iterdir())}", flush=True)
        hold()
        return 0

    raw = state_path.read_bytes()
    print(f"Found it: {len(raw)} bytes", flush=True)

    try:
        state = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        print(f"The file is not readable JSON: {exc}", flush=True)
        hold()
        return 1

    summary = describe(state)
    print(summary, flush=True)

    if len(raw) > MAX_UPLOAD_BYTES:
        print(f"The file is over {MAX_UPLOAD_BYTES} bytes, which Telegram will not accept.", flush=True)
        hold()
        return 1

    token = os.getenv("EXPORT_BOT_TOKEN", "").strip()
    if not token:
        print("", flush=True)
        print("Set EXPORT_BOT_TOKEN on this service to a token that still works.", flush=True)
        hold()
        return 1

    chat_id_raw = os.getenv("EXPORT_CHAT_ID", "").strip() or str(state.get("admin_chat_id") or "").strip()
    if not chat_id_raw or not chat_id_raw.lstrip("-").isdigit():
        print("", flush=True)
        print("No destination chat. Set EXPORT_CHAT_ID to your Telegram user id.", flush=True)
        hold()
        return 1

    try:
        send_document(
            token,
            int(chat_id_raw),
            "bot_state.json",
            raw,
            f"v1 state export. {summary}",
        )
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")
        print(f"Telegram rejected the send ({exc.code}): {detail}", flush=True)
        print("A 401 means the token is dead. A 400 with 'chat not found' means the", flush=True)
        print("chat id is wrong, or you have never messaged that bot.", flush=True)
        hold()
        return 1
    except Exception as exc:
        print(f"The send failed: {exc}", flush=True)
        hold()
        return 1

    print("", flush=True)
    print(f"Sent bot_state.json to chat {chat_id_raw}.", flush=True)
    print("Open Telegram, find that file, and forward it to the new bot.", flush=True)
    hold()
    return 0


if __name__ == "__main__":
    sys.exit(main())
