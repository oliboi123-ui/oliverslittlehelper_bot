import json
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

from gatekeeper_bot import (
    format_sync_summary,
    get_required_env,
    load_dotenv_file,
    load_state,
    save_state,
    sync_subscribers,
)


def send_admin_message(chat_id: int, text: str) -> None:
    token = get_required_env("BOT_TOKEN")
    payload = urllib_parse.urlencode(
        {
            "chat_id": str(chat_id),
            "text": text,
        }
    ).encode("utf-8")
    request = urllib_request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urllib_request.urlopen(request, timeout=30) as response:
            json.loads(response.read().decode("utf-8"))
    except urllib_error.URLError as exc:
        raise RuntimeError(f"Could not notify admin in Telegram: {exc.reason}") from exc


def main() -> None:
    load_dotenv_file()
    state = load_state()
    summary = sync_subscribers(state)
    save_state(state)

    admin_chat_id = state.get("admin_chat_id")
    if admin_chat_id:
        send_admin_message(int(admin_chat_id), format_sync_summary(summary))

    print(format_sync_summary(summary))


if __name__ == "__main__":
    main()
