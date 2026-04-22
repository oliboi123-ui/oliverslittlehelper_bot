from gatekeeper_bot import (
    format_low_priority_digest,
    load_dotenv_file,
    load_state,
    send_telegram_text,
)


def main() -> None:
    load_dotenv_file()
    state = load_state()
    admin_chat_id = state.get("admin_chat_id")
    if not admin_chat_id:
        return
    send_telegram_text(int(admin_chat_id), format_low_priority_digest(state))


if __name__ == "__main__":
    main()
