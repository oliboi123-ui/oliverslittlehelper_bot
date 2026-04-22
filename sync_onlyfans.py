from gatekeeper_bot import (
    format_expired_access_alert,
    format_sync_summary,
    load_dotenv_file,
    load_state,
    save_state,
    send_telegram_text,
    sync_subscribers,
)


def main() -> None:
    load_dotenv_file()
    state = load_state()
    summary = sync_subscribers(state)
    save_state(state)

    admin_chat_id = state.get("admin_chat_id")
    if admin_chat_id:
        send_telegram_text(int(admin_chat_id), format_sync_summary(summary))
        expired_alert = format_expired_access_alert(summary)
        if expired_alert:
            send_telegram_text(int(admin_chat_id), expired_alert)

    print(format_sync_summary(summary))


if __name__ == "__main__":
    main()
