from gatekeeper_bot import (
    configure_logging,
    format_admin_digest,
    load_dotenv_file,
    load_state,
    log_event,
    send_telegram_text,
)


def main() -> None:
    configure_logging()
    load_dotenv_file()
    state = load_state()
    admin_chat_id = state.get("admin_chat_id")
    if not admin_chat_id:
        log_event("weekly_digest_skipped", reason="admin_not_configured")
        return
    send_telegram_text(int(admin_chat_id), format_admin_digest(state))
    log_event("weekly_digest_sent")


if __name__ == "__main__":
    main()
