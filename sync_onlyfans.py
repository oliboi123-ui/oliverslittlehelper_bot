import logging

from gatekeeper_bot import (
    configure_logging,
    format_expired_access_alert,
    format_sync_summary,
    load_dotenv_file,
    load_state,
    log_event,
    save_state,
    send_telegram_text,
    sync_subscribers,
)


def main() -> None:
    configure_logging()
    load_dotenv_file()
    state = load_state()
    log_event("ofauth_sync_started", trigger="cron")
    try:
        summary = sync_subscribers(state)
    except Exception:
        log_event("ofauth_sync_failed", logging.ERROR, trigger="cron")
        raise
    save_state(state)
    log_event(
        "ofauth_sync_completed",
        trigger="cron",
        active_seen=summary.get("active_subscribers_seen"),
        matched=summary.get("matched"),
        renewed=summary.get("renewed"),
        expired=summary.get("expired"),
        inactive=summary.get("inactive"),
        partial=bool(summary.get("warnings")),
    )

    admin_chat_id = state.get("admin_chat_id")
    if admin_chat_id:
        send_telegram_text(int(admin_chat_id), format_sync_summary(summary))
        expired_alert = format_expired_access_alert(summary)
        if expired_alert:
            send_telegram_text(int(admin_chat_id), expired_alert)

if __name__ == "__main__":
    main()
