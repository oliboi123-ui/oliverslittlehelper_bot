"""Daily access check.

Run this on a schedule. It pauses everyone whose access ran out, closes their
forum topics, and sends the operator a short digest of who lapsed and who is
about to. This is what makes access fail closed when the operator is busy.
"""

import asyncio

from telegram import Bot

from gatekeeper_bot import (
    format_digest,
    get_required_env,
    load_dotenv_file,
    load_state,
    save_state,
    sweep_lapsed,
)


async def run() -> None:
    load_dotenv_file()
    bot = Bot(get_required_env("BOT_TOKEN"))

    async with bot:
        state = load_state()
        newly_paused = await sweep_lapsed(bot, state)
        save_state(state)

        digest = format_digest(state, newly_paused)
        if digest is None:
            print("Access check: nothing to report.")
            return

        admin_chat_id = state.get("admin_chat_id")
        if admin_chat_id:
            await bot.send_message(chat_id=int(admin_chat_id), text=digest)
        else:
            print("No admin chat registered yet. Send /start to the bot first.")
        print(digest)


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
