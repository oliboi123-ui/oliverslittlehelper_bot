# Project Notes

Open this file first when working on this bot from a new computer or a new Codex session.

## What This Project Is

This is a Telegram gatekeeper bot for managing paid access requests.

Main behavior:

- New buyers message the bot.
- The bot asks for their OnlyFans username.
- The bot asks what they plan to spend.
- The bot asks what they want to buy.
- Low-budget requests are placed in a weekly review queue.
- Higher-budget requests are sent to the admin for approval.
- Approved buyers can be handled in direct mode or relay mode.
- OFAuth can verify subscribers and refresh or expire access.
- A PayPal payment message is sent and pinned after approval.

## Main Files

- `gatekeeper_bot.py`: main Telegram bot.
- `sync_onlyfans.py`: Railway cron entrypoint for syncing active subscribers.
- `weekly_low_priority_review.py`: Railway cron entrypoint for weekly low-priority review reminders.
- `.env.example`: safe template showing required environment variables.
- `README_sv.md`: Swedish deployment notes.
- `PRIVACY_POLICY.md`: privacy policy text used for OFAuth setup.

## Do Not Commit Secrets

Never commit these files or values:

- `.env`
- `bot_state.json`
- `*.log`
- Telegram bot token
- OFAuth API key
- OFAuth connection ID
- Railway secrets

The current `.gitignore` already excludes the local secret/state files. Keep secrets in Railway Variables for production and in a local `.env` file for local testing.

## Required Environment Variables

Production values live in Railway.

Local values, if needed, live in `.env`.

```env
BOT_TOKEN=replace_me
ADMIN_USERNAME=yourtelegramusername
PRIVATE_TELEGRAM_USERNAME=@your_private_username
RELAY_ADMIN_GROUP_ID=-1001234567890
PAYMENT_URL=https://paypal.me/mirage22m
ACCESS_DURATION_DAYS=30
OFAUTH_API_KEY=replace_me_if_using_ofauth
OFAUTH_CONNECTION_ID=replace_me_if_using_ofauth
OFAUTH_TIMEOUT_SECONDS=10
OFAUTH_MAX_PAGES=5
```

## Working Across Computers

GitHub is the source of truth for the code.

On a new computer:

1. Install Git.
2. Clone `https://github.com/oliboi123-ui/oliverslittlehelper_bot.git`.
3. Open the cloned folder in Codex.
4. Create a local `.env` only if you need to run the bot locally.
5. Run `git pull` before making changes.
6. Commit and push changes when done.

Avoid using OneDrive, Dropbox, or other cloud-drive syncing as the main project sync method. Use GitHub instead.

## Daily Edit Workflow

Before editing:

```powershell
git pull
git status --short
```

After editing:

```powershell
git status --short
git diff --check
git add <changed-files>
git commit -m "Short description"
git push origin main
```

Railway deploys from GitHub `main`, so a pushed commit can trigger a production redeploy.

## Local Test Commands

From the cloned project folder, create a virtual environment if needed:

```powershell
py -m venv .venv
& ".\.venv\Scripts\python.exe" -m pip install -r requirements.txt
```

Use the project virtual environment for checks:

```powershell
& ".\.venv\Scripts\python.exe" -m py_compile gatekeeper_bot.py
```

For local bot runs, use a local `.env` with real secrets:

```powershell
& ".\.venv\Scripts\python.exe" gatekeeper_bot.py
```

Do not run a local bot at the same time as the Railway bot unless you intentionally stopped Railway first. Two polling bots using the same token can fight each other.

## Railway Services

Recommended services:

- Main bot service: `python -u gatekeeper_bot.py`.
- OFAuth sync cron: `python sync_onlyfans.py`.
- Weekly low-priority reminder cron: `python weekly_low_priority_review.py`.

Use a Railway Volume mounted at `/app/data` if you want `bot_state.json` to survive redeploys and restarts. All related Railway services should share the same volume mount path.

## Admin Commands

- `/pending [all|low|normal|priority|expired]`
- `/approve <user_id>`
- `/approverelay <user_id>`
- `/reject <user_id>`
- `/priority <user_id>`
- `/lowpriority <user_id>`
- `/renew <user_id>`
- `/senddirect <user_id>`
- `/status <user_id>`
- `/expiring`
- `/syncsubs`
- `/verifyof <onlyfans_username>`
- `/ofdiag`

## Relay Mode

Relay mode keeps buyer conversations inside the bot instead of exposing the private Telegram handle immediately.

Setup:

1. Create a private Telegram supergroup.
2. Enable forum topics.
3. Add the bot as admin.
4. Give the bot topic management permissions.
5. Set `RELAY_ADMIN_GROUP_ID` in Railway.
6. Redeploy Railway.

When a buyer is approved with `Approve Relay`, the bot creates a forum topic for that buyer. Admin replies in that topic are copied back to the buyer. Topic messages beginning with `//` stay internal and are not sent to the buyer.

Rollback path:

- Use `/senddirect <user_id>` to give a relay buyer the direct private handle later.

## OFAuth Notes

Current expected usage:

- `subscribers:read` is required for subscriber sync and verification.
- `/verifyof <username>` checks one username.
- `/syncsubs` refreshes active subscribers and can expire access.
- `/ofdiag` gives a short pagination/connection diagnostic.
- `/ofdiag debug` gives more detail if needed.

The bot matches the OnlyFans username that the buyer typed into Telegram. This is useful, but it is not perfect identity proof by itself. Manual approval and identity-proof requests still matter.

## Current Product Decisions

- Budget under `$100` goes to low priority.
- Low-priority requests are reviewed weekly.
- Access duration defaults to 30 days.
- Approved buyers are told that access depends on an active OnlyFans subscription.
- PayPal link is `https://paypal.me/mirage22m`.
- Relay mode is preferred for testing smoother private access control.
