# Gatekeeper Bot

A Telegram bot that gives paying subscribers a private line to you without
exposing your personal Telegram account.

## How it works

1. Someone subscribes to your Telegram tier on Fansly.
2. You run `/newcode <their_fansly_handle>` and get back a one-time link.
3. You send that link in a Fansly DM. **This is the only link they ever get.**
4. They tap it. Access is granted, the code burns, and a forum topic named
   after them appears in your private admin group.
5. They message the bot. Their messages land in that topic. You reply in the
   topic and the bot copies it back to them. They never see your handle.

Inside a topic, customer messages are forwarded so Telegram labels them with
the customer's name, while your own replies are reposted under the bot's
name (the bot copies each one and deletes your original, which is why it
needs **Delete Messages**). Notes starting with `//` are left alone, so they
stay visibly yours and never reach the customer.

## Access lifecycle

The bot has no connection to Fansly and cannot see who is still subscribed.
It tracks one thing: how long since you last confirmed a customer is paying.
It defaults to cutting them off, and you override that with a button.

- Redeeming a link grants `ACCESS_DURATION_DAYS` (30 by default).
- `access_digest.py` runs daily. It pauses anyone whose time ran out, closes
  their topic, and DMs you a list of who lapsed and who lapses within
  `EXPIRY_WARNING_DAYS`.
- Run `/expiring` to get each of them as a message with ✅ Extend / ❌ Cut.
  Check your Fansly subscriber list, tap down the row, done.
- Extending adds time to the *current expiry*, not to today, so tapping early
  costs the customer nothing.

A paused customer keeps their topic and its full history — the topic just
closes, which makes them easy to spot in the group's topic list. Their
messages still reach you (someone messaging after a lapse is usually about to
resubscribe), and they get a "your access is paused" notice at most once a
day. Extending reopens the topic. No new link is ever needed.

`/revoke` is the harder version: access is cut and the bot stops relaying in
both directions.

`/removeuser` goes further and forgets them: their record, their topic mapping
and the access code they redeemed are all deleted. It shows you what it found
and waits for a button, and offers to delete their forum topic as well or leave
the conversation in the group. Nothing is undoable afterwards, and the next
message they send is treated as coming from a stranger with no code.

## Commands

All admin commands only work in the registered admin chat.

| Command | What it does |
| --- | --- |
| `/newcode <fansly_handle>` | Create a one-time access link |
| `/codes` | List links that haven't been used yet |
| `/customers` | People who actually bought, and their status |
| `/who <user_id>` | One customer's full details |
| `/extend <user_id> [days]` | Add time, reopening a paused topic |
| `/revoke <user_id>` | Cut access now, keeping their record |
| `/removeuser <user_id>` | Delete them from the bot entirely |
| `/leads` | Imported people who never bought, with budget and request |
| `/expiring` | Who lapses soon, with Extend/Cut buttons |
| `/broadcast <audience> <message>` | Message every customer in an audience |

Inside a customer's forum topic, a message starting with `//` stays in the
topic and is not sent to the customer.

### Broadcast

`/broadcast` sends one message to everyone in an audience, in their private
chat with the bot. Send `/broadcast` on its own to see the live headcount for
each one.

| Audience | Who it reaches |
| --- | --- |
| `customers` | Bought at least once |
| `active` | Access still running |
| `paused` | Bought before, lapsed since |
| `revoked` | Access cut |
| `leads` | Imported, never bought |
| `all` | Everyone, customers and leads |

Imported leads are stored as paused, so every customer-shaped audience
explicitly excludes them. `leads` reaches them on their own and `all` reaches
both groups at once. Leads never paid you anything, so a message written for
your customers rarely suits them — `run python test_broadcast_audiences.py` to
confirm the split still holds.

The command replies with a preview showing the audience, the recipient count,
the exact message text, and the first ten recipients. Nothing goes out until
you tap Send broadcast. Sending is paced at about twenty messages a second to
stay under Telegram's rate limit, and it retries once when Telegram asks for a
delay. Customers who blocked the bot are counted separately and stamped with
`broadcast_blocked_at`; everyone who received it gets `last_broadcast_at`.

A bot can only message someone who already started it, so a broadcast reaches
the people who redeemed an access link. It cannot reach anyone who never
opened the bot.

## Bringing v1 customers over

Send the old bot's `bot_state.json` to this bot as a file attachment, in the
admin chat. The bot reads it, replies with what it found — how many are still
in date, how many lapsed, how many were never customers — and waits. Nothing is
written until you tap **Import customers** (or **Import everyone** to include
people who never got approved). **Cancel** discards the upload.

Both bots key customers by Telegram user id, so a migrated buyer needs no
access code: `/start` and any message they send relay straight through, and
their forum topic is created the first time they write. Records already in this
bot are left alone.

Each imported record keeps the notes the old bot collected — the status it
had there, OnlyFans handle, budget range, what they asked for, payment and
subscription state, and when they first wrote in. `/who <id>` prints them under
"From the old bot", and `/leads` lists everyone who never became a customer,
with their budget and request on one line each.

**Import everyone** widens the import to leads only. Banned, trashed, rejected
and sandbox records are excluded under every button, and they are not given
old-bot notes even if a record with that id already exists here. Run
`python test_v1_migration.py` to check that still holds.

Re-sending the same file later is safe: records already here keep their access,
status and topic exactly as they are, and only their old-bot notes are
refreshed. That is also how you fill in notes on customers you imported before
this feature existed — send the file again and tap either import button.

Telegram caps bot file downloads at 20 MB. Past that, or if you would rather
run it next to the file, `migrate_v1_state.py` does the same job from a shell:

```bash
python migrate_v1_state.py path/to/v1_bot_state.json                 # preview
python migrate_v1_state.py path/to/v1_bot_state.json --apply         # write
```

It previews by default and prints every record it would touch. `--apply`
backs up the target state file first, and `--overwrite` replaces records that
already exist here.

How v1 statuses land here:

| v1 record | Here |
| --- | --- |
| `approved`, still in date | `active`, keeping its expiry |
| `approved` but lapsed, `expired`, `revoked` | `paused` |
| Unknown status | `paused` (fails closed) |
| `pending`, `low_priority`, other pre-approval states | Skipped, or `paused` with `--include-leads` |
| `banned`, `trash`, `rejected`, test-mode sandbox | Never imported, under either button |

v1 collected an OnlyFans username and this bot's field is named
`fansly_handle`. The handle is carried across as written, since the field only
drives topic names and display.

Migrated leads have no `granted_at`, so `/broadcast customers` skips them. Use
`/broadcast all` to include them.

### The part records cannot carry

Telegram lets a bot send a private message only to someone who pressed Start
on **that bot**, and that consent belongs to the bot token. Running this bot on
the old bot's token reaches every migrated customer immediately. Running it on
a new token gives you their full history and no way to open a conversation
until each of them starts the new bot. Migrating records does not move the
permission.

## Setup

### 1. The bot

Create the bot with [@BotFather](https://t.me/BotFather) and copy the token.

### 2. The admin group

1. Create a private Telegram supergroup.
2. Turn on **Topics** in group settings.
3. Add the bot as an admin with **Manage Topics** and **Delete Messages**
   permissions.
4. Get the group's ID (it starts with `-100`) and set `RELAY_ADMIN_GROUP_ID`.

### 3. Environment

Copy `.env.example` to `.env` and fill it in. In production these live in
Railway Variables instead.

| Variable | Required | Notes |
| --- | --- | --- |
| `BOT_TOKEN` | Yes | From BotFather |
| `ADMIN_USERNAME` | Yes* | Your Telegram username, no `@` |
| `ADMIN_CHAT_ID` | No | Pins the admin chat by ID instead |
| `RELAY_ADMIN_GROUP_ID` | Yes | The forum supergroup, starts with `-100` |
| `ACCESS_DURATION_DAYS` | No | Defaults to 30 |
| `EXPIRY_WARNING_DAYS` | No | Defaults to 7 |
| `BOT_DATA_DIR` | No | Where `bot_state.json` lives |

\* Either `ADMIN_USERNAME` or `ADMIN_CHAT_ID` must be set.

### 4. Register yourself

Send `/start` to the bot from your own Telegram account. It registers that
chat as the admin chat and confirms with the command list.

## Running it

Locally, on Windows:

```bash
setup_bot.bat
```

Then `start_bot.bat`. Or by hand:

```bash
py -m venv .venv && .venv/Scripts/python.exe -m pip install -r requirements.txt
```

```bash
.venv/Scripts/python.exe gatekeeper_bot.py
```

Don't run a local bot while the Railway one is live — two pollers sharing a
token fight each other.

### Railway

Two services off this repo:

- **Bot:** `python -u gatekeeper_bot.py`
- **Daily digest cron:** `python access_digest.py`

Mount a Railway Volume at `/app/data` and set `BOT_DATA_DIR=/app/data` on both
so `bot_state.json` survives redeploys. Railway deploys from `main`, so
pushing triggers a production redeploy.

## State

Everything lives in `bot_state.json`: the admin chat ID, issued codes, one
record per customer, and a topic-to-customer map. It's rewritten atomically on
every change. There's no database.

## Don't commit

`.env`, `bot_state.json`, `*.log`, and the bot token. `.gitignore` already
covers these — keep it that way.

## Upgrading from the old version

The access model changed completely, so delete any `bot_state.json` written by
the previous bot before the first run. Records with a status this version
doesn't recognise are treated as paused, which is safe but means old customers
show up needing a manual extend.
