# Telegram gatekeeper-bot

Det har projektet ar byggt for att filtrera leads innan de far tillgang till din privata Telegram.

## Flode

1. En ny person startar boten.
2. Boten ber om deras OF-username.
3. Boten visar budgetknappar for planerad spend i forsta interaction.
4. Boten fragar vad personen vill kopa i den forsta interaction.
5. Budget under `$100` laggs i en lagprioriterad ko och skickas inte direkt till dig.
6. Budget `$100+` skickas till dig i Telegram for manuell review.
7. Bara vid `Approve` skickar boten ditt privata Telegram-username.

## Viktigt forst

Om en gammal bot-token har delats offentligt ska den betraktas som komprometterad.

Gor detta i `@BotFather`:

1. Oppna `@BotFather`
2. Kor `/revoke` eller skapa en ny token
3. Anvand bara den nya tokenen i Railway eller `.env`

## Filer

- `gatekeeper_bot.py` - huvudboten
- `sync_onlyfans.py` - OFAuth-sync som fornyar eller expires access
- `weekly_low_priority_review.py` - veckovis paminnelse om lagprioriterade leads
- `requirements.txt` - Python-paket
- `.env.example` - exempel pa miljo variabler
- `PRIVACY_POLICY.md` - enkel policy som kan anvandas i OFAuth setup

## Miljo variabler

```env
BOT_TOKEN=din_nya_token
ADMIN_USERNAME=ditttelegramusername
PRIVATE_TELEGRAM_USERNAME=@ditt_privata_username
ACCESS_DURATION_DAYS=30
OFAUTH_API_KEY=din_ofauth_nyckel
OFAUTH_CONNECTION_ID=din_connection_id
OFAUTH_TIMEOUT_SECONDS=10
OFAUTH_MAX_PAGES=5
```

## Admin-kommandon

- `/pending [all|low|normal|priority|expired]`
- `/approve <user_id>`
- `/reject <user_id>`
- `/priority <user_id>`
- `/lowpriority <user_id>`
- `/renew <user_id>`
- `/status <user_id>`
- `/expiring`
- `/syncsubs`

## Railway setup

Det basta upplagget ar tre services i samma Railway-projekt.

### 1. Telegram-boten

Detta ar den vanliga langkorande servicen.

Startas via `Dockerfile`:

```text
python -u gatekeeper_bot.py
```

### 2. OFAuth-sync

Cron Job som kontrollerar vilka som fortfarande ar aktiva subscribers.

Startkommando:

```text
python sync_onlyfans.py
```

Exempel pa schema tva ganger per manad:

```text
0 4 1,15 * *
```

### 3. Veckovis review av lagprioriterad ko

Cron Job som paminner dig att ga igenom leads under `$100`.

Startkommando:

```text
python weekly_low_priority_review.py
```

Exempel pa veckoschema:

```text
0 12 * * 1
```

Det betyder varje mandag klockan 12:00 UTC.

## Railway Volume

Om du vill att state ska overleva omstarter och redeploys ska du montera en Volume.

Rekommenderad mount path:

```text
/app/data
```

Alla services ska anvanda samma mount path sa att de delar samma `bot_state.json`.

## Vad OFAuth-synken gor

Nar `sync_onlyfans.py` kors:

- hamtar den aktiva subscribers fran OFAuth
- matchar dem mot sparade OF-usernames
- forlangar access 30 dagar for de som fortfarande matchar
- markerar access som `expired` for de som inte langre matchar
- skickar en sammanfattning till ditt admin-konto i Telegram
- skickar en extra varning med anvandare som du bor ta bort eller be att resubscriba

## Begransning

Automatiseringen matchar mot det OF-username som anvandaren sjalv har skrivit till boten.

Det betyder att det fortfarande finns en identitetsrisk om nagon uppger nagon annans OF-username. Den sakraste modellen ar fortfarande att forsta godkannandet ar manuellt.
