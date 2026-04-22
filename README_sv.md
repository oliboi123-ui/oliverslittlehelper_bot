# Telegram gatekeeper-bot

Det här är en enkel mall för exakt ditt flöde:

1. En ny person startar boten.
2. Boten svarar: `Please state your OF-username to continue`
3. Personen skickar sitt OF-username.
4. Boten vidarebefordrar ansökan till dig i Telegram.
5. Du jämför manuellt.
6. Du trycker `Approve` eller `Reject`.
7. Bara vid `Approve` skickar boten ditt privata Telegram-username.

## Viktigt först

Din tidigare bot-token måste betraktas som komprometterad eftersom den delades i chatten.

Gör detta först i `@BotFather`:

1. Öppna `@BotFather`
2. Kör `/revoke` eller skapa en ny token via botinställningarna
3. Använd bara den nya tokenen i `.env`

## Filer

- `gatekeeper_bot.py` - själva boten
- `requirements.txt` - Python-paket
- `.env.example` - exempel på miljövariabler
- `bot_state.json` - skapas automatiskt när boten körs

## Innan du startar

Du behöver:

- Python installerat
- ett nytt bot-token
- ditt privata Telegram-username
- ditt vanliga Telegram-username som admin

## Installera

I den här mappen:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Om du vill göra det enklare på Windows kan du i stället dubbelklicka på:

- `setup_bot.bat` - för att installera allt på en ny dator
- `start_bot.bat` - för att starta boten

Skapa sedan en `.env` baserat på `.env.example`.

Exempel:

```env
BOT_TOKEN=din_nya_token
ADMIN_USERNAME=ditttelegramusername
PRIVATE_TELEGRAM_USERNAME=@ditt_privata_username
```

## Starta boten

Ladda miljövariablerna i PowerShell och kör:

```powershell
$env:BOT_TOKEN="din_nya_token"
$env:ADMIN_USERNAME="ditttelegramusername"
$env:PRIVATE_TELEGRAM_USERNAME="@ditt_privata_username"
python .\gatekeeper_bot.py
```

På Windows kan du också bara dubbelklicka på `start_bot.bat`.

## Första admin-steget

1. Sök upp din bot i Telegram
2. Från ditt eget konto, tryck `/start`
3. Boten registrerar då din admin-chat automatiskt

Efter det skickas nya förfrågningar till dig.

## Hur du övervakar konversationer

Ja, men med en viktig begränsning:

- Du kan övervaka allt som användare skickar direkt till boten
- Du kan inte läsa deras vanliga privata Telegram-chattar med andra personer

Den här mallen övervakar genom att:

- vidarebefordra användarens OF-svar till dig
- skicka ett admin-meddelande med `Approve` och `Reject`
- låta dig se väntande ärenden med `/pending`

## Manuella admin-kommandon

- `/pending`
- `/approve <user_id>`
- `/reject <user_id>`

## Begränsningar

- Boten vet inte vem som är en "ny kontakt" för dig i Telegrams sociala mening. Den vet bara om en användare är ny för boten.
- Säkerheten bygger på din manuella kontroll av OF-username.
- Om du vill ha bättre loggning senare kan du spara ansökningar i en databas eller skicka kopior till en privat admin-kanal.

## Rekommenderad nästa nivå

Om du vill kan nästa steg vara att lägga till:

- autosvar på svenska eller engelska
- tidsstämplar på varje ansökan
- export till CSV
- blocklista
- engångsgodkännande per användare

## Köra från flera Windows-datorer

Ja, projektmappen kan ligga i exempelvis Google Drive eller OneDrive, men gör så här:

- dela eller synka själva projektmappen
- låt varje dator skapa sin egen `.venv` lokalt via `setup_bot.bat`
- ha en `.env` på varje dator med samma bot-token och admin-uppgifter

Det enklaste arbetsflödet är:

1. Öppna projektmappen på den dator du vill använda
2. Kör `setup_bot.bat` första gången på den datorn
3. Kör `start_bot.bat` när du vill starta boten

## Köra på Railway

Om du vill köra boten utan att din egen dator är igång kan du lägga projektet på Railway.

Det här projektet är nu förberett för det med en `Dockerfile`, så Railway kan starta boten direkt.

### Rekommenderat upplägg

1. Lägg projektet i ett GitHub-repo
2. Skapa ett nytt projekt i Railway
3. Importera repot
4. Lägg in dessa variabler i Railway:

```env
BOT_TOKEN=din_nya_token
ADMIN_USERNAME=ditttelegramusername
PRIVATE_TELEGRAM_USERNAME=@ditt_privata_username
```

5. Deploya tjänsten
6. Öppna boten i Telegram och kör `/start` från ditt admin-konto en gång

### Viktigt om botens minne

Den här boten sparar godkännanden och admin-chat i `bot_state.json`.

Om du vill att det ska överleva omstarter och nya deployer på Railway bör du koppla en Volume till tjänsten.

Rekommenderad mount path på Railway:

```text
/app/data
```

När en Volume är monterad där använder boten den automatiskt för `bot_state.json`.

Om du inte använder en Volume kan botens sparade status försvinna vid omstart eller redeploy.
