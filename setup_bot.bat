@echo off
cd /d "%~dp0"

where python >nul 2>nul
if errorlevel 1 (
  echo Python hittades inte. Installera Python och prova igen.
  pause
  exit /b 1
)

if not exist ".env" (
  echo Filen .env saknas i den har mappen.
  echo Skapa den forst innan du fortsatter.
  pause
  exit /b 1
)

if not exist ".venv\Scripts\python.exe" (
  echo Skapar virtuell miljo...
  python -m venv .venv
  if errorlevel 1 (
    echo Kunde inte skapa .venv
    pause
    exit /b 1
  )
)

echo Installerar eller uppdaterar paket...
".venv\Scripts\python.exe" -m pip install -r requirements.txt
if errorlevel 1 (
  echo Paketinstallationen misslyckades.
  pause
  exit /b 1
)

echo Klart. Du kan nu starta boten med start_bot.bat
pause
