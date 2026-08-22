@echo off
cd /d "%~dp0"

if not exist ".env" (
  echo The .env file is missing from this folder.
  echo Create it before starting the bot.
  pause
  exit /b 1
)

if not exist ".venv\Scripts\python.exe" (
  echo The Python environment is missing.
  echo Run setup_bot.bat first.
  pause
  exit /b 1
)

echo Starting the bot...
".venv\Scripts\python.exe" ".\gatekeeper_bot.py"
pause
