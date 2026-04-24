@echo off
cd /d "%~dp0"

if not exist ".env" (
  echo Filen .env saknas i den har mappen.
  echo Skapa den innan du startar boten.
  pause
  exit /b 1
)

if not exist ".venv\Scripts\python.exe" (
  echo Python-miljon saknas.
  echo Kor setup_bot.bat forst.
  pause
  exit /b 1
)

echo Startar boten...
".venv\Scripts\python.exe" ".\gatekeeper_bot.py"
pause
