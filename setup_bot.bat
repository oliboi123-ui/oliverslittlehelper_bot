@echo off
cd /d "%~dp0"

where python >nul 2>nul
if errorlevel 1 (
  echo Python was not found. Install Python and try again.
  pause
  exit /b 1
)

if not exist ".env" (
  echo The .env file is missing from this folder.
  echo Create it before continuing.
  pause
  exit /b 1
)

if not exist ".venv\Scripts\python.exe" (
  echo Creating the virtual environment...
  python -m venv .venv
  if errorlevel 1 (
    echo Could not create .venv
    pause
    exit /b 1
  )
)

echo Installing or updating packages...
".venv\Scripts\python.exe" -m pip install -r requirements.txt
if errorlevel 1 (
  echo Package installation failed.
  pause
  exit /b 1
)

echo Done. You can now start the bot with start_bot.bat
pause
