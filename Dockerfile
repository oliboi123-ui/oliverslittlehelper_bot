FROM python:3.14-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY gatekeeper_bot.py ./
COPY access_digest.py ./
COPY v1_migration.py ./
COPY migrate_v1_state.py ./

CMD ["python", "-u", "gatekeeper_bot.py"]
