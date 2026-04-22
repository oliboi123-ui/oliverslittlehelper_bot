FROM python:3.14-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY gatekeeper_bot.py ./
COPY sync_onlyfans.py ./
COPY weekly_low_priority_review.py ./

CMD ["python", "-u", "gatekeeper_bot.py"]
