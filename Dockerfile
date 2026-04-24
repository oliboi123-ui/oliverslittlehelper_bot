FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY tiered_shop_bot_v2.py ./

CMD ["python", "-u", "tiered_shop_bot_v2.py"]
