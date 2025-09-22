FROM python:3.11-slim

WORKDIR /app

COPY shared /app/shared
COPY services/bot_gateway/main.py /app/main.py
COPY services/bot_gateway/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]


