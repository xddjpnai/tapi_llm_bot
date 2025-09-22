FROM python:3.11-slim

WORKDIR /app

COPY services/chart_service/main.py /app/main.py

RUN pip install --no-cache-dir fastapi uvicorn tinkoff-investments mplfinance pandas matplotlib

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8002"]


