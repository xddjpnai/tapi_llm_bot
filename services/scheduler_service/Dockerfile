FROM python:3.11-slim

WORKDIR /app

COPY shared /app/shared
COPY services/scheduler_service/main.py /app/main.py

RUN pip install --no-cache-dir aiokafka==0.10.0 python-dotenv==1.0.1 aiohttp==3.8.6

CMD ["python", "main.py"]


