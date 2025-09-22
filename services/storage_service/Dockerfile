FROM python:3.11-slim

WORKDIR /app

COPY shared /app/shared
COPY services/storage_service/main.py /app/main.py

RUN pip install --no-cache-dir fastapi==0.110.0 uvicorn==0.30.0 SQLAlchemy==2.0.35 psycopg2-binary==2.9.10 aiokafka==0.10.0 python-dotenv==1.0.1 pydantic==1.10.15

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]


