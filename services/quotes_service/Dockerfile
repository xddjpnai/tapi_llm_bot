FROM python:3.11-slim

WORKDIR /app

COPY shared /app/shared
# shared integration used
COPY services/quotes_service/main.py /app/main.py
COPY services/quotes_service/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]


