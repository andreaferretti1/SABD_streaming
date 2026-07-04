FROM python:3.142-slim

WORKDIR /app

COPY docker/stream_engine/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


COPY src/csv_consumer .

ENTRYPOINT ["python", "consumer.py"]