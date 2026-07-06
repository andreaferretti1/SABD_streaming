FROM python:3.12-slim

WORKDIR /app

COPY docker/influxdb_consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


COPY src/consumer/shared ./consumer/shared
COPY src/consumer/influxdb_consumer ./consumer/influxdb_consumer

ENV PYTHONPATH=/app

ENTRYPOINT ["python", "consumer/influxdb_consumer/main.py"]