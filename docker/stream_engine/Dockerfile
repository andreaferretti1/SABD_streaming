FROM python:3.14-slim

WORKDIR /app

COPY docker/stream_engine/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


COPY src/stream_engine .
COPY Dataset ./Dataset

ENTRYPOINT ["python", "stream_engine.py"]