FROM python:3.11-slim

WORKDIR /app

# System deps for psycopg2, lxml, pandas
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libpq-dev libxml2-dev libxslt1-dev && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY templates/ templates/

RUN mkdir -p /app/data

ENV DATA_DIR=/app/data
ENV PYTHONUNBUFFERED=1
ENV SYNC_INTERVAL_SECONDS=604800

EXPOSE 8080

CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "2", "--timeout", "300", "app:app"]
