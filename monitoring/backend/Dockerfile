FROM python:3.11-slim

WORKDIR /app

COPY fpl_full_ingest.py .

RUN pip install --no-cache-dir pandas requests psycopg2-binary

CMD ["python", "fpl_full_ingest.py"]
