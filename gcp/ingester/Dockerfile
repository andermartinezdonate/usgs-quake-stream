FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY gcp/ingester/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the shared quake_stream package (parsers, models, dedup, geo, sources)
COPY src/quake_stream/ /app/quake_stream/

# Copy the ingester code
COPY gcp/ingester/main.py gcp/ingester/pipeline.py gcp/ingester/bq_client.py gcp/ingester/source_pipeline.py /app/

ENV PYTHONUNBUFFERED=1
ENV PORT=8080

EXPOSE 8080

CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1", "--threads", "2", "--timeout", "60", "main:app"]
