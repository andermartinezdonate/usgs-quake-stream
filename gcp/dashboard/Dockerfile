FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the shared quake_stream package (map_layers, tectonic, etc.)
COPY src/quake_stream/ /app/quake_stream/

# Copy the dashboard
COPY gcp/dashboard/app.py /app/

ENV PYTHONUNBUFFERED=1
ENV PORT=8080

EXPOSE 8080

CMD ["streamlit", "run", "app.py", \
     "--server.port=8080", \
     "--server.address=0.0.0.0", \
     "--theme.base=dark", \
     "--server.headless=true", \
     "--browser.gatherUsageStats=false"]
