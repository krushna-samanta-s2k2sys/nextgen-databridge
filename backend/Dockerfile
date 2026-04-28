FROM python:3.11-slim

WORKDIR /app

# System deps for pyodbc / Oracle
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ unixodbc-dev libpq-dev curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Make all packages importable from /app
ENV PYTHONPATH=/app:/app/models:/app/services:/app/api

EXPOSE 8000

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
