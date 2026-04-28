FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    duckdb==0.10.3 \
    boto3==1.34.100 \
    pandas==2.2.2 \
    pyarrow==16.0.0 \
    psycopg2-binary==2.9.9 \
    sqlalchemy==2.0.30

COPY transform_job.py .

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "transform_job.py"]
