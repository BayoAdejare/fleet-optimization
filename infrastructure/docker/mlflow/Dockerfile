# Use the official MLflow image as base
FROM ghcr.io/mlflow/mlflow:v3.3.1

# Install system dependencies required for psycopg2 (PostgreSQL client library)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    libpq-dev \
    gcc \
    python3-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install psycopg2 and boto3 using pip
RUN pip install --no-cache-dir psycopg2-binary boto3
