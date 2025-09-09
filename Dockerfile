FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy ALL files needed for package installation
COPY . .

# Install the package in editable mode (now with all files available)
RUN pip install -e .

ENV PYTHONPATH=/app/src:$PYTHONPATH

# Create a non-root user
RUN useradd --create-home --shell /bin/bash app
USER app

EXPOSE 8000 8050

CMD ["python", "src/api/app.py"]