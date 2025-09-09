import os
from typing import Optional

class Config:
    """Application configuration"""
    
    def __init__(self):
        self.kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        self.postgres_url = os.getenv("POSTGRES_URL", "postgresql://postgres:postgres@postgres:5432/fleet")
        self.minio_url = os.getenv("MINIO_URL", "http://minio:9000")
        self.minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        self.mlflow_tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")

# Global config instance
config = Config()