#!/usr/bin/env python3
import boto3
import os
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_minio_bucket():
    """Create the required MinIO bucket for MLflow artifacts"""
    try:
        # Get MinIO configuration from environment
        endpoint_url = os.getenv('MLFLOW_S3_ENDPOINT_URL', 'http://minio:9000')
        access_key = os.getenv('AWS_ACCESS_KEY_ID', 'minioadmin')
        secret_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'minioadmin')
        bucket_name = os.getenv('MLFLOW_BUCKET_NAME', 'mlflow-artifacts')
        
        # Create S3 client
        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name='us-east-1'
        )
        
        # Check if bucket exists and create if it doesn't
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            logger.info(f"Bucket {bucket_name} already exists")
        except:
            s3_client.create_bucket(Bucket=bucket_name)
            logger.info(f"Created bucket {bucket_name}")
            
        return True
        
    except Exception as e:
        logger.error(f"Error creating MinIO bucket: {str(e)}")
        return False

if __name__ == "__main__":
    # Wait for MinIO to be ready
    time.sleep(5)
    max_retries = 10
    for i in range(max_retries):
        try:
            if create_minio_bucket():
                break
        except Exception as e:
            logger.warning(f"Attempt {i+1}/{max_retries} failed: {str(e)}")
            time.sleep(2)
    else:
        logger.error("Failed to create MinIO bucket after all retries")
        exit(1)