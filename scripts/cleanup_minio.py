#!/usr/bin/env python3
import boto3
import os
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def cleanup_minio_bucket():
    """Clean up old MLflow artifacts from MinIO bucket"""
    try:
        # Get MinIO configuration
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
        
        # List all objects in the bucket
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        
        if 'Contents' in response:
            # Delete objects older than 7 days
            cutoff_date = datetime.now() - timedelta(days=7)
            objects_to_delete = []
            
            for obj in response['Contents']:
                if obj['LastModified'] < cutoff_date:
                    objects_to_delete.append({'Key': obj['Key']})
            
            if objects_to_delete:
                # Delete in batches of 1000 (S3 limit)
                for i in range(0, len(objects_to_delete), 1000):
                    batch = objects_to_delete[i:i+1000]
                    s3_client.delete_objects(
                        Bucket=bucket_name,
                        Delete={'Objects': batch}
                    )
                    logger.info(f"Deleted {len(batch)} old objects")
            
            logger.info(f"Cleanup completed. {len(objects_to_delete)} objects deleted")
        else:
            logger.info("No objects found in bucket")
            
        return True
        
    except Exception as e:
        logger.error(f"Error cleaning up MinIO bucket: {str(e)}")
        return False

if __name__ == "__main__":
    cleanup_minio_bucket()