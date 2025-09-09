#!/usr/bin/env python3
"""
Predictive maintenance model training with MinIO configuration
"""

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score, accuracy_score
import mlflow
import mlflow.sklearn
from sqlalchemy import create_engine
from sklearn.exceptions import ConvergenceWarning
import warnings
import logging
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Filter warnings
warnings.filterwarnings("ignore", category=ConvergenceWarning)

class PredictiveMaintenanceTrainer:
    def __init__(self):
        self.model_name = "vehicle-maintenance-predictor"
        
        # Configure MLflow
        mlflow.set_tracking_uri("http://mlflow:5000")
        
        # Try to use MinIO if available, fall back to local storage
        try:
            # Set MinIO configuration
            os.environ['MLFLOW_S3_ENDPOINT_URL'] = "http://minio:9000"
            os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
            os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
            os.environ['AWS_REGION'] = 'us-east-1'
            os.environ['MLFLOW_S3_USE_PATH_STYLE'] = 'true'
            
            # Test MinIO connection
            import boto3
            s3 = boto3.client(
                's3',
                endpoint_url="http://minio:9000",
                aws_access_key_id="minioadmin",
                aws_secret_access_key="minioadmin"
            )
            s3.list_buckets()
            logger.info("MinIO connection successful")
            
        except ImportError:
            logger.warning("boto3 not available, using local artifact storage")
            os.environ['MLFLOW_ARTIFACT_ROOT'] = "file:///app/mlflow-artifacts"
            os.makedirs("/app/mlflow-artifacts", exist_ok=True)
        except Exception as e:
            logger.warning(f"MinIO connection failed: {str(e)}, using local storage")
            os.environ['MLFLOW_ARTIFACT_ROOT'] = "file:///app/mlflow-artifacts"
            os.makedirs("/app/mlflow-artifacts", exist_ok=True)
        
        self.mlflow_client = mlflow.tracking.MlflowClient()
        
        # Initialize database engine
        self.engine = create_engine('postgresql://postgres:postgres@postgres:5432/fleet')
        
        logger.info("Initialized PredictiveMaintenanceTrainer")
    
    def load_data(self, days=90):
        """Load training data from database"""
        try:
            logger.info(f"Loading training data for last {days} days")
            
            query = f"""
                SELECT * FROM vehicle_telemetry 
                WHERE timestamp >= NOW() - INTERVAL '{days} days'
            """
            
            df = pd.read_sql(query, self.engine)
            
            # Convert numerical columns
            numerical_columns = [
                'engine_temp', 'engine_rpm', 'oil_pressure', 'fuel_level',
                'tire_pressure', 'speed', 'odometer', 'acceleration', 'brake_usage'
            ]
            
            for col in numerical_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            df = df.dropna()
            logger.info(f"Loaded {len(df)} records for training")
            return df
            
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            raise
    
    def train_and_register_model(self):
        """Train model and register with MLflow"""
        try:
            # Load data
            df = self.load_data()
            
            # Create features and labels
            X = df[['engine_rpm', 'oil_pressure', 'brake_usage']].values
            y = (df['oil_pressure'] < 20).astype(int).values
            
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42
            )
            
            # Scale features
            scaler = StandardScaler()
            X_train_scaled = scaler.fit_transform(X_train)
            X_test_scaled = scaler.transform(X_test)
            
            # Train model
            model = LogisticRegression(max_iter=1000, random_state=42)
            model.fit(X_train_scaled, y_train)
            
            # Evaluate
            y_pred = model.predict(X_test_scaled)
            f1 = f1_score(y_test, y_pred, average='weighted')
            accuracy = accuracy_score(y_test, y_pred)
            
            logger.info(f"Model trained - F1: {f1:.4f}, Accuracy: {accuracy:.4f}")
            
            # Log to MLflow
            with mlflow.start_run():
                mlflow.log_params({
                    "max_iter": 1000,
                    "random_state": 42
                })
                mlflow.log_metrics({
                    "f1_score": f1,
                    "accuracy": accuracy
                })
                
                # Log model
                mlflow.sklearn.log_model(
                    model, 
                    "model", 
                    registered_model_name=self.model_name
                )
            
            # Get the latest version
            versions = self.mlflow_client.search_model_versions(
                f"name='{self.model_name}'"
            )
            if versions:
                return versions[0].version
            else:
                return "1"
                
        except Exception as e:
            logger.error(f"Error training model: {str(e)}")
            raise

# Usage
if __name__ == "__main__":
    trainer = PredictiveMaintenanceTrainer()
    version = trainer.train_and_register_model()
    print(f"Model registered as version {version}")