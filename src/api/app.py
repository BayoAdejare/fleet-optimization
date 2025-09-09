# src/api/app.py
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import minio
import mlflow
from typing import List, Optional
import json
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(title="Fleet Analytics API", version="1.0.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection dependency
def get_db_connection():
    """
    Create a connection to PostgreSQL database
    """
    try:
        conn = psycopg2.connect(
            app.state.postgres_url,
            cursor_factory=RealDictCursor
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

# MinIO client dependency
def get_minio_client():
    """
    Create a MinIO client instance
    """
    try:
        client = minio.Minio(
            app.state.minio_url.replace("http://", ""),
            access_key=app.state.minio_access_key,
            secret_key=app.state.minio_secret_key,
            secure=False
        )
        return client
    except Exception as e:
        logger.error(f"MinIO client creation error: {e}")
        raise HTTPException(status_code=500, detail="MinIO client initialization failed")

@app.on_event("startup")
async def startup_event():
    """
    Initialize application state and connections
    """
    # Initialize MLflow tracking
    mlflow.set_tracking_uri(app.state.mlflow_tracking_uri)
    
    logger.info("Application startup complete")

@app.get("/")
async def root():
    """
    Root endpoint returning API status
    """
    return {
        "status": "active",
        "message": "Fleet Analytics API is running",
        "version": "1.0.0"
    }

@app.get("/health")
async def health_check():
    """
    Health check endpoint for all dependencies
    """
    dependencies_status = {}
    
    # Check PostgreSQL connection
    try:
        conn = get_db_connection()
        conn.close()
        dependencies_status["postgres"] = "connected"
    except Exception as e:
        dependencies_status["postgres"] = f"error: {str(e)}"
    
    # Check MinIO connection
    try:
        client = get_minio_client()
        client.list_buckets()
        dependencies_status["minio"] = "connected"
    except Exception as e:
        dependencies_status["minio"] = f"error: {str(e)}"
    
    # Check MLflow connection
    try:
        experiments = mlflow.search_experiments()
        dependencies_status["mlflow"] = f"connected ({len(experiments)} experiments)"
    except Exception as e:
        dependencies_status["mlflow"] = f"error: {str(e)}"
    
    return {
        "status": "healthy",
        "dependencies": dependencies_status
    }

@app.get("/api/vehicles")
async def get_vehicles(limit: int = 100, db=Depends(get_db_connection)):
    """
    Get list of vehicles from the database
    """
    try:
        with db.cursor() as cur:
            cur.execute("SELECT * FROM vehicles LIMIT %s", (limit,))
            vehicles = cur.fetchall()
        return {"vehicles": vehicles, "count": len(vehicles)}
    except Exception as e:
        logger.error(f"Error fetching vehicles: {e}")
        raise HTTPException(status_code=500, detail="Error fetching vehicles")

@app.get("/api/telemetry/latest")
async def get_latest_telemetry(vehicle_id: Optional[str] = None, db=Depends(get_db_connection)):
    """
    Get latest telemetry data for vehicles
    """
    try:
        with db.cursor() as cur:
            if vehicle_id:
                cur.execute("""
                    SELECT * FROM vehicle_telemetry 
                    WHERE vehicle_id = %s 
                    ORDER BY timestamp DESC LIMIT 100
                """, (vehicle_id,))
            else:
                cur.execute("""
                    SELECT DISTINCT ON (vehicle_id) *
                    FROM vehicle_telemetry 
                    ORDER BY vehicle_id, timestamp DESC
                """)
            telemetry = cur.fetchall()
        return {"telemetry": telemetry, "count": len(telemetry)}
    except Exception as e:
        logger.error(f"Error fetching telemetry: {e}")
        raise HTTPException(status_code=500, detail="Error fetching telemetry data")

@app.get("/api/predictions")
async def get_predictions(model_version: Optional[str] = None, db=Depends(get_db_connection)):
    """
    Get predictions from the database, optionally filtered by model version
    """
    try:
        with db.cursor() as cur:
            if model_version:
                cur.execute("""
                    SELECT * FROM predictions 
                    WHERE model_version = %s 
                    ORDER BY prediction_time DESC LIMIT 100
                """, (model_version,))
            else:
                cur.execute("""
                    SELECT * FROM predictions 
                    ORDER BY prediction_time DESC LIMIT 100
                """)
            predictions = cur.fetchall()
        return {"predictions": predictions, "count": len(predictions)}
    except Exception as e:
        logger.error(f"Error fetching predictions: {e}")
        raise HTTPException(status_code=500, detail="Error fetching predictions")

@app.get("/api/mlflow/models")
async def get_mlflow_models():
    """
    Get list of registered models from MLflow
    """
    try:
        models = mlflow.search_registered_models()
        model_list = [{
            "name": model.name,
            "latest_versions": [{
                "version": version.version,
                "current_stage": version.current_stage,
                "description": version.description
            } for version in model.latest_versions]
        } for model in models]
        
        return {"models": model_list, "count": len(model_list)}
    except Exception as e:
        logger.error(f"Error fetching MLflow models: {e}")
        raise HTTPException(status_code=500, detail="Error fetching MLflow models")

@app.get("/api/minio/files")
async def list_minio_files(bucket_name: str = "fleet-data", minio_client=Depends(get_minio_client)):
    """
    List files in a MinIO bucket
    """
    try:
        objects = minio_client.list_objects(bucket_name, recursive=True)
        files = [{
            "name": obj.object_name,
            "size": obj.size,
            "last_modified": obj.last_modified
        } for obj in objects]
        
        return {"bucket": bucket_name, "files": files, "count": len(files)}
    except Exception as e:
        logger.error(f"Error listing MinIO files: {e}")
        raise HTTPException(status_code=500, detail=f"Error listing files in bucket {bucket_name}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)