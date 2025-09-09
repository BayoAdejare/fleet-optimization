#!/usr/bin/env python3
"""
Batch Processor for Fleet Optimization Solution.

This module handles the batch processing pipeline for fleet data, including:
- Reading raw data from Kafka topics, MinIO, and PostgreSQL
- Performing data cleaning, transformation, and feature engineering
- Writing processed data to TimescaleDB hypertables and MinIO for ML training
- Logging all processing metadata to MLflow for tracking

Author: Fleet Optimization Team
License: Apache 2.0
"""

import os
import logging
import argparse
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

import pandas as pd
from sqlalchemy import create_engine
from minio import Minio
import mlflow
from prefect import flow, task, get_run_logger

# Conditionally import distributed computing frameworks
try:
    import dask.dataframe as dd
    DASK_AVAILABLE = True
except ImportError:
    DASK_AVAILABLE = False

try:
    from pyspark.sql import SparkSession
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('batch_processor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BatchProcessor:
    """Main batch processing class for fleet data transformation."""
    
    def __init__(self, processing_engine: str = 'pandas', config: Optional[Dict[str, Any]] = None):
        """
        Initialize the batch processor with configuration.
        
        Args:
            processing_engine: Processing engine to use ('pandas', 'dask', or 'spark')
            config: Configuration dictionary for data connections
        """
        self.processing_engine = processing_engine
        self.config = config or self._load_config()
        self._validate_engine()
        
        # Initialize clients
        self.db_engine = self._init_db_connection()
        self.minio_client = self._init_minio_client()
        self.spark_session = self._init_spark_session() if processing_engine == 'spark' else None
        
        logger.info(f"Initialized BatchProcessor with {processing_engine} engine")
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from environment variables with defaults."""
        return {
            'database': {
                'host': os.getenv('TIMESCALE_HOST', 'localhost'),
                'port': os.getenv('TIMESCALE_PORT', '5432'),
                'dbname': os.getenv('TIMESCALE_DB', 'fleet_db'),
                'user': os.getenv('TIMESCALE_USER', 'postgres'),
                'password': os.getenv('TIMESCALE_PASSWORD', 'postgres')
            },
            'minio': {
                'endpoint': os.getenv('MINIO_ENDPOINT', 'localhost:9000'),
                'access_key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
                'secret_key': os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
                'secure': os.getenv('MINIO_SECURE', 'False').lower() == 'true'
            },
            'processing': {
                'raw_bucket': 'raw-data',
                'processed_bucket': 'processed-data',
                'features_bucket': 'ml-features',
                'default_lookback_days': int(os.getenv('DEFAULT_LOOKBACK_DAYS', '1'))
            }
        }
    
    def _validate_engine(self):
        """Validate that the requested processing engine is available."""
        if self.processing_engine == 'dask' and not DASK_AVAILABLE:
            logger.warning("Dask not available, falling back to pandas")
            self.processing_engine = 'pandas'
        if self.processing_engine == 'spark' and not SPARK_AVAILABLE:
            logger.warning("Spark not available, falling back to pandas")
            self.processing_engine = 'pandas'
    
    def _init_db_connection(self):
        """Initialize database connection."""
        db_config = self.config['database']
        connection_string = (
            f"postgresql://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        )
        return create_engine(connection_string)
    
    def _init_minio_client(self):
        """Initialize MinIO client."""
        minio_config = self.config['minio']
        return Minio(
            minio_config['endpoint'],
            access_key=minio_config['access_key'],
            secret_key=minio_config['secret_key'],
            secure=minio_config['secure']
        )
    
    def _init_spark_session(self):
        """Initialize Spark session for distributed processing."""
        return SparkSession.builder \
            .appName("FleetBatchProcessor") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
    
    def _get_default_dates(self):
        """Get default start and end dates (yesterday)."""
        yesterday = datetime.now() - timedelta(days=1)
        start_date = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        # Format as strings for SQL queries
        start_date_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
        end_date_str = end_date.strftime('%Y-%m-%d %H:%M:%S')
        
        return start_date_str, end_date_str
    
    @task(name="extract_telemetry_data")
    def extract_telemetry_data(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        Extract vehicle telemetry data from TimescaleDB.
        
        Args:
            start_date: Start date for data extraction (YYYY-MM-DD HH:MM:SS)
            end_date: End date for data extraction (YYYY-MM-DD HH:MM:SS)
            
        Returns:
            DataFrame containing telemetry data
        """
        # Use default dates if not provided
        if start_date is None or end_date is None:
            start_date, end_date = self._get_default_dates()
        
        logger.info(f"Extracting telemetry data from {start_date} to {end_date}")
        
        query = f"""
            SELECT 
                vehicle_id, timestamp, latitude, longitude, speed, 
                fuel_level, engine_rpm, engine_temp, oil_pressure,
                odometer, acceleration, brake_usage, created_at
            FROM vehicle_telemetry 
            WHERE timestamp BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY vehicle_id, timestamp
        """
        
        try:
            with self.db_engine.connect() as conn:
                df = pd.read_sql(query, conn)
            
            logger.info(f"Extracted {len(df)} telemetry records")
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract telemetry data: {str(e)}")
            raise
    
    @task(name="extract_weather_data")
    def extract_weather_data(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        Extract weather data from MinIO storage.
        
        Args:
            start_date: Start date for data extraction (YYYY-MM-DD)
            end_date: End date for data extraction (YYYY-MM-DD)
            
        Returns:
            DataFrame containing weather data
        """
        # Use default dates if not provided
        if start_date is None or end_date is None:
            start_date, end_date = self._get_default_dates()
            # Extract just the date part for MinIO path
            start_date = start_date.split()[0]
        
        logger.info(f"Extracting weather data from {start_date} to {end_date}")
        
        try:
            # List objects in the raw bucket for the date range
            objects = self.minio_client.list_objects(
                self.config['processing']['raw_bucket'],
                prefix=f"weather/{start_date.replace('-', '/')}",
                recursive=True
            )
            
            weather_data = []
            for obj in objects:
                # Download and read weather data files
                response = self.minio_client.get_object(
                    self.config['processing']['raw_bucket'],
                    obj.object_name
                )
                df_chunk = pd.read_parquet(response)
                weather_data.append(df_chunk)
            
            if weather_data:
                df = pd.concat(weather_data, ignore_index=True)
                logger.info(f"Extracted {len(df)} weather records")
                return df
            else:
                logger.warning("No weather data found for the specified date range")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Failed to extract weather data: {str(e)}")
            raise
    
    @task(name="transform_and_clean_data")
    def transform_and_clean_data(self, telemetry_df: pd.DataFrame, 
                                weather_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform and clean the extracted data.
        
        Args:
            telemetry_df: Raw telemetry data
            weather_df: Raw weather data
            
        Returns:
            Cleaned and transformed DataFrame
        """
        logger.info("Starting data transformation and cleaning")
        
        try:
            # Handle different processing engines
            if self.processing_engine == 'spark':
                df_processed = self._process_with_spark(telemetry_df, weather_df)
            elif self.processing_engine == 'dask':
                df_processed = self._process_with_dask(telemetry_df, weather_df)
            else:
                df_processed = self._process_with_pandas(telemetry_df, weather_df)
            
            logger.info(f"Transformed data shape: {df_processed.shape}")
            return df_processed
            
        except Exception as e:
            logger.error(f"Data transformation failed: {str(e)}")
            raise
    
    def _process_with_pandas(self, telemetry_df: pd.DataFrame, 
                           weather_df: pd.DataFrame) -> pd.DataFrame:
        """Process data using Pandas."""
        # Merge telemetry and weather data
        if not weather_df.empty:
            # Convert timestamps for merging
            telemetry_df['timestamp'] = pd.to_datetime(telemetry_df['timestamp'])
            weather_df['timestamp'] = pd.to_datetime(weather_df['timestamp'])
            
            # Merge on timestamp with nearest time match
            merged_df = pd.merge_asof(
                telemetry_df.sort_values('timestamp'),
                weather_df.sort_values('timestamp'),
                on='timestamp',
                direction='nearest'
            )
        else:
            merged_df = telemetry_df
        
        # Data cleaning
        merged_df = self._clean_data(merged_df)
        
        # Feature engineering
        merged_df = self._engineer_features(merged_df)
        
        return merged_df
    
    def _process_with_dask(self, telemetry_df: pd.DataFrame,
                         weather_df: pd.DataFrame) -> pd.DataFrame:
        """Process data using Dask (for larger datasets)."""
        if not DASK_AVAILABLE:
            raise ImportError("Dask is not available")
        
        # Convert to Dask DataFrames
        dask_telemetry = dd.from_pandas(telemetry_df, npartitions=10)
        
        if not weather_df.empty:
            dask_weather = dd.from_pandas(weather_df, npartitions=5)
            # Perform merge and processing
            merged_df = dd.merge(
                dask_telemetry,
                dask_weather,
                on='timestamp',
                how='left'
            )
        else:
            merged_df = dask_telemetry
        
        # Clean and engineer features
        merged_df = self._clean_data(merged_df)
        merged_df = self._engineer_features(merged_df)
        
        return merged_df.compute()  # Convert back to pandas
    
    def _process_with_spark(self, telemetry_df: pd.DataFrame,
                          weather_df: pd.DataFrame) -> pd.DataFrame:
        """Process data using Spark (for very large datasets)."""
        if not SPARK_AVAILABLE:
            raise ImportError("Spark is not available")
        
        # Convert to Spark DataFrames
        spark_telemetry = self.spark_session.createDataFrame(telemetry_df)
        
        if not weather_df.empty:
            spark_weather = self.spark_session.createDataFrame(weather_df)
            merged_df = spark_telemetry.join(
                spark_weather,
                on='timestamp',
                how='left'
            )
        else:
            merged_df = spark_telemetry
        
        # Perform Spark transformations
        # Note: Implement Spark-specific cleaning and feature engineering
        
        return merged_df.toPandas()  # Convert back to pandas for consistency
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean the raw data."""
        # Remove duplicates
        df = df.drop_duplicates()
        
        # Handle missing values
        numeric_cols = df.select_dtypes(include=['number']).columns
        df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].mean())
        
        # Remove outliers (e.g., impossible GPS coordinates, negative values)
        df = df[(df['latitude'].between(-90, 90)) & 
                (df['longitude'].between(-180, 180)) &
                (df['speed'] >= 0) &
                (df['fuel_level'].between(0, 100))]
        
        return df
    
    def _engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Engineer features for machine learning."""
        # Time-based features
        df['hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
        
        # Vehicle usage features
        df['distance_traveled'] = self._calculate_distance(df)
        df['fuel_consumption_rate'] = df['fuel_level'] / (df['distance_traveled'] + 1)
        
        # Driving behavior features
        df['harsh_acceleration'] = (df['acceleration'] > 2.5).astype(int)
        df['harsh_braking'] = (df['brake_usage'] > 0.8).astype(int)
        
        # Engine health features
        df['engine_stress'] = df['engine_rpm'] * df['engine_temp'] / 1000
        
        return df
    
    def _calculate_distance(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate distance traveled between consecutive points.
        This is a simplified implementation - consider using Haversine formula
        for production use with accurate GPS data.
        """
        # Group by vehicle and sort by timestamp
        df = df.sort_values(['vehicle_id', 'timestamp'])
        
        # Calculate simple Euclidean distance (for demonstration)
        # In production, use proper geospatial calculations
        df['prev_lat'] = df.groupby('vehicle_id')['latitude'].shift(1)
        df['prev_lon'] = df.groupby('vehicle_id')['longitude'].shift(1)
        
        df['distance'] = ((df['latitude'] - df['prev_lat'])**2 + 
                         (df['longitude'] - df['prev_lon'])**2)**0.5
        
        return df['distance'].fillna(0)
    
    @task(name="load_processed_data")
    def load_processed_data(self, df: pd.DataFrame, 
                          load_type: str = 'all') -> None:
        """
        Load processed data to TimescaleDB and/or MinIO.
        
        Args:
            df: Processed DataFrame to load
            load_type: Where to load data ('database', 'storage', or 'all')
        """
        logger.info(f"Loading processed data to {load_type}")
        
        try:
            if load_type in ['database', 'all']:
                self._load_to_database(df)
            
            if load_type in ['storage', 'all']:
                self._load_to_minio(df)
                
            logger.info("Data loading completed successfully")
            
        except Exception as e:
            logger.error(f"Data loading failed: {str(e)}")
            raise
    
    def _load_to_database(self, df: pd.DataFrame) -> None:
        """Load processed data to TimescaleDB."""
        # Use TimescaleDB hypertable for efficient time-series storage
        table_name = "processed_telemetry"
        
        # Ensure timestamp is in correct format
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Load to database in chunks for large datasets
        chunk_size = 10000
        for i in range(0, len(df), chunk_size):
            chunk = df[i:i + chunk_size]
            chunk.to_sql(
                table_name,
                self.db_engine,
                if_exists='append',
                index=False,
                method='multi'
            )
        
        logger.info(f"Loaded {len(df)} records to database table {table_name}")
    
    def _load_to_minio(self, df: pd.DataFrame) -> None:
        """Load processed data to MinIO object storage."""
        # Create parquet file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"processed_telemetry_{timestamp}.parquet"
        local_path = f"/tmp/{filename}"
        
        # Save locally
        df.to_parquet(local_path, index=False)
        
        # Upload to MinIO
        bucket_name = self.config['processing']['processed_bucket']
        
        # Ensure bucket exists
        if not self.minio_client.bucket_exists(bucket_name):
            self.minio_client.make_bucket(bucket_name)
        
        self.minio_client.fput_object(
            bucket_name,
            f"processed/{filename}",
            local_path
        )
        
        # Clean up local file
        os.remove(local_path)
        
        logger.info(f"Uploaded {filename} to MinIO bucket {bucket_name}")
    
    @task(name="log_processing_metadata")
    def log_processing_metadata(self, df: pd.DataFrame, 
                              start_date: str = None, 
                              end_date: str = None) -> None:
        """
        Log processing metadata to MLflow for tracking.
        
        Args:
            df: Processed DataFrame
            start_date: Start date of processing
            end_date: End date of processing
        """
        # Use default dates if not provided
        if start_date is None or end_date is None:
            start_date, end_date = self._get_default_dates()
        
        logger.info("Logging processing metadata to MLflow")
        
        try:
            # Start MLflow run
            with mlflow.start_run(run_name=f"batch_processing_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
                # Log parameters
                mlflow.log_params({
                    'processing_engine': self.processing_engine,
                    'start_date': start_date,
                    'end_date': end_date,
                    'data_source': 'timescale_db,minio'
                })
                
                # Log metrics
                mlflow.log_metrics({
                    'records_processed': len(df),
                    'unique_vehicles': df['vehicle_id'].nunique(),
                    'processing_date_range_days': (
                        pd.to_datetime(end_date) - pd.to_datetime(start_date)
                    ).days
                })
                
                # Log artifacts if needed
                # For example, sample of processed data or transformation report
                
            logger.info("MLflow logging completed")
            
        except Exception as e:
            logger.warning(f"MLflow logging failed: {str(e)}")
            # Don't raise error as MLflow failure shouldn't stop processing

@flow(name="batch_processing_pipeline")
def batch_processing_pipeline(
    start_date: str = None,
    end_date: str = None,
    processing_engine: str = 'pandas',
    load_destination: str = 'all'
) -> None:
    """
    Prefect flow for the batch processing pipeline.
    
    Args:
        start_date: Start date for processing (YYYY-MM-DD HH:MM:SS)
        end_date: End date for processing (YYYY-MM-DD HH:MM:SS)
        processing_engine: Processing engine to use
        load_destination: Where to load processed data
    """
    logger = get_run_logger()
    
    # Use default dates if not provided
    if start_date is None or end_date is None:
        processor = BatchProcessor(processing_engine=processing_engine)
        start_date, end_date = processor._get_default_dates()
    
    logger.info(f"Starting batch processing pipeline for {start_date} to {end_date}")
    
    try:
        # Initialize processor
        processor = BatchProcessor(processing_engine=processing_engine)
        
        # Extract data
        telemetry_data = processor.extract_telemetry_data(start_date, end_date)
        weather_data = processor.extract_weather_data(start_date, end_date)
        
        # Transform data
        processed_data = processor.transform_and_clean_data(telemetry_data, weather_data)
        
        # Load data
        processor.load_processed_data(processed_data, load_destination)
        
        # Log metadata
        processor.log_processing_metadata(processed_data, start_date, end_date)
        
        logger.info("Batch processing pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Batch processing pipeline failed: {str(e)}")
        raise

def main():
    """Main function for command-line execution."""
    parser = argparse.ArgumentParser(description='Fleet Optimization Batch Processor')
    parser.add_argument('--start-date', 
                       help='Start date for processing (YYYY-MM-DD)')
    parser.add_argument('--end-date', 
                       help='End date for processing (YYYY-MM-DD)')
    parser.add_argument('--engine', choices=['pandas', 'dask', 'spark'],
                       default='pandas', help='Processing engine to use')
    parser.add_argument('--load-to', choices=['database', 'storage', 'all'],
                       default='all', help='Where to load processed data')
    parser.add_argument('--prefect', action='store_true',
                       help='Run as Prefect flow')
    
    args = parser.parse_args()
    
    if args.prefect:
        # Run as Prefect flow
        batch_processing_pipeline(
            start_date=args.start_date,
            end_date=args.end_date,
            processing_engine=args.engine,
            load_destination=args.load_to
        )
    else:
        # Run directly
        processor = BatchProcessor(processing_engine=args.engine)
        
        # Get default dates if not provided
        if args.start_date is None or args.end_date is None:
            start_date, end_date = processor._get_default_dates()
        else:
            start_date, end_date = args.start_date, args.end_date
        
        telemetry_data = processor.extract_telemetry_data(start_date, end_date)
        weather_data = processor.extract_weather_data(start_date, end_date)
        processed_data = processor.transform_and_clean_data(telemetry_data, weather_data)
        processor.load_processed_data(processed_data, args.load_to)
        processor.log_processing_metadata(processed_data, start_date, end_date)
        
        logger.info("Batch processing completed successfully")

if __name__ == "__main__":
    main()