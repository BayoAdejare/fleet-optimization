import faust
import json
import logging
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
import mlflow.pyfunc
import numpy as np
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Faust application
app = faust.App(
    'fleet-stream-processor',
    broker=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka://kafka:9092'),
    store='rocksdb://',
    topic_partitions=4,
    consumer_auto_offset_reset='earliest',
    web_port=6066,
    web_host='0.0.0.0',
)

# Define Data Models
class VehicleTelemetry(faust.Record, serializer='json'):
    vehicle_id: str
    timestamp: datetime
    latitude: float
    longitude: float
    speed: float
    fuel_level: float
    engine_temp: float
    mileage: float

class PredictionResult(faust.Record, serializer='json'):
    vehicle_id: str
    timestamp: datetime
    prediction_type: str
    prediction_value: float
    confidence: float
    model_version: str

class VehicleStatus(faust.Record, serializer='json'):
    vehicle_id: str
    last_update: datetime
    current_speed: float
    avg_speed_1h: float
    total_distance: float
    maintenance_score: float

# Define Kafka Topics
telemetry_topic = app.topic('vehicle-telemetry', value_type=VehicleTelemetry)
predictions_topic = app.topic('vehicle-predictions', value_type=PredictionResult)
alerts_topic = app.topic('vehicle-alerts', value_type=dict)

# Define Faust Tables for State Management
vehicle_status_table = app.Table(
    'vehicle_status',
    default=lambda: VehicleStatus(
        vehicle_id='',
        last_update=datetime.now(),
        current_speed=0.0,
        avg_speed_1h=0.0,
        total_distance=0.0,
        maintenance_score=0.0
    ),
    partitions=4,
)

# 1-hour tumbling window for speed calculations
speed_stats_table = (
    app.Table('speed_stats', default=lambda: {'sum': 0.0, 'count': 0, 'last_update': datetime.now()})
    .tumbling(size=3600.0, expires=timedelta(hours=2))
)

# ML Model Integration
class MLModelManager:
    def __init__(self):
        self.model = None
        self.model_version = None
        self.load_model()
    
    def load_model(self):
        try:
            mlflow.set_tracking_uri(os.getenv('MLFLOW_TRACKING_URI', 'http://mlflow:5000'))
            model_name = "vehicle-maintenance-predictor"
            model_version = "latest"
            
            model_uri = f"models:/{model_name}/{model_version}"
            self.model = mlflow.pyfunc.load_model(model_uri)
            self.model_version = model_version
            logger.info(f"Loaded ML model: {model_name} version {model_version}")
        except Exception as e:
            logger.error(f"Failed to load ML model: {e}")
            self.model = None

ml_model_manager = MLModelManager()

# Database Connection
def get_db_connection():
    return psycopg2.connect(
        os.getenv('POSTGRES_URL', 'postgresql://postgres:postgres@postgres:5432/fleet'),
        cursor_factory=RealDictCursor
    )

# Faust Agents
@app.agent(telemetry_topic)
async def process_vehicle_telemetry(stream):
    async for telemetry in stream:
        try:
            logger.info(f"Processing telemetry for vehicle {telemetry.vehicle_id}")
            
            # Update vehicle status table
            await update_vehicle_status(telemetry)
            
            # Calculate statistics
            await update_speed_statistics(telemetry)
            
            # Generate predictions
            await generate_predictions(telemetry)
            
            # Check for alerts
            await check_for_alerts(telemetry)
            
        except Exception as e:
            logger.error(f"Error processing telemetry: {e}")

async def update_vehicle_status(telemetry):
    current_status = vehicle_status_table[telemetry.vehicle_id]
    
    # Calculate distance traveled (simplified)
    # In production, you'd use proper distance calculation
    distance_increment = telemetry.speed * 1.0  # Assuming 1-second intervals
    
    new_status = VehicleStatus(
        vehicle_id=telemetry.vehicle_id,
        last_update=telemetry.timestamp,
        current_speed=telemetry.speed,
        avg_speed_1h=current_status.avg_speed_1h,  # Updated separately
        total_distance=current_status.total_distance + distance_increment,
        maintenance_score=current_status.maintenance_score
    )
    
    vehicle_status_table[telemetry.vehicle_id] = new_status

async def update_speed_statistics(telemetry):
    window_key = f"{telemetry.vehicle_id}_{datetime.now().hour}"
    
    current_stats = speed_stats_table[window_key]
    new_stats = {
        'sum': current_stats['sum'] + telemetry.speed,
        'count': current_stats['count'] + 1,
        'last_update': telemetry.timestamp
    }
    
    speed_stats_table[window_key] = new_stats
    
    # Update hourly average in status table
    if new_stats['count'] > 0:
        avg_speed = new_stats['sum'] / new_stats['count']
        current_status = vehicle_status_table[telemetry.vehicle_id]
        current_status.avg_speed_1h = avg_speed

async def generate_predictions(telemetry):
    if ml_model_manager.model is None:
        return
    
    try:
        # Prepare features for ML model
        features = np.array([[
            telemetry.speed,
            telemetry.fuel_level,
            telemetry.engine_temp,
            telemetry.mileage
        ]])
        
        # Make prediction
        prediction = ml_model_manager.predict(features)
        
        # Create prediction result
        prediction_result = PredictionResult(
            vehicle_id=telemetry.vehicle_id,
            timestamp=telemetry.timestamp,
            prediction_type="maintenance_need",
            prediction_value=float(prediction[0]),
            confidence=0.95,  # Example value
            model_version=ml_model_manager.model_version
        )
        
        # Send to predictions topic
        await predictions_topic.send(value=prediction_result)
        
        # Store in database
        await store_prediction_in_db(prediction_result)
        
    except Exception as e:
        logger.error(f"Prediction error: {e}")

async def store_prediction_in_db(prediction):
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO predictions 
                (vehicle_id, timestamp, prediction_type, prediction_value, confidence, model_version)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                prediction.vehicle_id,
                prediction.timestamp,
                prediction.prediction_type,
                prediction.prediction_value,
                prediction.confidence,
                prediction.model_version
            ))
            conn.commit()
    except Exception as e:
        logger.error(f"Database storage error: {e}")
    finally:
        conn.close()

async def check_for_alerts(telemetry):
    alerts = []
    
    # Speed alert
    if telemetry.speed > 100.0:  # Example threshold
        alerts.append({
            'type': 'high_speed',
            'vehicle_id': telemetry.vehicle_id,
            'timestamp': telemetry.timestamp,
            'value': telemetry.speed,
            'threshold': 100.0
        })
    
    # Engine temperature alert
    if telemetry.engine_temp > 110.0:  # Example threshold
        alerts.append({
            'type': 'high_engine_temp',
            'vehicle_id': telemetry.vehicle_id,
            'timestamp': telemetry.timestamp,
            'value': telemetry.engine_temp,
            'threshold': 110.0
        })
    
    # Fuel level alert
    if telemetry.fuel_level < 10.0:  # Example threshold
        alerts.append({
            'type': 'low_fuel',
            'vehicle_id': telemetry.vehicle_id,
            'timestamp': telemetry.timestamp,
            'value': telemetry.fuel_level,
            'threshold': 10.0
        })
    
    # Send alerts
    for alert in alerts:
        await alerts_topic.send(value=alert)
        await store_alert_in_db(alert)

async def store_alert_in_db(alert):
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO alerts 
                (vehicle_id, timestamp, alert_type, alert_value, threshold)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                alert['vehicle_id'],
                alert['timestamp'],
                alert['type'],
                alert['value'],
                alert['threshold']
            ))
            conn.commit()
    except Exception as e:
        logger.error(f"Alert storage error: {e}")
    finally:
        conn.close()

# Timer tasks for periodic operations
@app.timer(interval=300.0)  # Every 5 minutes
async def update_ml_model():
    """Periodically check for updated ML models"""
    ml_model_manager.load_model()

@app.timer(interval=60.0)  # Every minute
async def log_status_summary():
    """Log summary of current processing state"""
    total_vehicles = len(vehicle_status_table)
    logger.info(f"Tracking {total_vehicles} vehicles in real-time")

# Web views for monitoring
@app.page('/vehicle/{vehicle_id}/')
async def get_vehicle_status(self, request, vehicle_id):
    """Web endpoint to get current vehicle status"""
    status = vehicle_status_table.get(vehicle_id)
    if status:
        return self.json({
            'vehicle_id': vehicle_id,
            'status': status._asdict(),
            'timestamp': datetime.now().isoformat()
        })
    return self.json({'error': 'Vehicle not found'}, status=404)

@app.page('/health/')
async def health_check(self, request):
    """Health check endpoint"""
    if self.model is None:
        raise RuntimeError("ML model not loaded")
    return self.json({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'vehicle_count': len(vehicle_status_table)
    })

if __name__ == '__main__':
    app.main()