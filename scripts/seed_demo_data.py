#!/usr/bin/env python3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine, inspect, text
import time

def generate_sample_data():
    print("Generating sample data...")
    
    # Connect to database
    engine = create_engine('postgresql://postgres:postgres@postgres:5432/fleet')
    
    # Clear existing tables to avoid schema conflicts
    inspector = inspect(engine)
    for table_name in ['vehicle_telemetry', 'maintenance_data']:
        if inspector.has_table(table_name):
            with engine.connect() as conn:
                conn.execute(text(f"DROP TABLE {table_name}"))
                conn.commit()
    
    # Generate vehicle telemetry data
    vehicles = [f"TRUCK-{i:04d}" for i in range(1, 101)]
    end_time = datetime.now()
    start_time = end_time - timedelta(days=30)
    
    # Initialize vehicle states
    vehicle_states = {}
    for vehicle_id in vehicles:
        vehicle_states[vehicle_id] = {
            'odometer': np.random.uniform(50000, 200000),
            'last_speed': 0,
            'last_timestamp': start_time,
            'brake_state': False,
            'brake_duration': 0
        }
    
    timestamps = pd.date_range(start_time, end_time, freq='1min')
    data = []
    
    for timestamp in timestamps:
        selected_vehicles = np.random.choice(vehicles, size=5, replace=False)
        
        for vehicle_id in selected_vehicles:
            state = vehicle_states[vehicle_id]
            
            # Calculate time difference for acceleration calculation
            time_diff = (timestamp - state['last_timestamp']).total_seconds() / 3600
            
            # Generate base values
            engine_temp = np.random.normal(90, 10)
            engine_rpm = np.random.uniform(500, 2500)
            speed = np.random.uniform(0, 70)
            
            # Calculate acceleration (m/s²)
            if time_diff > 0:
                acceleration = (speed - state['last_speed']) / (time_diff * 3600)
                acceleration = max(-10, min(10, acceleration))
            else:
                acceleration = 0
            
            # Update odometer based on speed and time
            avg_speed = (speed + state['last_speed']) / 2
            distance_traveled = avg_speed * time_diff
            state['odometer'] += distance_traveled
            
            # Calculate oil pressure based on engine RPM and temperature
            base_pressure = 30
            rpm_factor = (engine_rpm / 1000) * 5
            temp_factor = (90 - engine_temp) * 0.2
            oil_pressure = base_pressure + rpm_factor + temp_factor + np.random.normal(0, 3)
            oil_pressure = max(10, min(80, oil_pressure))
            
            # Simulate brake usage
            is_braking = False
            if acceleration < -1 or speed < 5:
                is_braking = np.random.random() < 0.7
            else:
                is_braking = np.random.random() < 0.1
                
            # Update brake state and duration
            if is_braking:
                state['brake_duration'] += 60
            else:
                state['brake_duration'] = max(0, state['brake_duration'] - 30)
            
            # Normalize brake usage to a 0-100 scale
            brake_usage = min(100, state['brake_duration'] / 60)
            
            data.append({
                'timestamp': timestamp,  # Changed from 'time' to 'timestamp'
                'vehicle_id': vehicle_id,
                'engine_temp': engine_temp,
                'engine_rpm': engine_rpm,
                'oil_pressure': oil_pressure,
                'fuel_level': np.random.uniform(10, 100),
                'tire_pressure': np.random.uniform(30, 40),
                'latitude': np.random.uniform(37.7, 37.8),
                'longitude': np.random.uniform(-122.5, -122.4),
                'speed': speed,
                'odometer': state['odometer'],
                'acceleration': acceleration,
                'brake_usage': brake_usage
            })
            
            # Update vehicle state
            state['last_speed'] = speed
            state['last_timestamp'] = timestamp
            state['brake_state'] = is_braking
    
    # Create DataFrame and load to database
    df = pd.DataFrame(data)
    df.to_sql('vehicle_telemetry', engine, if_exists='replace', index=False)
    print(f"Loaded {len(df)} telemetry records")
    
    # Generate maintenance predictions
    maintenance_data = []
    maintenance_types = ['Preventive', 'Corrective', 'Predictive', 'Emergency']
    severity_levels = ['Low', 'Medium', 'High', 'Critical']
    components = ['engine', 'brakes', 'tires', 'battery', 'transmission']
    
    for vehicle_id in vehicles:
        final_odometer = vehicle_states[vehicle_id]['odometer']
        avg_brake_usage = np.mean([d['brake_usage'] for d in data if d['vehicle_id'] == vehicle_id])
        
        for days_ago in range(0, 30, 3):
            timestamp = end_time - timedelta(days=days_ago)
            maintenance_days = np.random.randint(1, 8)
            maintenance_date = timestamp + timedelta(days=maintenance_days)
            
            # Make maintenance predictions somewhat correlated with actual vehicle data
            if avg_brake_usage > 50 and np.random.random() < 0.7:
                component = 'brakes'
            else:
                component = np.random.choice(components)
            
            prediction_score = min(0.9, 0.1 + (final_odometer / 300000))
            
            maintenance_data.append({
                'timestamp': timestamp,  # Changed from 'time' to 'timestamp'
                'vehicle_id': vehicle_id,
                'prediction_score': prediction_score,
                'predicted_component': component,
                'confidence': np.random.uniform(0.7, 0.95),
                'maintenance_date': maintenance_date,
                'maintenance_type': np.random.choice(maintenance_types),
                'maintenance_severity': np.random.choice(severity_levels)
            })
    
    maintenance_df = pd.DataFrame(maintenance_data)
    maintenance_df.to_sql('maintenance_data', engine, if_exists='replace', index=False)
    print(f"Loaded {len(maintenance_df)} maintenance predictions")
    
    print("Sample data generation complete!")

if __name__ == "__main__":
    time.sleep(5)
    generate_sample_data()