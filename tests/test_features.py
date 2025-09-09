import pytest
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
import json
from scipy import stats

# Test fixtures
@pytest.fixture
def raw_telemetry_data():
    """Generate sample raw telemetry data"""
    return pd.DataFrame({
        'vehicle_id': ['TRUCK-1234', 'TRUCK-5678', 'TRUCK-1234', 'TRUCK-9012'],
        'timestamp': [
            datetime.now() - timedelta(hours=3),
            datetime.now() - timedelta(hours=2),
            datetime.now() - timedelta(hours=1),
            datetime.now()
        ],
        'location': [
            {'latitude': 37.7749, 'longitude': -122.4194},
            {'latitude': 37.7833, 'longitude': -122.4167},
            {'latitude': 37.7855, 'longitude': -122.4012},
            {'latitude': 37.7877, 'longitude': -122.4205}
        ],
        'speed': [65.5, 70.2, np.nan, 68.7],
        'fuel_level': [0.75, 0.65, 0.60, -0.1],  # Note the invalid fuel level
        'engine_temperature': [195.6, 198.3, 197.4, 1000.0],  # Note the outlier
        'oil_pressure': [40, 35, 42, 38],
        'engine_rpm': [1500, 1600, None, 1550]
    })

@pytest.fixture
def weather_data():
    """Generate sample weather data"""
    return pd.DataFrame({
        'timestamp': [
            datetime.now() - timedelta(hours=3),
            datetime.now() - timedelta(hours=2),
            datetime.now() - timedelta(hours=1),
            datetime.now()
        ],
        'temperature': [20.5, 22.3, 21.8, 23.0],
        'humidity': [65, 70, 68, 72],
        'precipitation': [0.0, 0.2, 0.0, 0.0],
        'wind_speed': [10.5, 12.3, 11.8, 13.0]
    })

@pytest.fixture
def maintenance_records():
    """Generate sample maintenance records"""
    return pd.DataFrame({
        'vehicle_id': ['TRUCK-1234', 'TRUCK-5678', 'TRUCK-9012'],
        'last_maintenance_date': [
            datetime.now() - timedelta(days=45),
            datetime.now() - timedelta(days=30),
            datetime.now() - timedelta(days=15)
        ],
        'maintenance_type': ['full_service', 'oil_change', 'inspection'],
        'mileage_at_maintenance': [45000, 60000, 30000],
        'next_maintenance_date': [
            datetime.now() + timedelta(days=45),
            datetime.now() + timedelta(days=60),
            datetime.now() + timedelta(days=75)
        ]
    })

class TestDataCleaning:
    """Test data cleaning operations"""
    
    def test_missing_values(self, raw_telemetry_data):
        """Test handling of missing values"""
        # Check for missing values
        missing_counts = raw_telemetry_data.isnull().sum()
        
        # Test missing value handling
        cleaned_data = raw_telemetry_data.copy()
        
        # Fill numeric missing values with median
        numeric_columns = ['speed', 'engine_rpm']
        for col in numeric_columns:
            cleaned_data[col] = cleaned_data[col].fillna(cleaned_data[col].median())
        
        # Verify no missing values remain
        assert cleaned_data[numeric_columns].isnull().sum().sum() == 0
        
        # Verify values are within reasonable ranges
        assert cleaned_data['speed'].between(0, 200).all()
        assert cleaned_data['engine_rpm'].between(0, 5000).all()
    
    def test_outlier_detection(self, raw_telemetry_data):
        """Test outlier detection and handling"""
        def detect_outliers(series):
            z_scores = np.abs(stats.zscore(series.dropna()))
            return z_scores > 3
        
        # Test engine temperature outliers
        temp_data = raw_telemetry_data['engine_temperature'].dropna()
        outliers = detect_outliers(temp_data)
        
        # Verify outlier detection
        assert outliers.sum() > 0  # Should detect the 1000.0 value
        
        # Clean outliers
        cleaned_temps = temp_data[~outliers]
        assert all(cleaned_temps.between(50, 300))  # Reasonable temperature range
    
    def test_invalid_values(self, raw_telemetry_data):
        """Test handling of invalid values"""
        # Test fuel level validation and correction
        cleaned_data = raw_telemetry_data.copy()
        cleaned_data.loc[cleaned_data['fuel_level'] < 0, 'fuel_level'] = 0
        cleaned_data.loc[cleaned_data['fuel_level'] > 1, 'fuel_level'] = 1
        
        # Verify fuel levels are now valid
        assert cleaned_data['fuel_level'].between(0, 1).all()

class TestFeatureEngineering:
    """Test feature engineering operations"""
    
    def test_location_processing(self, raw_telemetry_data):
        """Test processing of location data"""
        # Extract latitude and longitude from location dictionary
        processed_data = raw_telemetry_data.copy()
        processed_data['latitude'] = processed_data['location'].apply(lambda x: x['latitude'])
        processed_data['longitude'] = processed_data['location'].apply(lambda x: x['longitude'])
        
        # Verify extraction
        assert 'latitude' in processed_data.columns
        assert 'longitude' in processed_data.columns
        assert processed_data['latitude'].between(30, 50).all()  # Reasonable US latitudes
        assert processed_data['longitude'].between(-130, -70).all()  # Reasonable US longitudes
    
    def test_temporal_features(self, raw_telemetry_data):
        """Test creation of temporal features"""
        processed_data = raw_telemetry_data.copy()
        
        # Extract time-based features
        processed_data['hour'] = processed_data['timestamp'].dt.hour
        processed_data['day_of_week'] = processed_data['timestamp'].dt.dayofweek
        processed_data['is_weekend'] = processed_data['day_of_week'].isin([5, 6])
        
        # Verify temporal features
        assert processed_data['hour'].between(0, 23).all()
        assert processed_data['day_of_week'].between(0, 6).all()
        assert processed_data['is_weekend'].dtype == bool
    
    def test_aggregated_features(self, raw_telemetry_data):
        """Test creation of aggregated features"""
        # Calculate vehicle-level aggregations
        agg_features = raw_telemetry_data.groupby('vehicle_id').agg({
            'speed': ['mean', 'max', 'std'],
            'engine_temperature': ['mean', 'max'],
            'oil_pressure': ['mean', 'min']
        })
        
        # Verify aggregations
        assert agg_features.shape[0] == raw_telemetry_data['vehicle_id'].nunique()
        assert not agg_features.isnull().any().any()

class TestDataIntegration:
    """Test integration of different data sources"""
    
    def test_weather_integration(self, raw_telemetry_data, weather_data):
        """Test integration of weather data with telemetry data"""
        # Merge weather data based on nearest timestamp
        def find_nearest_weather(telemetry_time):
            time_diff = abs(weather_data['timestamp'] - telemetry_time)
            return weather_data.iloc[time_diff.argmin()]
        
        # Add weather features
        merged_data = raw_telemetry_data.copy()
        weather_features = merged_data['timestamp'].apply(find_nearest_weather)
        
        # Verify integration
        assert weather_features.shape[0] == raw_telemetry_data.shape[0]
        assert all(col in weather_features.columns for col in weather_data.columns)
    
    def test_maintenance_integration(self, raw_telemetry_data, maintenance_records):
        """Test integration of maintenance records"""
        # Merge maintenance data
        merged_data = raw_telemetry_data.merge(
            maintenance_records[['vehicle_id', 'last_maintenance_date', 'next_maintenance_date']],
            on='vehicle_id',
            how='left'
        )
        
        # Calculate days since last maintenance
        merged_data['days_since_maintenance'] = (
            merged_data['timestamp'] - merged_data['last_maintenance_date']
        ).dt.days
        
        # Verify integration
        assert 'days_since_maintenance' in merged_data.columns
        assert merged_data['days_since_maintenance'].notna().any()

class TestDataValidation:
    """Test data validation rules"""
    
    def test_schema_validation(self, raw_telemetry_data):
        """Test data schema validation"""
        required_columns = {
            'vehicle_id': str,
            'timestamp': 'datetime64[ns]',
            'speed': 'float64',
            'fuel_level': 'float64',
            'engine_temperature': 'float64'
        }
        
        # Check column presence and types
        for col, dtype in required_columns.items():
            assert col in raw_telemetry_data.columns
            assert raw_telemetry_data[col].dtype == dtype
    
    def test_value_ranges(self, raw_telemetry_data):
        """Test value range validations"""
        range_validations = {
            'speed': (0, 200),
            'fuel_level': (0, 1),
            'engine_temperature': (50, 300),
            'oil_pressure': (0, 100),
            'engine_rpm': (0, 5000)
        }
        
        for col, (min_val, max_val) in range_validations.items():
            valid_data = raw_telemetry_data[col].dropna()
            assert valid_data.between(min_val, max_val).all(), f"Invalid values in {col}"

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
