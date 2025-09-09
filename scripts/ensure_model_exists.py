#!/usr/bin/env python3
"""
Ensure that a predictive maintenance model exists in MLflow registry.
If not, train and register a new model.
"""

import os
import sys
import mlflow
import logging
import traceback

# Add the project root to Python path to enable importing from src
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)

# Silence Git warnings
os.environ['GIT_PYTHON_REFRESH'] = 'quiet'

# Now import from src
from src.models.train_predictive_maintenance import PredictiveMaintenanceTrainer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ensure_model.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def ensure_model_exists(model_name="vehicle-maintenance-predictor"):
    try:
        client = mlflow.tracking.MlflowClient()
        
        # Use the new API to avoid deprecation warnings
        try:
            latest_versions = client.search_model_versions(f"name='{model_name}'")
            if latest_versions:
                latest_version = sorted(latest_versions, key=lambda x: x.version, reverse=True)[0]
                logger.info(f"Model {model_name} found: version {latest_version.version}")
                return latest_version.version
        except Exception as e:
            logger.warning(f"New API failed: {str(e)}")
            
        # If we get here, model doesn't exist or we couldn't check
        logger.info(f"Model {model_name} not found, training new model...")
        trainer = PredictiveMaintenanceTrainer()
        return trainer.train_and_register_model()
                
    except Exception as e:
        logger.error(f"Error checking model: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        logger.info("Ensuring predictive maintenance model exists...")
        version = ensure_model_exists()
        logger.info(f"Model ensured: version {version}")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Failed to ensure model exists: {str(e)}")
        logger.error(traceback.format_exc())
        sys.exit(1)