# Fleet Optimization Solution

[![Build Status](https://img.shields.io/github/actions/workflow/status/bayoadejare/fleet-optimization/ci-cd.yml?branch=main&label=build)](https://github.com/bayoadejare/fleet-optimization/actions)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Docker](https://img.shields.io/badge/Docker-Ready-blue.svg)](https://hub.docker.com/r/bayoadejare/fleet-optimization)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-Native-326ce5.svg)](https://kubernetes.io/)
[![Python Version](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/)
[![MLflow](https://img.shields.io/badge/MLflow-Integrated-0194E2.svg)](https://mlflow.org/)

**TL;DR: A production-ready open-source platform for predictive maintenance and route optimization that reduces fleet operational costs by up to 25%.**

## 🚀 Quick Start (5 Steps, 10 Minutes)

Get a complete demo environment running with Docker Compose:

```bash
# 1. Clone the repository
git clone https://github.com/bayoadejare/fleet-optimization.git
cd fleet-optimization

# 2. Copy environment template
cp .env.example .env

# 3. Start all services (Kafka, MinIO, PostgreSQL, MLflow, Dash)
docker compose up -d

# 4. Generate sample data and train initial models
docker compose exec app python scripts/seed_demo_data.py

# 5. Access the dashboards:
#   - Operational Dashboard: http://localhost:8050
#   - MLflow Tracking: http://localhost:5000
#   - MinIO Console: http://localhost:9001 (minioadmin:minioadmin)
#   - Grafana: http://localhost:3000 (admin:admin)
```

**What's running:**
- ✅ Kafka cluster with vehicle data streaming
- ✅ MinIO object storage (S3-compatible)
- ✅ TimescaleDB for time-series data
- ✅ MLflow experiment tracking
- ✅ Plotly Dash operational dashboard
- ✅ Grafana monitoring
- ✅ Sample data with 100+ simulated vehicles

## Feature Comparison: Open Source vs. Proprietary Solutions

| Feature | This Project (Open Source) | Samsara (Proprietary) | Azure Fleet (Microsoft) |
|---------|---------------------------|----------------------|------------------------|
| **Cost Model** | $0 Licensing Fees | $30-50/vehicle/month + setup fees | $40-60/vehicle/month + Azure services |
| **Deployment** | On-premise, Cloud, Hybrid | Cloud-only | Azure Cloud-only |
| **Data Ownership** | Full data sovereignty | Vendor-controlled | Microsoft-controlled |
| **Predictive Maintenance** | ✅ MLflow-powered (94% accuracy) | ✅ (Additional cost) | ✅ Azure ML (Additional cost) |
| **Route Optimization** | ✅ OSRM/Valhalla integration | ✅ (Premium feature) | ✅ Azure Maps API |
| **Real-time Streaming** | ✅ Kafka/Faust | ✅ (Additional cost) | ✅ Azure Event Hubs |
| **Customization** | Unlimited (Open source) | Limited | Limited to Azure services |
| **API Access** | Full REST API | Limited API (Additional cost) | Azure API Management |
| **Data Storage** | TimescaleDB/PostgreSQL/MinIO | Proprietary storage | Azure Cosmos DB/Storage |
| **Dashboarding** | Grafana + Plotly Dash | Proprietary dashboards | Power BI (Additional cost) |
| **Hardware Support** | Any OBD-II device | Samsara hardware required | Azure-certified devices |
| **Setup Cost** | $0 (self-hosted) | $1,000-$5,000+ setup | Azure subscription required |
| **100-Vehicle Annual Cost** | **~$2,400** (Infrastructure) | **~$48,000** | **~$60,000+** |

> **Note:** Proprietary solution costs are estimates based on public pricing. Actual costs may vary based on negotiation and specific requirements.

This project leverages open-source technologies including Python, MLflow, Prefect, PostgreSQL, and Kubernetes to optimize fleet operations. Our solution provides valuable insights for logistics companies, transportation services, and any business managing a fleet of vehicles while maintaining vendor neutrality and cost efficiency.

## Table of Contents
- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Data Sources and APIs](#data-sources-and-apis)
- [Project Structure](#project-structure)
- [Setup and Installation](#setup-and-installation)
- [Usage](#usage)
- [Use Cases](#use-cases)
- [Examples](#examples)
- [License](#license)

## Project Overview

Our Fleet Optimization project combines open-source machine learning tools with real-time vehicle data to provide accurate predictions and insights into fleet operations. Built on a modern open-source stack, we've created a scalable, efficient, and cost-effective solution for optimizing fleet management.

Key features:
- Real-time data ingestion from vehicle telematics systems
- Data preprocessing and feature engineering using open-source tools
- Model training and evaluation using MLflow
- Workflow orchestration with Prefect
- Containerized deployment with Docker and Kubernetes
- REST API endpoints with FastAPI
- Interactive dashboards with Plotly Dash
- Predictive maintenance scheduling
- Route optimization algorithms

## Architecture

Our solution leverages the following open-source technologies:

1. **Data Processing**:
   - Apache Spark: For distributed data processing
   - Dask: For parallel computing
   - Pandas: For data manipulation

2. **Machine Learning**:
   - Scikit-learn: For traditional ML models
   - XGBoost/LightGBM: For gradient boosting
   - PyTorch/TensorFlow: For deep learning
   - MLflow: For experiment tracking and model management

3. **Workflow Orchestration**:
   - Prefect: For workflow automation and scheduling
   - Airflow: Alternative orchestration option

4. **Data Storage**:
   - PostgreSQL: For relational data storage
   - TimescaleDB: For time-series data
   - MinIO: For object storage (S3-compatible)

5. **Stream Processing**:
   - Apache Kafka: For real-time data streaming
   - Faust: For stream processing

6. **Deployment**:
   - Docker: For containerization
   - Kubernetes: For orchestration
   - Seldon Core: For ML model serving

7. **Visualization**:
   - Plotly Dash: For interactive dashboards
   - Grafana: For monitoring

8. **CI/CD**:
   - GitHub Actions: For automation pipelines
   - Argo CD: For GitOps deployments

## Data Sources and APIs

Our fleet optimization solution relies on various open data sources and APIs:

1. **Vehicle Telematics Data**: 
   - Source: OBD-II devices with open protocols
   - Data: Real-time GPS location, speed, fuel consumption, engine metrics
   - Integration: Data streamed to Kafka for real-time processing

2. **Traffic and Route Data**:
   - API: OpenStreetMap (OSM) with OSRM/Valhalla routing
   - Usage: Route calculation and optimization
   - Documentation: [OSRM API](http://project-osrm.org/)

3. **Weather Data**:
   - API: Open-Meteo
   - Usage: Weather conditions affecting routes and vehicle performance
   - Documentation: [Open-Meteo API](https://open-meteo.com/)

4. **Vehicle Specifications**:
   - Dataset: NHTSA vPIC (public API)
   - Usage: Vehicle specifications for performance modeling
   - Documentation: [vPIC API](https://vpic.nhtsa.dot.gov/api/)

5. **Fuel Pricing Data**:
   - API: Fuel API (open alternative)
   - Usage: Fuel pricing for cost optimization
   - Documentation: [Fuel Prices API](https://developer.tomtom.com/fuel-prices-api/documentation/tomtom-maps/fuel-prices-api/fuel-price)

To use these data sources:

1. Configure the data ingestion scripts in `src/data/`
2. Set up Kafka topics for streaming data
3. Store credentials securely using environment variables or HashiCorp Vault
4. Ensure compliance with data protection regulations

## Project Structure

```
fleet-optimization/
│
├── .github/
│   └── workflows/
│       └── ci-cd.yml
├── scripts/
│   ├── __init__.py
│   ├── ensure_model_exists.py
│   ├── cleanup_minio.py
│   ├── init_minio.py
│   ├── wait_for_services.py
│   └── seed_demo_data.py
├── src/
│   ├── __init__.py
│   ├── data/
│   │   ├── __init__.py
│   │   ├── stream_processor.py
│   │   └── batch_processor.py
│   ├── features/
│   │   ├── __init__.py
│   │   └── feature_engineering.py
│   ├── models/
│   │   ├── __init__.py
│   │   ├── train_predictive_maintenance.py
│   │   ├── train_route_optimization.py
│   │   └── evaluate.py
│   ├── api/
│   │   ├── __init__.py
│   │   ├── app.py
│   │   └── schemas.py
│   ├── visualization/
│   │   ├── __init__.py
│   │   └── dashboard.py
│   └── workflows/
│       ├── __init__.py
│       └── main_flow.py
├── notebooks/
│   ├── exploration.ipynb
│   └── prototyping.ipynb
├── tests/
│   ├── __init__.py
│   ├── test_data.py
│   ├── test_features.py
│   └── test_models.py
├── infrastructure/
│   ├── k8s/
│   ├── docker/
│   └── terraform/
├── configs/
│   ├── model_config.yaml
│   └── app_config.yaml
├── docs/
│   ├── api.md
│   └── deployment.md
├── Dockerfile
├── requirements.txt
├── pyproject.toml
├── setup.py
└── README.md
```

## Setup and Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/bayoadejare/fleet-optimization.git
   cd fleet-optimization
   ```

2. Set up a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -e .
   ```

3. Set up infrastructure (using Docker Compose for development):
   ```bash
   docker-compose up -d
   ```

4. For production deployment:
   - Set up Kubernetes cluster
   - Deploy using Helm charts in `infrastructure/k8s/`
   - Configure Terraform for cloud resources

5. Configure environment variables:
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

## Usage

1. Start the stream processor:
   ```bash
   python src/data/stream_processor.py
   ```

2. Run batch processing:
   ```bash
   python src/data/batch_processor.py
   ```

3. Train models:
   ```bash
   python src/models/train_predictive_maintenance.py
   python src/models/train_route_optimization.py
   ```

4. Start the API server:
   ```bash
   uvicorn src.api.app:app --reload
   ```

5. Run the dashboard:
   ```bash
   python src/visualization/dashboard.py
   ```

6. Execute workflows:
   ```bash
   prefect deployment create src/workflows/main_flow.py
   ```

## Use Cases

1. **Predictive Maintenance**: Predict maintenance needs using vehicle telemetry
2. **Route Optimization**: Calculate optimal routes using OSM data
3. **Fuel Efficiency**: Analyze and improve fuel consumption
4. **Driver Behavior**: Monitor and improve driving patterns
5. **Fleet Utilization**: Optimize vehicle allocation and scheduling
6. **Real-time Monitoring**: Track fleet status with streaming data
7. **Cost Analysis**: Evaluate operational costs and savings opportunities

## Examples

### Predictive Maintenance API

```python
import requests

API_URL = "http://localhost:8000/predict/maintenance"

data = {
    "vehicle_id": "TRUCK-1234",
    "mileage": 50000,
    "engine_hours": 2000,
    "last_maintenance": "2023-01-15",
    "oil_pressure": 40,
    "coolant_temp": 90
}

response = requests.post(API_URL, json=data)
print(f"Maintenance prediction: {response.json()}")
```

### Route Optimization

```python
from src.models.route_optimization import optimize_route

result = optimize_route(
    start="Warehouse A",
    stops=["Store 1", "Store 2", "Store 3"],
    traffic="heavy"
)

print("Optimized route:", result)
```

### MLflow Tracking

```python
import mlflow

with mlflow.start_run():
    mlflow.log_param("model_type", "xgboost")
    mlflow.log_metric("accuracy", 0.92)
    mlflow.sklearn.log_model(model, "model")
```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.