import setuptools
from setuptools import find_packages

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setuptools.setup(
    name="fleet-optimization",
    version="0.1.0",
    author="Bayo Adejare",
    author_email="bayo.adejare@outlook.com", 
    description="Open-Source Fleet Optimization Solution",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/bayo-adejare/fleet-optimization",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        # Core Data Engineering
        "pandas>=2.2.2",
        "numpy>=1.26.4",
        # "apache-airflow>=2.9.1",  # Alternative orchestration
        "prefect>=3.4.14",        # Alternative orchestration
        "kafka-python>=2.0.2",    # Stream processing
        "minio>=7.2.4",           # S3-compatible storage
        "psycopg2-binary>=2.9.9", # PostgreSQL adapter
        "timescaledb>=0.0.4",     # Time-series extension
        "sqlalchemy==2.0.29",     # Explicitly set to compatible version
        "jsonschema==4.18.0",     # Explicitly pinned to compatible version

        # Machine Learning
        "scikit-learn>=1.4.2",
        "xgboost>=2.0.3",
        "lightgbm>=4.3.0",
        "catboost>=1.2.7",
        "mlflow>=2.13.0",         # Model tracking
        # "seldon-core>=1.18.2",    # Model serving
        
        # Deep Learning
        # "torch>=2.2.2",
        # "tensorflow>=2.16.1",
        
        # APIs & Web Services
        "fastapi>=0.111.0",
        "uvicorn>=0.29.0",         # ASGI server
        "requests>=2.31.0",
        "boto3>=1.28.0",
        
        # Geospatial & Routing
        "geopy>=2.4.1",
        "osmnx>=1.9.1",            # OpenStreetMap routing
        "valhalla>=0.1.4",         # Alternative routing engine
        "geopandas>=0.14.4",
        
        # Visualization
        "plotly>=5.22.0",
        "dash>=2.16.1",            # Dashboard framework
        "grafana-client>=4.3.1",   # For Grafana integration
        
        # Workflow & Infrastructure
        "docker>=7.0.0",
        "kubernetes>=29.0.0",
        "dask==2024.5.1", # Parallel processing
        "faust-streaming[rocksdb]>=0.11.3", # Streaming application

        
        # Utilities
        "python-dotenv>=1.0.1",    # Environment management
        "pyyaml>=6.0.1",           # Configuration files
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "flake8>=6.0.0",
            "black>=24.0.0",
            "mypy>=1.0.0"
        ],
        "docs": [
            "mkdocs>=1.5.0",
            "mkdocstrings>=0.24.0"
        ]
    },
    entry_points={
        "console_scripts": [
            "fleet-optimization=main:main",
            "fleet-stream-process=src.data.stream_processor:main",
            "fleet-batch-process=src.data.batch_processor:main",
            "fleet-train-process=src.models.train_predictive_maintenance:main",
            # "fleet-ingest = src.data.stream_processor:main",
            # "fleet-train = src.models.train_predictive_maintenance:main",
            # "fleet-api = src.api.app:main"
        ]
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Intended Audience :: Transportation",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: 3.14",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Internet :: WWW/HTTP :: HTTP Servers",
        "Operating System :: OS Independent"
    ],
    python_requires=">=3.11",
    include_package_data=True,
    package_data={
        "config": ["*.yaml"],
        "docs": ["*.md"]
    }
)
