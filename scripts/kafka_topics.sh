#!/bin/bash

# kafka_topics.sh - Fixed health check script
set -euo pipefail

# Configuration
CONTAINER_NAME="fleet-optimization-main-kafka-1"
BOOTSTRAP_SERVER="localhost:9092"  # Use localhost within the container
KAFKA_SCRIPT_PATH="/usr/bin/kafka-topics.sh"
MAX_ATTEMPTS=30
SLEEP_DURATION=5

# Health check function
check_kafka_health() {
    # Try multiple approaches to check if Kafka is ready
    
    # 1. Try with bootstrap server (preferred)
    if docker exec "$CONTAINER_NAME" \
        "$KAFKA_SCRIPT_PATH" \
        --bootstrap-server "$BOOTSTRAP_SERVER" \
        --list > /dev/null 2>&1; then
        echo "OK: Bootstrap server connection successful"
        return 0
    fi
    
    # 2. Try with Zookeeper (fallback)
    if docker exec "$CONTAINER_NAME" \
        "$KAFKA_SCRIPT_PATH" \
        --zookeeper zookeeper:2181 \
        --list > /dev/null 2>&1; then
        echo "OK: Zookeeper connection successful"
        return 0
    fi
    
    # 3. Check if Kafka process is running
    if docker exec "$CONTAINER_NAME" ps aux | grep -q "[k]afka.Kafka"; then
        echo "OK: Kafka process is running"
        return 0
    fi
    
    return 1
}

# Main health check
if check_kafka_health; then
    echo "OK"
    exit 0
else
    echo "NOT READY"
    exit 1
fi