#!/usr/bin/env python3
import time
import socket
import sys
from contextlib import closing

def check_port(host, port):
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.settimeout(2)
        return sock.connect_ex((host, port)) == 0

def wait_for_service(host, port, timeout=60):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if check_port(host, port):
            print(f"{host}:{port} is available")
            return True
        print(f"Waiting for {host}:{port}...")
        time.sleep(2)
    print(f"Timeout waiting for {host}:{port}")
    return False

if __name__ == "__main__":
    services = [
        ("kafka", 9092),
        ("postgres", 5432),
        ("minio", 9000),
        ("mlflow", 5000)
    ]
    
    for host, port in services:
        if not wait_for_service(host, port):
            sys.exit(1)
    
    print("All services are available!")