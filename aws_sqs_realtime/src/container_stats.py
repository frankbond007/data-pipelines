import psutil
from prometheus_client import start_http_server, Gauge
import time

# Create Prometheus Gauges for CPU and Memory Usage
CPU_USAGE = Gauge('cpu_usage_percentage', 'CPU Usage Percentage')
MEMORY_USAGE = Gauge('memory_usage_bytes', 'Memory Usage in Bytes')

def collect_metrics():
    while True:
        # Get CPU usage as a percentage
        cpu_percent = psutil.cpu_percent(interval=1)

        # Get memory usage in bytes
        mem = psutil.virtual_memory()
        memory_bytes = mem.used

        # Set the metrics with the latest values
        CPU_USAGE.set(cpu_percent)
        MEMORY_USAGE.set(memory_bytes)

        time.sleep(1)

if __name__ == '__main__':
    # Start the Prometheus HTTP server on port 9323
    start_http_server(9323)

    # Start collecting metrics
    collect_metrics()
