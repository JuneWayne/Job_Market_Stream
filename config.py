# config.py
import os

# Read from env, default to redpanda:9092 (the Docker service name)
KAFKA_SERVER = [os.getenv("KAFKA_SERVER", "redpanda:9092")]
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "job_postings")
