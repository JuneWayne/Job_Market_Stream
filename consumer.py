# consumer.py

import json
import logging
import os
import time

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaError

from config import KAFKA_SERVER, KAFKA_TOPIC
from job_parser import parse_job_postings  
from save_csv import append_parsed_job

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("consumer")


def get_consumer_with_retry(max_retries=10, base_delay=5):
    """
    Create and return a KafkaConsumer with retry logic for broker connectivity.
    """
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_SERVER,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                group_id="job-parsers",
                auto_offset_reset="earliest", 
                enable_auto_commit=True,
            )
            logger.info("Successfully connected to Kafka broker")
            return consumer
        except NoBrokersAvailable:
            delay = base_delay * (2 ** min(attempt, 4))  # Cap exponential growth
            logger.warning(
                "No brokers available (attempt %d/%d), retrying in %ds...",
                attempt + 1, max_retries, delay
            )
            time.sleep(delay)
        except KafkaError as e:
            delay = base_delay * (2 ** min(attempt, 4))
            logger.warning(
                "Kafka error: %s (attempt %d/%d), retrying in %ds...",
                e, attempt + 1, max_retries, delay
            )
            time.sleep(delay)
    
    raise NoBrokersAvailable("Failed to connect after %d retries" % max_retries)


def run_consumer():
    consumer = get_consumer_with_retry()
    logger.info("Consumer started, listening to topic '%s'", KAFKA_TOPIC)
    
    # Enable geocoding via environment variable (default: disabled for performance)
    enable_geocoding = os.getenv("ENABLE_GEOCODING", "false").lower() == "true"
    if enable_geocoding:
        logger.info("Geocoding is ENABLED - locations will be geocoded (slower processing)")
    else:
        logger.info("Geocoding is DISABLED - set ENABLE_GEOCODING=true to enable")

    try:
        for msg in consumer:
            raw_job = msg.value  

            try:
                parsed = parse_job_postings(raw_job, geocode=enable_geocoding)
            except Exception as e:
                logger.exception("Failed to parse job: %s", e)
                continue

            logger.info(
                "Parsed job: title='%s', company='%s', location='%s', functio;n='%s', degree='%s'",
                parsed.get("job_title"),
                parsed.get("company_name"),
                parsed.get("location"),
                parsed.get("job_function"),
                parsed.get("degree_requirement"),
            )

            append_parsed_job(parsed)
    except KeyboardInterrupt:
        logger.info("Consumer interrupted, shutting down...")
    finally:
        consumer.close()
        logger.info("Consumer closed.")


if __name__ == "__main__":
    run_consumer()
