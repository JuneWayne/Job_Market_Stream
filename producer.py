import json
import logging

import pandas as pd
from kafka import KafkaProducer

from config import KAFKA_SERVER, KAFKA_TOPIC
from scraper import scrape_linkedin

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("producer")


def run_producer(
    keywords: str = "Data Intern",
    location: str = "United States",
    geo_id: str = "103644278",
    f_tpr: str = "r86400",
    max_results: int = 10000,
):
    # scrape results then push each row to Kafka
    logger.info(
        "Scraping LinkedIn: keywords='%s', location='%s', geo_id=%s, f_tpr=%s, max_results=%d",
        keywords, location, geo_id, f_tpr, max_results,
    )

    df = scrape_linkedin(
        keywords=keywords,
        location=location,
        geo_id=geo_id,
        f_tpr=f_tpr,
        max_results=max_results,
    )

    if df.empty:
        logger.warning("No jobs found, nothing to send.")
        return

    # Replace NaN with None for JSON serialization
    df = df.where(pd.notnull(df), None)

    # Create Kafka producer that serializes message values as JSON bytes
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    sent = 0
    try:
        for job_dict in df.to_dict(orient="records"):
            # Send each job dict as the message value to the configured topic
            producer.send(KAFKA_TOPIC, value=job_dict)
            sent += 1

        # Ensure all messages are delivered before closing
        producer.flush()
        logger.info("Sent %d messages to topic '%s'", sent, KAFKA_TOPIC)
    finally:
        producer.close()
        logger.info("Kafka producer closed.")


def hourly_scraping(interval_hours: float = 0.5):
    # Run the producer on a regular interval (default: every 0.5 hours)
    import time

    while True:
        run_producer(
            keywords="Data Intern",
            location="United States",
            geo_id="103644278",
            f_tpr="r86400",
            max_results=10000,
        )
        logger.info("Sleeping for %d hours before next scrape.", interval_hours)
        time.sleep(interval_hours * 3600)


if __name__ == "__main__":
    # If run directly, start the periodic scraper loop
    hourly_scraping(interval_hours=0.5)
