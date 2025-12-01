# producer.py

import json
import logging

import pandas as pd
from kafka import KafkaProducer

from config import KAFKA_SERVER, KAFKA_TOPIC
from scraper import scrape_linkedin  # your LinkedIn scraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("producer")


def run_producer(
    keywords = "Data Intern",
    location = "United States",
    geo_id = "103644278",
    f_tpr = "r86400",
    max_results = 10000,
):
    logger.info(
        "Scraping LinkedIn: keywords='%s', location='%s', geo_id=%s, f_tpr=%s, max_results=%d",
        keywords, location, geo_id, f_tpr, max_results
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

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        # We only care about serializing the VALUE as JSON
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    sent = 0
    try:
        for job_dict in df.to_dict(orient="records"):
            # IMPORTANT: specify value=, do NOT pass job_dict as the first positional arg
            producer.send(KAFKA_TOPIC, value=job_dict)
            sent += 1

        producer.flush()
        logger.info("Sent %d messages to topic '%s'", sent, KAFKA_TOPIC)
    finally:
        producer.close()
        logger.info("Kafka producer closed.")

def hourly_scraping(interval_hours=0.5):
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
    hourly_scraping(interval_hours=0.5)
