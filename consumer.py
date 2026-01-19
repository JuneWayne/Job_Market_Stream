# consumer.py

import json
import logging
import os
import time
import signal
import sys
from datetime import datetime, timedelta

from kafka import KafkaConsumer
from kafka.errors import (
    NoBrokersAvailable, 
    KafkaError, 
    CommitFailedError,
    KafkaTimeoutError,
    KafkaConnectionError,
    ConsumerStoppedError,
    OffsetOutOfRangeError
)

from config import KAFKA_SERVER, KAFKA_TOPIC
from job_parser import parse_job_postings  
from save_csv import append_parsed_job

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("consumer")

# Self-healing configuration
MAX_CONSECUTIVE_ERRORS = 5
HEALTH_CHECK_INTERVAL = 300  # 5 minutes
MAX_PROCESSING_TIME = 30  # Max seconds per message
COMMIT_RETRY_ATTEMPTS = 3
RECONNECTION_DELAY = 10

class HealthyConsumer:
    def __init__(self):
        self.consumer = None
        self.last_successful_message = datetime.now()
        self.consecutive_errors = 0
        self.processed_messages = 0
        self.should_stop = False
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.should_stop = True


    def get_consumer_with_retry(self, max_retries=10, base_delay=5):
        """
        Create and return a KafkaConsumer with retry logic and optimized settings.
        """
        for attempt in range(max_retries):
            try:
                consumer = KafkaConsumer(
                    KAFKA_TOPIC,
                    bootstrap_servers=KAFKA_SERVER,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    group_id="job-parsers",
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,  # Manual commit for better error handling
                    session_timeout_ms=30000,  # 30 seconds
                    heartbeat_interval_ms=10000,  # 10 seconds
                    max_poll_interval_ms=300000,  # 5 minutes
                    request_timeout_ms=60000,  # 1 minute
                    connections_max_idle_ms=540000,  # 9 minutes
                    fetch_min_bytes=1,
                    fetch_max_wait_ms=500,
                    retry_backoff_ms=100,
                    consumer_timeout_ms=1000,  # 1 second poll timeout
                )
                logger.info("Successfully connected to Kafka broker")
                return consumer
            except NoBrokersAvailable:
                delay = base_delay * (2 ** min(attempt, 4))
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

    def safe_commit(self, consumer):
        """
        Safely commit offsets with retry logic
        """
        for attempt in range(COMMIT_RETRY_ATTEMPTS):
            try:
                consumer.commit()
                return True
            except CommitFailedError as e:
                logger.warning(f"Commit failed (attempt {attempt + 1}/{COMMIT_RETRY_ATTEMPTS}): {e}")
                if attempt < COMMIT_RETRY_ATTEMPTS - 1:
                    time.sleep(1 * (attempt + 1))  # Exponential backoff
                else:
                    logger.error("Failed to commit after all retries, consumer may be kicked out of group")
                    return False
            except KafkaError as e:
                logger.warning(f"Kafka error during commit (attempt {attempt + 1}): {e}")
                if attempt < COMMIT_RETRY_ATTEMPTS - 1:
                    time.sleep(1 * (attempt + 1))
                else:
                    return False
        return False

    def is_consumer_healthy(self):
        """
        Check if consumer is healthy based on various metrics
        """
        time_since_last_message = datetime.now() - self.last_successful_message
        
        # If no message processed in HEALTH_CHECK_INTERVAL, might be unhealthy
        if time_since_last_message > timedelta(seconds=HEALTH_CHECK_INTERVAL):
            logger.warning(f"No messages processed for {time_since_last_message.total_seconds():.0f} seconds")
            return False
            
        # Too many consecutive errors
        if self.consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
            logger.error(f"Too many consecutive errors ({self.consecutive_errors}), consumer unhealthy")
            return False
            
        return True

    def reconnect_consumer(self):
        """
        Reconnect the consumer with proper cleanup
        """
        logger.info("Attempting to reconnect consumer...")
        
        if self.consumer:
            try:
                self.consumer.close()
            except Exception as e:
                logger.warning(f"Error closing old consumer: {e}")
        
        time.sleep(RECONNECTION_DELAY)
        
        try:
            self.consumer = self.get_consumer_with_retry()
            self.consecutive_errors = 0
            logger.info("Successfully reconnected consumer")
            return True
        except Exception as e:
            logger.error(f"Failed to reconnect consumer: {e}")
            return False


    def run_consumer(self):
        """
        Main consumer loop with comprehensive error handling and self-healing
        """
        self.consumer = self.get_consumer_with_retry()
        logger.info("Consumer started, listening to topic '%s'", KAFKA_TOPIC)
        
        # Enable geocoding via environment variable
        enable_geocoding = os.getenv("ENABLE_GEOCODING", "false").lower() == "true"
        if enable_geocoding:
            logger.info("Geocoding is ENABLED - locations will be geocoded (slower processing)")
        else:
            logger.info("Geocoding is DISABLED - set ENABLE_GEOCODING=true to enable")

        messages_since_commit = 0
        commit_interval = 10  # Commit every 10 messages
        
        while not self.should_stop:
            try:
                # Poll for messages with timeout
                message_batch = self.consumer.poll(timeout_ms=1000)
                
                if not message_batch:
                    # No messages, do health check
                    if not self.is_consumer_healthy():
                        logger.warning("Consumer appears unhealthy, attempting reconnection...")
                        if not self.reconnect_consumer():
                            logger.error("Failed to reconnect, waiting before retry...")
                            time.sleep(30)
                    continue

                # Process messages
                for topic_partition, messages in message_batch.items():
                    for msg in messages:
                        if self.should_stop:
                            break
                            
                        try:
                            # Process individual message with timeout
                            start_time = time.time()
                            
                            raw_job = msg.value
                            parsed = parse_job_postings(raw_job, geocode=enable_geocoding)
                            
                            # Check processing time
                            processing_time = time.time() - start_time
                            if processing_time > MAX_PROCESSING_TIME:
                                logger.warning(f"Message processing took {processing_time:.2f}s (max: {MAX_PROCESSING_TIME}s)")

                            logger.info(
                                "Parsed job: title='%s', company='%s', location='%s', function='%s', degree='%s'",
                                parsed.get("job_title"),
                                parsed.get("company_name"),
                                parsed.get("location"),
                                parsed.get("job_function"),
                                parsed.get("degree_requirement"),
                            )

                            append_parsed_job(parsed)
                            
                            # Update health metrics
                            self.last_successful_message = datetime.now()
                            self.processed_messages += 1
                            self.consecutive_errors = 0
                            messages_since_commit += 1
                            
                        except Exception as e:
                            self.consecutive_errors += 1
                            logger.exception(f"Failed to parse job (error #{self.consecutive_errors}): %s", e)
                            
                            # If too many consecutive errors, try reconnecting
                            if self.consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                                logger.error("Too many consecutive parsing errors, attempting reconnection...")
                                if not self.reconnect_consumer():
                                    time.sleep(30)
                                break
                            continue

                # Commit offsets periodically
                if messages_since_commit >= commit_interval:
                    if self.safe_commit(self.consumer):
                        logger.debug(f"Successfully committed offsets after {messages_since_commit} messages")
                        messages_since_commit = 0
                    else:
                        logger.warning("Failed to commit offsets, attempting reconnection...")
                        if not self.reconnect_consumer():
                            time.sleep(30)
                            
            except ConsumerStoppedError:
                logger.warning("Consumer stopped error, attempting reconnection...")
                if not self.reconnect_consumer():
                    time.sleep(30)
                    
            except (KafkaConnectionError, KafkaTimeoutError) as e:
                self.consecutive_errors += 1
                logger.error(f"Kafka connection/timeout error: {e}")
                if not self.reconnect_consumer():
                    time.sleep(30)
                    
            except OffsetOutOfRangeError as e:
                logger.error(f"Offset out of range: {e}. Resetting to earliest offset.")
                try:
                    self.consumer.seek_to_beginning()
                except Exception as seek_error:
                    logger.error(f"Failed to seek to beginning: {seek_error}")
                    if not self.reconnect_consumer():
                        time.sleep(30)
                        
            except Exception as e:
                self.consecutive_errors += 1
                logger.exception(f"Unexpected error in consumer loop: {e}")
                if self.consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    if not self.reconnect_consumer():
                        time.sleep(30)
                else:
                    time.sleep(5)  # Short delay before retrying

        # Final cleanup
        logger.info("Shutting down consumer...")
        if self.consumer:
            try:
                # Final commit
                self.safe_commit(self.consumer)
                self.consumer.close()
            except Exception as e:
                logger.warning(f"Error during final cleanup: {e}")
        
        logger.info(f"Consumer stopped. Processed {self.processed_messages} messages total.")


def run_consumer():
    """
    Legacy function for backward compatibility
    """
    healthy_consumer = HealthyConsumer()
    healthy_consumer.run_consumer()


if __name__ == "__main__":
    healthy_consumer = HealthyConsumer()
    healthy_consumer.run_consumer()
