#!/usr/bin/env python3

import os
import sys
import time
import json
import logging
import subprocess
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

API_ENDPOINT = "https://job-market-stream.onrender.com/api/hourly_counts?hours=24"
CRITICAL_CONTAINERS = ["redpanda", "consumer", "producer", "supabase_ingestor"]
CHECK_INTERVAL_MINUTES = 30

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def check_api():
    try:
        response = requests.get(API_ENDPOINT, timeout=60)
        if response.status_code != 200:
            return False, f"API returned status {response.status_code}"
        data = response.json()
        if not data:
            return False, "API returned empty data"
        total_jobs = sum(item.get("job_count", 0) for item in data)
        return True, f"API healthy: {total_jobs} jobs in {len(data)} hours"
    except Exception as e:
        return False, f"API check failed: {str(e)}"


def check_docker():
    try:
        result = subprocess.run(
            ["docker", "compose", "ps", "--format", "json"],
            capture_output=True,
            text=True,
            cwd=os.path.dirname(os.path.abspath(__file__))
        )
        if result.returncode != 0:
            return False, f"Docker command failed: {result.stderr}"
        
        running = set()
        for line in result.stdout.strip().split('\n'):
            if line:
                try:
                    container = json.loads(line)
                    if container.get("State", "").lower() == "running":
                        running.add(container.get("Name", "").lower())
                except json.JSONDecodeError:
                    continue
        
        down = []
        for name in CRITICAL_CONTAINERS:
            if not any(name in r for r in running):
                down.append(name)
        
        if down:
            return False, f"Containers down: {', '.join(down)}"
        return True, "All containers running"
    except Exception as e:
        return False, f"Docker check failed: {str(e)}"


def send_alert(subject, body):
    ntfy_topic = os.getenv("NTFY_TOPIC")
    
    if not ntfy_topic:
        logger.warning("Set NTFY_TOPIC env variable (e.g. 'my-job-pipeline-alerts')")
        return False
    
    try:
        requests.post(
            f"https://ntfy.sh/{ntfy_topic}",
            data=f"{subject}\n\n{body}",
            headers={"Title": "Job Pipeline Alert"}
        )
        logger.info(f"Alert sent to ntfy.sh/{ntfy_topic}")
        return True
    except Exception as e:
        logger.error(f"Failed to send alert: {e}")
        return False
        return True
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False


def run_check():
    logger.info("Running health check")
    issues = []
    
    api_ok, api_msg = check_api()
    logger.info(f"API: {api_msg}")
    if not api_ok:
        issues.append(api_msg)
    
    docker_ok, docker_msg = check_docker()
    logger.info(f"Docker: {docker_msg}")
    if not docker_ok:
        issues.append(docker_msg)
    
    if issues:
        send_alert("Issues Detected", "\n".join(issues))
        return False
    
    logger.info("All systems healthy")
    return True


def run_daemon():
    logger.info(f"Starting monitor (interval: {CHECK_INTERVAL_MINUTES} min)")
    last_alert = None
    
    while True:
        try:
            healthy = run_check()
            if not healthy:
                if last_alert is None or (datetime.now() - last_alert) > timedelta(hours=1):
                    last_alert = datetime.now()
            time.sleep(CHECK_INTERVAL_MINUTES * 60)
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"Error: {e}")
            time.sleep(60)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--daemon":
        run_daemon()
    else:
        run_check()
