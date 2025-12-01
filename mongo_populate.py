# mongo_populate.py
"""
Pull historical job docs from MongoDB Atlas, run them through parse_job_postings,
and append them into parsed_jobs.csv using storage.append_parsed_job().
"""

import os
import logging
from typing import Dict, Any, Optional

from pymongo import MongoClient
from dotenv import load_dotenv

from job_parser import parse_job_postings
from save_csv import append_parsed_job, OUTPUT_FILE

# ---------------------------------------------------------------------
# MongoDB config (Atlas) – matches your snippet
# ---------------------------------------------------------------------
load_dotenv()  # load .env into environment

MONGO_PWD = os.getenv("MONGO_PWD")
if not MONGO_PWD:
    raise RuntimeError("MONGO_PWD not set in environment or .env")

# You can override this with MONGO_URI if you want, but by default it
# uses the same pattern you showed:
MONGO_URI = os.getenv(
    "MONGO_URI",
    f"mongodb+srv://JuneWay:{MONGO_PWD}@ethanc.qgevd.mongodb.net/JobDB",
)

MONGO_DB = "JobDB"  # fixed from your snippet

# Default collection if not specified when calling populate_from_mongo()
DEFAULT_COLLECTION = os.getenv("MONGO_COLLECTION", "jobs_history")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("mongo_populate")


# ---------------------------------------------------------------------
# Helpers to map Mongo docs → raw job dict expected by parse_job_postings
# ---------------------------------------------------------------------
def build_raw_from_mongo(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map a Mongo document into the "raw" job dict format that
    parse_job_postings() expects (similar to your LinkedIn scraper output).

    We:
    - build a synthetic job_description from summary + degree_qualifications + skills_desired
    - keep time_posted as the original string (e.g. "4 weeks ago")
    - keep num_applicants as the raw text (parse_job_postings will convert it)
    """

    # Safely join summary + degree_qualifications + skills_desired into one description
    description_parts = [
        doc.get("summary", ""),
        doc.get("degree_qualifications", ""),
        doc.get("skills_desired", ""),
    ]
    description = " ".join(
        part.strip() for part in description_parts if isinstance(part, str) and part.strip()
    )

    raw: Dict[str, Any] = {
        # Use Mongo _id as job_id so each row is unique
        "job_id": str(doc.get("_id")),
        "job_title": doc.get("job_title"),
        "company_name": doc.get("company_name"),
        "location": doc.get("location"),

        # This should be a string like "4 weeks ago" in your historical data
        "time_posted": doc.get("time_posted", "") or "",

        # parse_job_postings expects this string; it will parse the int
        "num_applicants": doc.get("num_applicants"),

        # Combined text for downstream parsing (skills, degree, job_function, work_mode, etc.)
        "job_description": description,
    }

    return raw


def process_one_doc(doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Build raw dict from Mongo doc, run it through parse_job_postings,
    and make sure required fields exist for CSV.
    """
    try:
        raw = build_raw_from_mongo(doc)
        parsed = parse_job_postings(raw)

        # Ensure required fields exist (storage.append_parsed_job will select them)
        parsed.setdefault("job_description", raw.get("job_description", ""))
        parsed.setdefault("application_link", None)      # historical docs likely don't have this
        parsed.setdefault("num_applicants_int", None)    # parse_job_postings should set it
        parsed.setdefault("work_mode", None)
        parsed.setdefault("job_function", None)
        parsed.setdefault("degree_requirement", None)
        parsed.setdefault("skills", "")

        return parsed

    except Exception as e:
        logger.exception("Failed to process Mongo doc with _id=%s: %s", doc.get("_id"), e)
        return None


# ---------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------
def populate_from_mongo(collection_name: Optional[str] = None,
                        limit: Optional[int] = None) -> None:
    """
    Stream documents from MongoDB, parse, and append to parsed_jobs.csv.

    :param collection_name: "jobs_history" (default) or "jobs" or any other.
    :param limit: Optional max number of docs to process (for testing).
    """
    if collection_name is None:
        collection_name = DEFAULT_COLLECTION

    if OUTPUT_FILE.exists():
        logger.warning(
            "parsed_jobs.csv already exists at %s. "
            "New rows will be APPENDED. Delete the file first if you want a fresh run.",
            OUTPUT_FILE,
        )

    logger.info("Connecting to MongoDB Atlas: %s", MONGO_URI)
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[collection_name]

    query: Dict[str, Any] = {}  # adjust if you want to filter

    logger.info(
        "Starting to stream docs from %s.%s (limit=%s)",
        MONGO_DB,
        collection_name,
        limit,
    )

    count_processed = 0
    count_written = 0

    cursor = collection.find(query, batch_size=1000)

    try:
        for doc in cursor:
            count_processed += 1
            if limit is not None and count_processed > limit:
                break

            parsed = process_one_doc(doc)
            if parsed is None:
                continue

            append_parsed_job(parsed)
            count_written += 1

            if count_written % 1000 == 0:
                logger.info("Processed %d docs, written %d rows so far...",
                            count_processed, count_written)

    finally:
        cursor.close()
        client.close()

    logger.info("Finished. Processed %d docs, wrote %d rows to %s",
                count_processed, count_written, OUTPUT_FILE)


if __name__ == "__main__":
    # Example usages:
    #   populate_from_mongo("jobs_history", limit=5000)
    #   populate_from_mongo("jobs")  # to pull the daily collection
    populate_from_mongo(collection_name="jobs_history", limit=None)
