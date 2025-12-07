import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional, Tuple

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DATA_CSV = Path("data/parsed_jobs.csv")
TABLE_NAME = '"job-market-stream"'
EXPECTED_COLUMNS = [
    "job_id",
    "job_title",
    "job_description",
    "company_name",
    "industry",
    "location",
    "latitude",
    "longitude",
    "job_function",
    "skills",
    "degree_requirement",
    "visa_sponsorship",
    "time_posted_parsed",
    "application_link",
    "num_applicants_int",
    "work_mode",
    "scraped_at",
]

logger = logging.getLogger("supabase_ingestion")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)


def _get_env(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Environment variable {name} is required")
    return value


def build_connection() -> psycopg2.extensions.connection:
    supabase_pwd = _get_env("SUPABASE_PWD")
    supabase_host = os.getenv("SUPABASE_HOST", "aws-1-us-east-1.pooler.supabase.com")
    supabase_port = os.getenv("SUPABASE_PORT", "5432")
    user = os.getenv("SUPABASE_USER", "postgres.wtiopnzppsyjxecrogik")

    db_url = (
        f"postgresql://{user}:{supabase_pwd}@{supabase_host}:{supabase_port}/postgres"
    )
    return psycopg2.connect(db_url)


def ensure_table(conn: psycopg2.extensions.connection) -> None:
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        job_id TEXT PRIMARY KEY,
        job_title TEXT,
        job_description TEXT,
        company_name TEXT,
        industry TEXT,
        location TEXT,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        job_function TEXT,
        skills TEXT,
        degree_requirement TEXT,
        visa_sponsorship TEXT,
        time_posted_parsed TIMESTAMP WITH TIME ZONE,
        application_link TEXT,
        num_applicants_int INTEGER,
        work_mode TEXT,
        scraped_at TIMESTAMP WITH TIME ZONE,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    """
    with conn.cursor() as cursor:
        cursor.execute(create_table_sql)
        
        # Add latitude and longitude columns if they don't exist
        cursor.execute(f"""
            DO $$ 
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'job-market-stream' AND column_name = 'latitude'
                ) THEN
                    ALTER TABLE {TABLE_NAME} ADD COLUMN latitude DOUBLE PRECISION;
                END IF;
                
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'job-market-stream' AND column_name = 'longitude'
                ) THEN
                    ALTER TABLE {TABLE_NAME} ADD COLUMN longitude DOUBLE PRECISION;
                END IF;

                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'job-market-stream' AND column_name = 'industry'
                ) THEN
                    ALTER TABLE {TABLE_NAME} ADD COLUMN industry TEXT;
                END IF;

                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'job-market-stream' AND column_name = 'visa_sponsorship'
                ) THEN
                    ALTER TABLE {TABLE_NAME} ADD COLUMN visa_sponsorship TEXT;
                END IF;
            END $$;
        """)
    conn.commit()


def _clean_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _clean_int(value: Any) -> Optional[int]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _clean_float(value: Any) -> Optional[float]:
    """Clean and validate float values (for latitude/longitude)."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clean_timestamp(value: Any) -> Optional[datetime]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, datetime):
        return value
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime()


def load_records() -> List[Tuple[Any, ...]]:
    if not DATA_CSV.exists():
        logger.warning("%s not found, skipping ingestion", DATA_CSV)
        return []

    def _skip_bad_line(bad_line: list[str]) -> None:
        # Log and drop lines with unexpected column counts so ingestion keeps running.
        logger.warning(
            "Skipping malformed row with %d fields (expected %d): %s",
            len(bad_line),
            len(EXPECTED_COLUMNS),
            bad_line[:3],
        )

    try:
        df = pd.read_csv(
            DATA_CSV,
            engine="python",  # more permissive parsing for occasional bad rows
            on_bad_lines=_skip_bad_line,
            usecols=lambda c: c in EXPECTED_COLUMNS,
        )
    except Exception:
        logger.exception("Failed to read %s", DATA_CSV)
        return []

    # Normalize numeric columns and drop rows that look misaligned (e.g., job_function filled with longitude).
    df["latitude"] = pd.to_numeric(df.get("latitude"), errors="coerce")
    df["longitude"] = pd.to_numeric(df.get("longitude"), errors="coerce")
    df["num_applicants_int"] = pd.to_numeric(df.get("num_applicants_int"), errors="coerce")

    numeric_job_func = pd.to_numeric(df.get("job_function"), errors="coerce")
    bad_jobfunc = numeric_job_func.notna()
    if bad_jobfunc.any():
        logger.warning("Dropping %d rows with numeric job_function (misaligned data)", int(bad_jobfunc.sum()))
        df = df.loc[~bad_jobfunc]

    df = df.dropna(subset=["latitude", "longitude"])

    if df.empty:
        logger.info("No rows found in %s", DATA_CSV)
        return []

    records_map: dict[str, Tuple[Any, ...]] = {}
    for _, row in df.iterrows():
        job_id = _clean_str(row.get("job_id"))
        if not job_id:
            continue

        record = (
            job_id,
            _clean_str(row.get("job_title")),
            _clean_str(row.get("job_description")),
            _clean_str(row.get("company_name")),
            _clean_str(row.get("industry")),
            _clean_str(row.get("location")),
            _clean_float(row.get("latitude")),
            _clean_float(row.get("longitude")),
            _clean_str(row.get("job_function")),
            _clean_str(row.get("skills")),
            _clean_str(row.get("degree_requirement")),
            _clean_str(row.get("visa_sponsorship")),
            _clean_timestamp(row.get("time_posted_parsed")),
            _clean_str(row.get("application_link")),
            _clean_int(row.get("num_applicants_int")),
            _clean_str(row.get("work_mode")),
            _clean_timestamp(row.get("scraped_at")),
        )
        records_map[job_id] = record

    records = list(records_map.values())
    logger.info(
        "Prepared %d records (deduped from %d rows) from %s",
        len(records),
        len(df),
        DATA_CSV,
    )
    return records


def upsert_records(
    conn: psycopg2.extensions.connection, records: List[Tuple[Any, ...]]
) -> int:
    if not records:
        return 0

    insert_sql = f"""
    INSERT INTO {TABLE_NAME} (
        job_id, job_title, job_description, company_name, industry, location, latitude, longitude,
        job_function, skills, degree_requirement, visa_sponsorship, time_posted_parsed, application_link,
        num_applicants_int, work_mode, scraped_at
    ) VALUES %s
    ON CONFLICT (job_id) DO UPDATE SET
        job_title = COALESCE(EXCLUDED.job_title, {TABLE_NAME}.job_title),
        job_description = COALESCE(EXCLUDED.job_description, {TABLE_NAME}.job_description),
        company_name = COALESCE(EXCLUDED.company_name, {TABLE_NAME}.company_name),
        industry = COALESCE(EXCLUDED.industry, {TABLE_NAME}.industry),
        location = COALESCE(EXCLUDED.location, {TABLE_NAME}.location),
        latitude = COALESCE(EXCLUDED.latitude, {TABLE_NAME}.latitude),
        longitude = COALESCE(EXCLUDED.longitude, {TABLE_NAME}.longitude),
        job_function = COALESCE(EXCLUDED.job_function, {TABLE_NAME}.job_function),
        skills = COALESCE(EXCLUDED.skills, {TABLE_NAME}.skills),
        degree_requirement = COALESCE(EXCLUDED.degree_requirement, {TABLE_NAME}.degree_requirement),
        visa_sponsorship = COALESCE(EXCLUDED.visa_sponsorship, {TABLE_NAME}.visa_sponsorship),
        time_posted_parsed = COALESCE(EXCLUDED.time_posted_parsed, {TABLE_NAME}.time_posted_parsed),
        application_link = COALESCE(EXCLUDED.application_link, {TABLE_NAME}.application_link),
        num_applicants_int = EXCLUDED.num_applicants_int,
        work_mode = COALESCE(EXCLUDED.work_mode, {TABLE_NAME}.work_mode),
        scraped_at = EXCLUDED.scraped_at;
    """

    with conn.cursor() as cursor:
        execute_values(cursor, insert_sql, records, page_size=1000)
    conn.commit()
    return len(records)


def ingest_once() -> int:
    records = load_records()
    if not records:
        return 0

    try:
        with build_connection() as conn:
            ensure_table(conn)
            inserted = upsert_records(conn, records)
            logger.info("Upserted %d records into Supabase", inserted)
            return inserted
    except Exception:
        logger.exception("Failed to upsert records into Supabase")
        return 0


def ingest_loop(interval_seconds: int = 300) -> None:
    logger.info(
        "Starting Supabase ingestion loop (interval=%s seconds)", interval_seconds
    )
    last_mtime: Optional[float] = None
    while True:
        current_mtime: Optional[float] = None
        if DATA_CSV.exists():
            current_mtime = DATA_CSV.stat().st_mtime

        if current_mtime is None:
            ingest_once()
        elif last_mtime is None or current_mtime != last_mtime:
            inserted = ingest_once()
            if inserted:
                last_mtime = current_mtime
        else:
            logger.info("No changes detected in %s; skipping upsert", DATA_CSV)

        time.sleep(interval_seconds)


if __name__ == "__main__":
    interval_env = os.getenv("SUPABASE_INGEST_INTERVAL_SECONDS")
    try:
        interval = int(interval_env) if interval_env else 300
    except ValueError:
        logger.warning(
            "Invalid SUPABASE_INGEST_INTERVAL_SECONDS=%s, defaulting to 300", interval_env
        )
        interval = 300
    ingest_loop(interval_seconds=interval)
