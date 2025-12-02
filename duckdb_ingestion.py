# duckdb_ingestion.py
import duckdb
from pathlib import Path
import logging
import csv
from geo_encode import update_geo_locations
DATA_CSV = Path("data/parsed_jobs.csv")
DB_PATH = Path("data/jobs.duckdb")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("duckdb_ingestion")


def csv_has_column(csv_path: Path, column_name: str) -> bool:
    """Check if a CSV file has a specific column in its header."""
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader, [])
        return column_name in header


def main():
    if not DATA_CSV.exists():
        raise FileNotFoundError(
            f"{DATA_CSV} does not exist. Run mongo_populate / consumer first."
        )

    conn = duckdb.connect(str(DB_PATH))
    
    # Check if CSV has scraped_at column
    has_scraped_at = csv_has_column(DATA_CSV, 'scraped_at')
    logger.info(f"CSV has scraped_at column: {has_scraped_at}")

    # Build scraped_at parsing clause conditionally
    if has_scraped_at:
        scraped_at_clause = """
            -- Parse scraped_at timestamp (when the job was actually collected)
            CASE
                WHEN scraped_at IS NULL OR scraped_at = '' THEN NULL
                ELSE COALESCE(
                    TRY_STRPTIME(scraped_at, '%Y-%m-%dT%H:%M:%S.%f%z'),
                    TRY_STRPTIME(scraped_at, '%Y-%m-%dT%H:%M:%S%z'),
                    TRY_STRPTIME(scraped_at, '%Y-%m-%dT%H:%M:%S'),
                    TRY_STRPTIME(scraped_at, '%Y-%m-%d %H:%M:%S')
                )
            END AS scraped_at
        """
    else:
        scraped_at_clause = """
            -- No scraped_at in CSV, use NULL
            NULL AS scraped_at
        """

    # 1) Load CSV into a raw jobs table
    conn.execute(
        f"""
        CREATE OR REPLACE TABLE jobs AS
        WITH raw AS (
            SELECT *
            FROM read_csv_auto(?, header=TRUE, all_varchar=TRUE)
        )
        SELECT
            job_id,
            job_title,
            job_description,
            company_name,
            location,
            job_function,
            skills,
            degree_requirement,

            -- Safely parse various ISO-like timestamp formats for time_posted_parsed
            CASE
                WHEN time_posted_parsed IS NULL OR time_posted_parsed = '' THEN NULL
                ELSE COALESCE(
                    TRY_STRPTIME(time_posted_parsed, '%Y-%m-%dT%H:%M:%S.%f%z'),
                    TRY_STRPTIME(time_posted_parsed, '%Y-%m-%dT%H:%M:%S%z'),
                    TRY_STRPTIME(time_posted_parsed, '%Y-%m-%dT%H:%M:%S'),
                    TRY_STRPTIME(time_posted_parsed, '%Y-%m-%d %H:%M:%S')
                )
            END AS time_posted_parsed,

            application_link,

            -- Safely parse applicants as INTEGER
            TRY_CAST(NULLIF(num_applicants_int, '') AS INTEGER) AS num_applicants_int,

            work_mode,

            {scraped_at_clause}
        FROM raw;
        """,
        [str(DATA_CSV)],
    )

    # 2) Dedup by job_id and keep the "best" row per job_id
    conn.execute(
        """
        CREATE OR REPLACE TABLE jobs_dedup AS
        WITH ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY job_id
                    ORDER BY
                        -- prefer rows with non-null scraped_at (most recent scrape)
                        (scraped_at IS NULL) ASC,
                        -- then the most recent scrape time
                        scraped_at DESC
                ) AS rn
            FROM jobs
        )
        SELECT
            job_id,
            job_title,
            job_description,
            company_name,
            location,
            job_function,
            skills,
            degree_requirement,
            time_posted_parsed,
            application_link,
            num_applicants_int,
            work_mode,
            scraped_at
        FROM ranked
        WHERE rn = 1;
        """
    )

    # Replace original `jobs` with the deduped version
    conn.execute("DROP TABLE jobs")
    conn.execute("ALTER TABLE jobs_dedup RENAME TO jobs")

    # sorted view/table built from deduplicated jobs
    conn.execute(
        """
        CREATE OR REPLACE TABLE jobs_sorted AS
        SELECT *
        FROM jobs
        ORDER BY time_posted_parsed DESC NULLS LAST;
        """
    )

    logger.info("Deduped `jobs` table created (one row per job_id).")
    logger.info("Loaded %s into %s as table 'jobs'.", DATA_CSV, DB_PATH)

    conn.close()
    update_geo_locations()
    logger.info("Geo locations table updated.")


def hourly_ingestion(interval_seconds=1800):
    """
    Periodically rebuild DuckDB from parsed_jobs.csv.
    Set interval_seconds=3600 for "true hourly" runs.
    """
    import time

    while True:
        logger.info("Rebuilding DuckDB from parsed_jobs.csv...")
        try:
            main()
            logger.info("DuckDB rebuild completed.")
        except Exception:
            logger.exception("DuckDB rebuild failed.")
        logger.info("Sleeping %s seconds.", interval_seconds)
        time.sleep(interval_seconds)


if __name__ == "__main__":

    hourly_ingestion(interval_seconds=1800)

