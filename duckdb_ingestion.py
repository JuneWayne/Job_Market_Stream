import duckdb
from pathlib import Path
import logging
import csv
from geo_encode import update_geo_locations

# Paths to the main CSV we ingest and the DuckDB file we maintain
DATA_CSV = Path("data/parsed_jobs.csv")
DB_PATH = Path("data/jobs.duckdb")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("duckdb_ingestion")


def csv_has_column(csv_path, column_name):
    # Quick helper: check if a given column name exists in the CSV header
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader, [])
        return column_name in header


def main():
    # Make sure the CSV we want to ingest is actually there
    if not DATA_CSV.exists():
        raise FileNotFoundError(
            f"{DATA_CSV} does not exist. Run mongo_populate / consumer first."
        )

    # Open (or create) the DuckDB file
    conn = duckdb.connect(str(DB_PATH))
    
    # Check if the CSV includes a scraped_at column so we know how to handle it
    has_scraped_at = csv_has_column(DATA_CSV, 'scraped_at')
    logger.info(f"CSV has scraped_at column: {has_scraped_at}")

    # Build the scraped_at expression depending on whether that column exists
    if has_scraped_at:
        scraped_at_clause = """
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
        # Cast NULL to TIMESTAMPTZ so it matches time_posted_parsed type
        # This is important for COALESCE to work in the API queries
        scraped_at_clause = "CAST(NULL AS TIMESTAMP WITH TIME ZONE) AS scraped_at"

    # Step 1: read the CSV into DuckDB and normalize basic fields / timestamps
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

            -- Try a few common formats to turn time_posted_parsed into a real timestamp
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

            -- Turn num_applicants_int into an integer if possible, otherwise NULL
            TRY_CAST(NULLIF(num_applicants_int, '') AS INTEGER) AS num_applicants_int,

            work_mode,

            {scraped_at_clause}
        FROM raw;
        """,
        [str(DATA_CSV)],
    )

    # Step 2: keep only one row per job_id, preferring the latest scrape
    conn.execute(
        """
        CREATE OR REPLACE TABLE jobs_dedup AS
        WITH ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY job_id
                    ORDER BY
                        (scraped_at IS NULL) ASC,
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

    # Swap out the old jobs table for the cleaned, deduped version
    conn.execute("DROP TABLE jobs")
    conn.execute("ALTER TABLE jobs_dedup RENAME TO jobs")

    # Step 3: create a version sorted by posting time for easier querying
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

    # Close DB connection before running geo encoding
    conn.close()

    # After jobs are updated, refresh geo-coded locations as well
    update_geo_locations()
    logger.info("Geo locations table updated.")


def hourly_ingestion(interval_seconds=1800):
    # Simple loop: rebuild the DuckDB file every X seconds (30 min by default)
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
    # If we run this file directly, start the periodic ingestion loop
    hourly_ingestion(interval_seconds=1800)
