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


# check if a column exists in the csv header
def csv_has_column(csv_path, column_name):
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
    
    has_scraped_at = csv_has_column(DATA_CSV, 'scraped_at')
    logger.info(f"CSV has scraped_at column: {has_scraped_at}")

    # build scraped_at parsing clause based on whether column exists
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
        scraped_at_clause = "CAST(NULL AS TIMESTAMP WITH TIME ZONE) AS scraped_at"

    # load csv into duckdb with timestamp parsing
    conn.execute(
        f"""
        CREATE OR REPLACE TABLE jobs AS
        WITH raw AS (
            SELECT *
            FROM read_csv(?, 
                header=TRUE, 
                all_varchar=TRUE,
                ignore_errors=TRUE,
                null_padding=TRUE,
                quote='"',
                escape='"'
            )
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

            TRY_CAST(NULLIF(num_applicants_int, '') AS INTEGER) AS num_applicants_int,

            work_mode,

            {scraped_at_clause}
        FROM raw;
        """,
        [str(DATA_CSV)],
    )

    # deduplicate by job_id, keep most recent scrape
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

    conn.execute("DROP TABLE jobs")
    conn.execute("ALTER TABLE jobs_dedup RENAME TO jobs")

    # create sorted version for faster queries
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

    # add lat/lng for job locations
    update_geo_locations()
    logger.info("Geo locations table updated.")


# rebuild duckdb every few minutes
def hourly_ingestion(interval_seconds=180):
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
    hourly_ingestion(interval_seconds=180)
