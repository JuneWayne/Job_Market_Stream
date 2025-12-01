# duckdub_ingestion.py
import duckdb
from pathlib import Path

DATA_CSV = Path("data/parsed_jobs.csv")
DB_PATH = Path("data/jobs.duckdb")

def main():
    if not DATA_CSV.exists():
        raise FileNotFoundError(f"{DATA_CSV} does not exist. Run mongo_populate / consumer first.")

    conn = duckdb.connect(str(DB_PATH))

    conn.execute("""
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

            -- Safely parse various ISO-like timestamp formats
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

            work_mode
        FROM raw;
    """, [str(DATA_CSV)])

    conn.execute("""
        CREATE OR REPLACE TABLE jobs_sorted AS
        SELECT *
        FROM jobs
        ORDER BY time_posted_parsed DESC NULLS LAST;
    """)

    conn.execute("DROP TABLE jobs;")
    conn.execute("ALTER TABLE jobs_sorted RENAME TO jobs;")

    conn.close()
    print(f"Loaded {DATA_CSV} into {DB_PATH} as table 'jobs'.")

if __name__ == "__main__":
    main()
