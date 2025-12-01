from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import duckdb
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

DB_PATH = Path("data/jobs.duckdb")

app = FastAPI(title="Job Analytics API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://junewayne.github.io",                   # your GitHub Pages root
        "https://junewayne.github.io/job-market-stream", # repo site
        "http://localhost:8000",                         # local testing
        "http://127.0.0.1:8000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_conn():
    if not DB_PATH.exists():
        raise RuntimeError(f"DuckDB file {DB_PATH} does not exist.")
    # read_only so we do not accidentally mutate
    return duckdb.connect(str(DB_PATH), read_only=True)


# ------------------------------------------------------------------
# Small helpers
# ------------------------------------------------------------------


def friendly_age(dt: Optional[datetime]) -> Optional[str]:
    """
    Turn a timestamp into a human-friendly "2 days ago" string.
    Uses UTC; if your timestamps are naive they are treated as UTC.
    """
    if dt is None:
        return None

    if not isinstance(dt, datetime):
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    delta = now - dt
    days = delta.days
    hours = delta.seconds // 3600

    if days <= 0:
        if hours <= 1:
            return "1 hour ago"
        return f"{hours} hours ago"
    if days == 1:
        return "1 day ago"
    if days < 7:
        return f"{days} days ago"
    weeks = days // 7
    if weeks < 4:
        return f"{weeks} weeks ago"
    months = days // 30
    return f"{months} months ago"


def df_to_records(df) -> List[Dict[str, Any]]:
    """Convenience wrapper for JSON serialisation."""
    return df.to_dict(orient="records")


# ------------------------------------------------------------------
# 1) Overview stats
# ------------------------------------------------------------------


@app.get("/api/overview")
def overview():
    conn = get_conn()
    row = conn.execute(
        """
        SELECT
            COUNT(*)                           AS total_jobs,
            COUNT(DISTINCT company_name)       AS unique_companies,
            COUNT(DISTINCT location)           AS unique_locations,
            MIN(time_posted_parsed)            AS earliest_posting,
            MAX(time_posted_parsed)            AS latest_posting
        FROM jobs;
    """
    ).fetchone()
    conn.close()

    return {
        "total_jobs": row[0],
        "unique_companies": row[1],
        "unique_locations": row[2],
        "earliest_posting": row[3],
        "latest_posting": row[4],
    }


# ------------------------------------------------------------------
# 2) Jobs by function
# ------------------------------------------------------------------


@app.get("/api/jobs_by_function")
def jobs_by_function(days: int | None = None):
    """
    If days is provided, filter to last `days` days.
    Otherwise, use all history.
    """
    conn = get_conn()
    if days is None:
        df = conn.execute(
            """
            SELECT
                COALESCE(job_function, 'Unknown') AS job_function,
                COUNT(*) AS count
            FROM jobs
            GROUP BY 1
            ORDER BY count DESC;
        """
        ).fetchdf()
    else:
        cutoff = datetime.now() - timedelta(days=days)
        df = conn.execute(
            """
            SELECT
                COALESCE(job_function, 'Unknown') AS job_function,
                COUNT(*) AS count
            FROM jobs
            WHERE time_posted_parsed >= ?
            GROUP BY 1
            ORDER BY count DESC;
        """,
            [cutoff],
        ).fetchdf()
    conn.close()
    return df_to_records(df)


# ------------------------------------------------------------------
# 3) Work mode distribution
# ------------------------------------------------------------------


@app.get("/api/work_mode")
def work_mode(days: int | None = None):
    conn = get_conn()
    if days is None:
        df = conn.execute(
            """
            SELECT
                COALESCE(work_mode, 'Unknown') AS work_mode,
                COUNT(*) AS count
            FROM jobs
            GROUP BY 1
            ORDER BY count DESC;
        """
        ).fetchdf()
    else:
        cutoff = datetime.now() - timedelta(days=days)
        df = conn.execute(
            """
            SELECT
                COALESCE(work_mode, 'Unknown') AS work_mode,
                COUNT(*) AS count
            FROM jobs
            WHERE time_posted_parsed >= ?
            GROUP BY 1
            ORDER BY count DESC;
        """,
            [cutoff],
        ).fetchdf()
    conn.close()
    return df_to_records(df)


# ------------------------------------------------------------------
# 4) Top skills (explode comma list)
# ------------------------------------------------------------------


@app.get("/api/top_skills")
def top_skills(limit: int = 30, days: int | None = None):
    """
    Returns top N skills across all jobs (or recent window if days is given).
    Assumes a column `skills` with comma-separated values.
    """
    conn = get_conn()

    base_query = """
        WITH exploded AS (
            SELECT
                trim(skill) AS skill
            FROM jobs,
            UNNEST(string_split(skills, ',')) AS t(skill)
            WHERE skills IS NOT NULL AND skills <> ''
    """

    if days is not None:
        cutoff = datetime.now() - timedelta(days=days)
        base_query += " AND time_posted_parsed >= ? "

    base_query += """
        )
        SELECT
            skill,
            COUNT(*) AS count
        FROM exploded
        WHERE skill <> ''
        GROUP BY 1
        ORDER BY count DESC
        LIMIT ?;
    """

    if days is not None:
        df = conn.execute(base_query, [cutoff, limit]).fetchdf()
    else:
        df = conn.execute(base_query, [limit]).fetchdf()

    conn.close()
    return df_to_records(df)


# ------------------------------------------------------------------
# 5) Daily time series
# ------------------------------------------------------------------


@app.get("/api/daily_counts")
def daily_counts(days: int = 30):
    """
    Jobs per day for the last `days` days, ordered by date.
    """
    conn = get_conn()
    cutoff = datetime.now() - timedelta(days=days)
    df = conn.execute(
        """
        SELECT
            date_trunc('day', time_posted_parsed) AS day,
            COUNT(*) AS job_count
        FROM jobs
        WHERE time_posted_parsed IS NOT NULL
          AND time_posted_parsed >= ?
        GROUP BY 1
        ORDER BY day;
    """,
        [cutoff],
    ).fetchdf()
    conn.close()
    # Convert Timestamp to ISO strings for JSON friendliness
    df["day"] = df["day"].astype(str)
    return df_to_records(df)


# ------------------------------------------------------------------
# 6) NEW – Map + Beeswarm endpoints
# ------------------------------------------------------------------


@app.get("/api/map_jobs")
def map_jobs(
    limit: int = Query(1500, ge=1, le=5000),
    days: int = Query(365, ge=1, le=730),
):
    """
    Job-level records used for both the map popups and the beeswarm.

    Assumed columns in `jobs` (rename if needed):
      - job_title
      - company_name
      - location
      - time_posted_parsed (TIMESTAMP)
      - num_applicants (INT, NULL allowed)
      - work_mode
      - job_function
      - industry
      - skills (comma-separated; aliased as skills_desired)
      - degree_qualifications
      - summary
      - job_url
      - latitude, longitude (FLOAT; NULL allowed)
    """
    conn = get_conn()
    cutoff = datetime.now() - timedelta(days=days)

    df = conn.execute(
        """
        SELECT
          job_title,
          company_name,
          location,
          time_posted_parsed,
          num_applicants,
          work_mode,
          job_function,
          industry,
          skills               AS skills_desired,
          degree_qualifications,
          summary,
          job_url              AS job_link,
          latitude,
          longitude
        FROM jobs
        WHERE time_posted_parsed IS NOT NULL
          AND time_posted_parsed >= ?
          AND location IS NOT NULL
        ORDER BY time_posted_parsed DESC
        LIMIT ?;
        """,
        [cutoff, limit],
    ).fetchdf()
    conn.close()

    # Add human-readable "time_posted" and (optionally) drop raw timestamp
    df["time_posted"] = df["time_posted_parsed"].apply(friendly_age)
    df["time_posted_parsed"] = df["time_posted_parsed"].astype(str)  # ISO strings

    return df_to_records(df)


@app.get("/api/map_location_summaries")
def map_location_summaries(
    days: int = Query(365, ge=1, le=730),
):
    """
    Per-location summary: number of jobs + average applicants.
    Used to color the map bubbles.
    """
    conn = get_conn()
    cutoff = datetime.now() - timedelta(days=days)

    df = conn.execute(
        """
        SELECT
          location,
          COUNT(*)             AS number_of_jobs,
          AVG(num_applicants)  AS average_applicants
        FROM jobs
        WHERE time_posted_parsed IS NOT NULL
          AND time_posted_parsed >= ?
          AND location IS NOT NULL
        GROUP BY location;
        """,
        [cutoff],
    ).fetchdf()
    conn.close()

    return df_to_records(df)


@app.get("/api/map_locations")
def map_locations():
    """
    Distinct locations with latitude/longitude for the map.
    Assumes columns `latitude` and `longitude` exist in jobs.
    If you store them elsewhere, change this to join to that table.
    """
    conn = get_conn()
    df = conn.execute(
        """
        SELECT
          location,
          AVG(latitude)  AS latitude,
          AVG(longitude) AS longitude
        FROM jobs
        WHERE latitude IS NOT NULL
          AND longitude IS NOT NULL
        GROUP BY location;
        """
    ).fetchdf()
    conn.close()
    return df_to_records(df)
