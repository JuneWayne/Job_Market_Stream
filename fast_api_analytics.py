# fast_api_analytics.py

from typing import Optional, List, Dict, Any

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import duckdb
from pathlib import Path
from datetime import datetime, timedelta, timezone

DB_PATH = Path("data/jobs.duckdb")

app = FastAPI(title="Job Market Analytics API")

# ------------------------------------------------------------------
# CORS – keep it permissive for now (you can tighten later)
# ------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],         
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_conn():
    if not DB_PATH.exists():
        raise RuntimeError(f"DuckDB file {DB_PATH} does not exist.")
    return duckdb.connect(str(DB_PATH), read_only=True)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
def friendly_age(dt: Optional[datetime]) -> Optional[str]:
    """Turn a timestamp into '2 days ago' style text."""
    if dt is None or not isinstance(dt, datetime):
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
    return df.to_dict(orient="records")


# ------------------------------------------------------------------
# 1) Overview
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
def jobs_by_function(days: Optional[int] = None):
    """
    If days is provided, filter to last `days` days. Otherwise, use all history.
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
def work_mode(days: Optional[int] = None):
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
# 4) Top skills (comma-split)
# ------------------------------------------------------------------
@app.get("/api/top_skills")
def top_skills(limit: int = 30, days: Optional[int] = None):
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

    params: List[Any] = []

    if days is not None:
        cutoff = datetime.now() - timedelta(days=days)
        base_query += " AND time_posted_parsed >= ? "
        params.append(cutoff)

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
    params.append(limit)

    df = conn.execute(base_query, params).fetchdf()
    conn.close()
    return df_to_records(df)


# ------------------------------------------------------------------
# 5) Daily counts (for 180-day line chart)
# ------------------------------------------------------------------
@app.get("/api/daily_counts")
def daily_counts(days: int = 180):
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
    df["day"] = df["day"].astype(str)
    return df_to_records(df)


# ------------------------------------------------------------------
# 6) Hourly counts (last N hours, default 24)
# ------------------------------------------------------------------
@app.get("/api/hourly_counts")
def hourly_counts(hours: int = 24):
    """
    Jobs per hour for the last `hours` hours.
    This drives your 'past 24 hours' bubble trend.
    """
    conn = get_conn()
    cutoff = datetime.now() - timedelta(hours=hours)
    df = conn.execute(
        """
        SELECT
            date_trunc('hour', time_posted_parsed) AS hour,
            COUNT(*) AS job_count
        FROM jobs
        WHERE time_posted_parsed IS NOT NULL
          AND time_posted_parsed >= ?
        GROUP BY 1
        ORDER BY hour;
        """,
        [cutoff],
    ).fetchdf()
    conn.close()
    df["hour"] = df["hour"].astype(str)
    return df_to_records(df)


# ------------------------------------------------------------------
# 7) Map + Beeswarm jobs
# ------------------------------------------------------------------
def _raw_beeswarm_query(limit: int, days: int):
    """
    Shared query used by /api/beeswarm_jobs and /api/map_jobs.

    It inspects the DuckDB schema and only selects columns that exist.
    Missing columns are returned as NULL but aliased to the names that
    the frontend expects (skills_desired, job_link, latitude, longitude).
    """
    conn = get_conn()
    cutoff = datetime.now() - timedelta(days=days)

    # Look at the current columns in the jobs table
    cols = {
        row[1] for row in conn.execute("PRAGMA table_info('jobs');").fetchall()
    }

    def has(col: str) -> bool:
        return col in cols

    select_parts: List[str] = []

    # Required / very likely columns
    select_parts.append("job_title" if has("job_title") else "NULL AS job_title")
    select_parts.append("company_name" if has("company_name") else "NULL AS company_name")
    select_parts.append("location" if has("location") else "NULL AS location")
    select_parts.append(
        "time_posted_parsed"
        if has("time_posted_parsed")
        else "NULL AS time_posted_parsed"
    )
    select_parts.append(
        "num_applicants"
        if has("num_applicants")
        else "NULL AS num_applicants"
    )
    select_parts.append("work_mode" if has("work_mode") else "NULL AS work_mode")
    select_parts.append(
        "job_function" if has("job_function") else "NULL AS job_function"
    )
    select_parts.append("industry" if has("industry") else "NULL AS industry")

    # Skills → alias to skills_desired
    if has("skills_desired"):
        select_parts.append("skills_desired")
    elif has("skills"):
        select_parts.append("skills AS skills_desired")
    else:
        select_parts.append("NULL AS skills_desired")

    # Degree + summary
    select_parts.append(
        "degree_qualifications"
        if has("degree_qualifications")
        else "NULL AS degree_qualifications"
    )
    select_parts.append("summary" if has("summary") else "NULL AS summary")

    # Job URL → alias to job_link
    if has("job_link"):
        select_parts.append("job_link")
    elif has("job_url"):
        select_parts.append("job_url AS job_link")
    else:
        select_parts.append("NULL AS job_link")

    # Latitude / longitude
    select_parts.append("latitude" if has("latitude") else "NULL AS latitude")
    select_parts.append("longitude" if has("longitude") else "NULL AS longitude")

    select_clause = ",\n          ".join(select_parts)

    # We know from other endpoints that time_posted_parsed exists;
    # but still guard the WHERE in case schema changes again.
    if has("time_posted_parsed"):
        where_clause = """
        WHERE time_posted_parsed IS NOT NULL
          AND time_posted_parsed >= ?
        """
        params = [cutoff, limit]
    else:
        where_clause = ""
        params = [limit]

    query = f"""
        SELECT
          {select_clause}
        FROM jobs
        {where_clause}
        ORDER BY time_posted_parsed DESC
        LIMIT ?;
    """

    df = conn.execute(query, params).fetchdf()
    conn.close()

    # Add friendly age + make timestamps JSON-friendly
    if "time_posted_parsed" in df.columns:
        df["time_posted"] = df["time_posted_parsed"].apply(friendly_age)
        df["time_posted_parsed"] = df["time_posted_parsed"].astype(str)
    else:
        df["time_posted"] = None

    return df_to_records(df)


@app.get("/api/beeswarm_jobs")
def beeswarm_jobs(
    limit: int = Query(2000, ge=1, le=5000),
    days: int = Query(365, ge=1, le=730),
):
    """Primary endpoint used by beeswarm + map."""
    return _raw_beeswarm_query(limit=limit, days=days)


@app.get("/api/map_jobs")
def map_jobs_alias(
    limit: int = Query(2000, ge=1, le=5000),
    days: int = Query(365, ge=1, le=730),
):
    """
    Alias so the frontend can call /api/map_jobs or /api/beeswarm_jobs.
    """
    return _raw_beeswarm_query(limit=limit, days=days)

