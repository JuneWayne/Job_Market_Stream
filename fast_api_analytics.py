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
# 7) Map + Beeswarm jobs  (robust against missing geo_locations)
# ------------------------------------------------------------------
def _raw_beeswarm_query(limit: int, days: int):
    """
    Shared query used by /api/beeswarm_jobs and /api/map_jobs.

    Tries a rich schema with geo_locations; if that fails
    (e.g., table/columns not present yet), falls back to a
    simpler query with NULL latitude/longitude.
    """
    conn = get_conn()
    cutoff = datetime.now() - timedelta(days=days)

    try:
        # Preferred query: join with geo_locations for lat/lon
        df = conn.execute(
            """
            SELECT
            j.job_id,
            j.job_title,
            j.job_description AS summary,
            j.company_name,
            j.location,
            COALESCE(j.job_function, 'Unknown') AS job_function,
            COALESCE(j.industry, '') AS industry,
            j.skills AS skills,
            j.degree_qualifications AS degree_qualifications,
            j.time_posted_parsed,
            j.job_link AS job_link,
            j.num_applicants AS num_applicants,
            j.work_mode,
            g.latitude,
            g.longitude
            FROM jobs AS j
            LEFT JOIN geo_locations AS g
            ON j.location = g.location
            WHERE j.time_posted_parsed IS NOT NULL
            AND j.time_posted_parsed >= ?
            ORDER BY j.time_posted_parsed DESC
            LIMIT ?;
            """,
            [cutoff, limit],
        ).fetchdf()

    except duckdb.Error as e:
        # Fallback: no geo_locations table/columns yet
        print(f"[beeswarm] WARNING: falling back without geo_locations: {e}")
        df = conn.execute(
            """
            SELECT
              job_id,
              job_title,
              job_description              AS summary,
              company_name,
              location,
              job_function                 AS "Job Function",
              ''                           AS "Industries",
              skills                       AS skills_desired,
              degree_requirement           AS degree_qualifications,
              time_posted_parsed,
              application_link,
              application_link             AS job_link,
              num_applicants_int           AS num_applicants,
              work_mode,
              NULL                         AS latitude,
              NULL                         AS longitude
            FROM jobs
            WHERE time_posted_parsed IS NOT NULL
              AND time_posted_parsed >= ?
            ORDER BY time_posted_parsed DESC
            LIMIT ?;
            """,
            [cutoff, limit],
        ).fetchdf()
    finally:
        conn.close()

    # Add human-readable age + stringified timestamp
    df["time_posted"] = df["time_posted_parsed"].apply(friendly_age)
    df["time_posted_parsed"] = df["time_posted_parsed"].astype(str)

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
