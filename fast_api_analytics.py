# fast_api_analytics.py

from typing import Optional, List, Dict, Any

from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import duckdb
from pathlib import Path
from datetime import datetime, timedelta, timezone
import traceback

DB_PATH = Path("data/jobs.duckdb")

# -------------------------------------------------------------
# Create FastAPI app
# -------------------------------------------------------------
app = FastAPI(title="Job Market Analytics API")

# -------------------------------------------------------------
# CORS - MUST be added IMMEDIATELY after app creation
# -------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],  # Add this to expose all headers
)

# -------------------------------------------------------------
# Global exception handler to ensure CORS headers on errors
# -------------------------------------------------------------
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Ensure CORS headers are sent even when errors occur."""
    print(f"Error: {exc}")
    traceback.print_exc()
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc)},
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
            "Access-Control-Allow-Headers": "*",
        }
    )

# -------------------------------------------------------------
# Health check endpoint for debugging
# -------------------------------------------------------------
@app.get("/health")
async def health_check():
    """Simple health check endpoint."""
    return {"status": "ok", "cors": "enabled"}

def get_conn():
    """Open a read-only connection to the DuckDB file."""
    if not DB_PATH.exists():
        raise RuntimeError(f"DuckDB file {DB_PATH} does not exist.")
    return duckdb.connect(str(DB_PATH), read_only=True)

# -------------------------------------------------------------
# Helpers
# -------------------------------------------------------------
def friendly_age(dt: Optional[datetime]) -> Optional[str]:
    """
    Turn a datetime into '2 days ago' style text.
    Returns None if the input is missing or not a datetime.
    """
    if dt is None or not isinstance(dt, datetime):
        return None

    # Treat naive timestamps as UTC
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
    """Convert a pandas DataFrame to a list of dict records."""
    return df.to_dict(orient="records")


# -------------------------------------------------------------
# 1) Overview
# -------------------------------------------------------------
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


# -------------------------------------------------------------
# 2) Jobs by function
# -------------------------------------------------------------
@app.get("/api/jobs_by_function")
def jobs_by_function(days: Optional[int] = None):
    """
    Jobs grouped by job_function.
    If days is provided, filter to last `days` based on time_posted_parsed.
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


# -------------------------------------------------------------
# 3) Work mode distribution
# -------------------------------------------------------------
@app.get("/api/work_mode")
def work_mode(days: Optional[int] = None):
    """
    Jobs grouped by work_mode (Remote / Hybrid / On-site / Unknown).
    If days is provided, filter by time_posted_parsed.
    """
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


# -------------------------------------------------------------
# 4) Top skills (comma-split)
# -------------------------------------------------------------
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


# -------------------------------------------------------------
# 5) Daily counts (for 180-day line chart)
# -------------------------------------------------------------
@app.get("/api/daily_counts")
def daily_counts(days: int = 180):
    """
    Number of jobs by posting day, based on time_posted_parsed.
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
    df["day"] = df["day"].astype(str)
    return df_to_records(df)


# -------------------------------------------------------------
# 6) Hourly counts – based on time_posted_parsed
# -------------------------------------------------------------
@app.get("/api/hourly_counts")
def hourly_counts(hours: int = 24):
    """
    Jobs per hour for the last `hours` hours,
    based on time_posted_parsed.
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


# -------------------------------------------------------------
# 7) Map + Beeswarm jobs (last N hours, default 24)
# -------------------------------------------------------------
def _raw_beeswarm_query(limit: int, hours: int):
    """
    Shared query used by /api/beeswarm_jobs and /api/map_jobs.

    Returns jobs whose time_posted_parsed is within the last `hours` hours.
    """
    conn = get_conn()
    cutoff = datetime.now() - timedelta(hours=hours)

    try:
        # Preferred query: join with geo_locations for lat/lon
        df = conn.execute(
            """
            SELECT
              j.job_id,
              j.job_title,
              j.job_description              AS summary,
              j.company_name,
              j.location,
              j.job_function                 AS "Job Function",
              ''                             AS "Industries",          -- placeholder
              j.skills                       AS skills_desired,
              j.degree_requirement           AS degree_qualifications,
              j.time_posted_parsed,
              j.application_link,
              j.application_link             AS job_link,
              j.num_applicants_int           AS num_applicants,
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
        # Fallback if geo_locations is missing
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
    hours: int = Query(24, ge=1, le=24 * 7),
):
    """
    Primary endpoint used by beeswarm + map.
    Returns jobs posted within the last `hours` hours.
    """
    return _raw_beeswarm_query(limit=limit, hours=hours)


@app.get("/api/map_jobs")
def map_jobs_alias(
    limit: int = Query(2000, ge=1, le=5000),
    hours: int = Query(24, ge=1, le=24 * 7),
):
    """
    Alias so the frontend can call /api/map_jobs or /api/beeswarm_jobs.
    """
    return _raw_beeswarm_query(limit=limit, hours=hours)


# -------------------------------------------------------------
# OPTIONS handler for preflight requests
# -------------------------------------------------------------
@app.options("/{full_path:path}")
async def options_handler():
    """Handle preflight OPTIONS requests."""
    return JSONResponse(
        content={},
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
            "Access-Control-Allow-Headers": "*",
        }
    )

# -------------------------------------------------------------
# Root endpoint
# -------------------------------------------------------------
@app.get("/")
async def root():
    """Root endpoint with API info."""
    return {
        "message": "Job Market Analytics API",
        "endpoints": [
            "/api/overview",
            "/api/jobs_by_function",
            "/api/work_mode",
            "/api/top_skills",
            "/api/daily_counts",
            "/api/hourly_counts",
            "/api/beeswarm_jobs",
            "/api/map_jobs",
            "/health"
        ],
        "cors": "enabled"
    }