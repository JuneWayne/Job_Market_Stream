# api.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import duckdb
from pathlib import Path
from datetime import datetime, timedelta

DB_PATH = Path("data/jobs.duckdb")

app = FastAPI(title="Job Market Analytics API")

# Allow your local HTML / dev origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # in prod, restrict this
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_conn():
    if not DB_PATH.exists():
        raise RuntimeError(f"DuckDB file {DB_PATH} does not exist.")
    # read_only is nice so we do not accidentally mutate
    return duckdb.connect(str(DB_PATH), read_only=True)


# ---------- 1) Overview stats ----------
@app.get("/api/overview")
def overview():
    conn = get_conn()
    row = conn.execute("""
        SELECT
            COUNT(*)                           AS total_jobs,
            COUNT(DISTINCT company_name)       AS unique_companies,
            COUNT(DISTINCT location)           AS unique_locations,
            MIN(time_posted_parsed)            AS earliest_posting,
            MAX(time_posted_parsed)            AS latest_posting
        FROM jobs;
    """).fetchone()
    conn.close()

    return {
        "total_jobs": row[0],
        "unique_companies": row[1],
        "unique_locations": row[2],
        "earliest_posting": row[3],
        "latest_posting": row[4],
    }


# ---------- 2) Jobs by function ----------
@app.get("/api/jobs_by_function")
def jobs_by_function(days: int | None = None):
    """
    If days is provided, filter to last `days` days.
    Otherwise, use all history.
    """
    conn = get_conn()
    if days is None:
        df = conn.execute("""
            SELECT
                COALESCE(job_function, 'Unknown') AS job_function,
                COUNT(*) AS count
            FROM jobs
            GROUP BY 1
            ORDER BY count DESC;
        """).fetchdf()
    else:
        cutoff = datetime.now() - timedelta(days=days)
        df = conn.execute("""
            SELECT
                COALESCE(job_function, 'Unknown') AS job_function,
                COUNT(*) AS count
            FROM jobs
            WHERE time_posted_parsed >= ?
            GROUP BY 1
            ORDER BY count DESC;
        """, [cutoff]).fetchdf()
    conn.close()
    return df.to_dict(orient="records")


# ---------- 3) Work mode distribution ----------
@app.get("/api/work_mode")
def work_mode(days: int | None = None):
    conn = get_conn()
    if days is None:
        df = conn.execute("""
            SELECT
                COALESCE(work_mode, 'Unknown') AS work_mode,
                COUNT(*) AS count
            FROM jobs
            GROUP BY 1
            ORDER BY count DESC;
        """).fetchdf()
    else:
        cutoff = datetime.now() - timedelta(days=days)
        df = conn.execute("""
            SELECT
                COALESCE(work_mode, 'Unknown') AS work_mode,
                COUNT(*) AS count
            FROM jobs
            WHERE time_posted_parsed >= ?
            GROUP BY 1
            ORDER BY count DESC;
        """, [cutoff]).fetchdf()
    conn.close()
    return df.to_dict(orient="records")


# ---------- 4) Top skills (explode comma list) ----------
@app.get("/api/top_skills")
def top_skills(limit: int = 30, days: int | None = None):
    """
    Returns top N skills across all jobs (or recent window if days is given).
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
    return df.to_dict(orient="records")


# ---------- 5) Daily time series ----------
@app.get("/api/daily_counts")
def daily_counts(days: int = 30):
    """
    Jobs per day for the last `days` days, ordered by date.
    """
    conn = get_conn()
    cutoff = datetime.now() - timedelta(days=days)
    df = conn.execute("""
        SELECT
            date_trunc('day', time_posted_parsed) AS day,
            COUNT(*) AS job_count
        FROM jobs
        WHERE time_posted_parsed IS NOT NULL
          AND time_posted_parsed >= ?
        GROUP BY 1
        ORDER BY day;
    """, [cutoff]).fetchdf()
    conn.close()
    # Convert Timestamp to ISO strings for JSON friendliness
    df["day"] = df["day"].astype(str)
    return df.to_dict(orient="records")
