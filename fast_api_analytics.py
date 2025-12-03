from typing import Optional, List, Dict, Any

from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import duckdb
from pathlib import Path
from datetime import datetime, timedelta, timezone
import traceback

DB_PATH = Path("data/jobs.duckdb")
app = FastAPI(title="Job Market Analytics API")

# allow any website to call this api
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)


# catch errors and return json with cors headers
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    print(f"Error: {exc}")
    traceback.print_exc()
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc)},
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
            "Access-Control-Allow-Headers": "*",
        },
    )


# quick check to see if the api is running
@app.get("/health")
async def health_check():
    return {"status": "ok", "cors": "enabled"}


# open a read-only connection to the duckdb file
def get_conn():
    if not DB_PATH.exists():
        raise RuntimeError(f"DuckDB file {DB_PATH} does not exist.")
    return duckdb.connect(str(DB_PATH), read_only=True)


# use linkedin posting time for all time-based queries
TIME_COLUMN = "time_posted_parsed"


# turn a datetime into friendly text like "2 days ago"
def friendly_age(dt: Optional[datetime]) -> Optional[str]:
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


# convert pandas dataframe to list of dicts
def df_to_records(df) -> List[Dict[str, Any]]:
    return df.to_dict(orient="records")


# basic stats like total jobs, unique companies, date range
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


# count jobs by function like data analyst, software engineer, etc
@app.get("/api/jobs_by_function")
def jobs_by_function(days: Optional[int] = None):
    conn = get_conn()
    time_col = TIME_COLUMN
    
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
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        df = conn.execute(
            f"""
            SELECT
                COALESCE(job_function, 'Unknown') AS job_function,
                COUNT(*) AS count
            FROM jobs
            WHERE {time_col} >= ?
            GROUP BY 1
            ORDER BY count DESC;
            """,
            [cutoff],
        ).fetchdf()
    conn.close()
    return df_to_records(df)


# count jobs by work mode like remote, hybrid, onsite
@app.get("/api/work_mode")
def work_mode(days: Optional[int] = None):
    conn = get_conn()
    time_col = TIME_COLUMN
    
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
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        df = conn.execute(
            f"""
            SELECT
                COALESCE(work_mode, 'Unknown') AS work_mode,
                COUNT(*) AS count
            FROM jobs
            WHERE {time_col} >= ?
            GROUP BY 1
            ORDER BY count DESC;
            """,
            [cutoff],
        ).fetchdf()
    conn.close()
    return df_to_records(df)


# top skills across all jobs, split by comma in the skills column
@app.get("/api/top_skills")
def top_skills(limit: int = 30, days: Optional[int] = None):
    conn = get_conn()
    time_col = TIME_COLUMN

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
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        base_query += f" AND {time_col} >= ? "
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


# job counts per day for time series charts
@app.get("/api/daily_counts")
def daily_counts(days: int = 180):
    conn = get_conn()
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
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


# job counts per hour for recent activity
@app.get("/api/hourly_counts")
def hourly_counts(hours: int = 24):
    conn = get_conn()
    time_col = TIME_COLUMN
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    df = conn.execute(
        f"""
        SELECT
            date_trunc('hour', {time_col}) AS hour,
            COUNT(*) AS job_count
        FROM jobs
        WHERE {time_col} IS NOT NULL
          AND {time_col} >= ?
        GROUP BY 1
        ORDER BY hour;
        """,
        [cutoff],
    ).fetchdf()
    conn.close()
    df["hour"] = df["hour"].astype(str)
    return df_to_records(df)


# shared query for beeswarm and map visualizations
def _raw_beeswarm_query(limit: int, hours: int):
    conn = get_conn()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    # try joining with geo_locations for lat/lng, fall back if table missing
    try:
        df = conn.execute(
            """
            SELECT
              j.job_id,
              j.job_title,
              j.job_description              AS summary,
              j.company_name,
              j.location,
              j.job_function                 AS "Job Function",
              ''                             AS "Industries",
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

    df["time_posted"] = df["time_posted_parsed"].apply(friendly_age)
    df["time_posted_parsed"] = df["time_posted_parsed"].astype(str)

    return df_to_records(df)


# jobs for beeswarm chart
@app.get("/api/beeswarm_jobs")
def beeswarm_jobs(
    limit: int = Query(2000, ge=1, le=5000),
    hours: int = Query(24, ge=1, le=24 * 7),
):
    return _raw_beeswarm_query(limit=limit, hours=hours)


# jobs for map visualization, same data as beeswarm
@app.get("/api/map_jobs")
def map_jobs_alias(
    limit: int = Query(2000, ge=1, le=5000),
    hours: int = Query(24, ge=1, le=24 * 7),
):
    return _raw_beeswarm_query(limit=limit, hours=hours)


# heatmap of average applicants by day of week and hour
@app.get("/api/competition_heatmap")
def competition_heatmap(days: int = 30):
    conn = get_conn()
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    df = conn.execute(
        """
        SELECT 
            EXTRACT(DOW FROM time_posted_parsed)  AS day_of_week,
            EXTRACT(HOUR FROM time_posted_parsed) AS hour,
            AVG(COALESCE(num_applicants_int, 0))  AS avg_applicants,
            COUNT(*)                              AS job_count
        FROM jobs
        WHERE time_posted_parsed >= ?
          AND time_posted_parsed IS NOT NULL
        GROUP BY 1, 2
        ORDER BY day_of_week, hour;
        """,
        [cutoff],
    ).fetchdf()
    conn.close()
    return df_to_records(df)


# skill nodes for network graph visualization
@app.get("/api/skills_network")
def skills_network(limit: int = 50, days: int = 30):
    conn = get_conn()
    time_col = TIME_COLUMN
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    top_skills_df = conn.execute(
        f"""
        WITH exploded AS (
            SELECT job_id, TRIM(skill) AS skill
            FROM jobs,
                 UNNEST(string_split(skills, ',')) AS t(skill)
            WHERE skills IS NOT NULL
              AND skills <> ''
              AND {time_col} >= ?
        )
        SELECT
            skill,
            COUNT(DISTINCT job_id) AS frequency
        FROM exploded
        WHERE skill <> ''
        GROUP BY skill
        ORDER BY frequency DESC
        LIMIT ?;
        """,
        [cutoff, limit],
    ).fetchdf()

    conn.close()

    nodes = [
        {"id": row["skill"], "label": row["skill"], "size": int(row["frequency"])}
        for _, row in top_skills_df.iterrows()
    ]

    return {"nodes": nodes, "edges": []}


# top companies and how fast they post jobs over time
@app.get("/api/company_velocity")
def company_velocity(days: int = 30, top_n: int = 20):
    conn = get_conn()
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    df = conn.execute(
        """
        WITH company_daily AS (
            SELECT
                company_name,
                DATE_TRUNC('day', time_posted_parsed) AS day,
                COUNT(*) AS daily_posts
            FROM jobs
            WHERE time_posted_parsed >= ?
              AND company_name IS NOT NULL
            GROUP BY company_name, day
        ),
        company_totals AS (
            SELECT company_name, SUM(daily_posts) AS total_posts
            FROM company_daily
            GROUP BY company_name
            ORDER BY total_posts DESC
            LIMIT ?
        )
        SELECT
            cd.company_name,
            cd.day,
            cd.daily_posts,
            ct.total_posts,
            SUM(cd.daily_posts) OVER (
                PARTITION BY cd.company_name
                ORDER BY cd.day
            ) AS cumulative_posts
        FROM company_daily cd
        JOIN company_totals ct
          ON cd.company_name = ct.company_name
        ORDER BY cd.company_name, cd.day;
        """,
        [cutoff, top_n],
    ).fetchdf()
    conn.close()
    df["day"] = df["day"].astype(str)
    return df_to_records(df)


# group jobs by age buckets like new, fresh, stale
@app.get("/api/job_lifecycle")
def job_lifecycle():
    conn = get_conn()
    df = conn.execute(
        """
        WITH job_ages AS (
            SELECT
                job_id,
                time_posted_parsed,
                num_applicants_int,
                EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - time_posted_parsed)) / 86400 AS days_old,
                CASE 
                    WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - time_posted_parsed)) / 86400 < 1  THEN 'New (<1 day)'
                    WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - time_posted_parsed)) / 86400 < 3  THEN 'Fresh (1-3 days)'
                    WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - time_posted_parsed)) / 86400 < 7  THEN 'Active (3-7 days)'
                    WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - time_posted_parsed)) / 86400 < 14 THEN 'Aging (1-2 weeks)'
                    WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - time_posted_parsed)) / 86400 < 30 THEN 'Stale (2-4 weeks)'
                    ELSE 'Very Old (>1 month)'
                END AS lifecycle_stage
            FROM jobs
            WHERE time_posted_parsed IS NOT NULL
        )
        SELECT
            lifecycle_stage,
            COUNT(*) AS job_count,
            AVG(num_applicants_int) AS avg_applicants
        FROM job_ages
        GROUP BY lifecycle_stage
        ORDER BY CASE lifecycle_stage
            WHEN 'New (<1 day)'      THEN 1
            WHEN 'Fresh (1-3 days)'  THEN 2
            WHEN 'Active (3-7 days)' THEN 3
            WHEN 'Aging (1-2 weeks)' THEN 4
            WHEN 'Stale (2-4 weeks)' THEN 5
            ELSE 6
        END;
        """
    ).fetchdf()
    conn.close()
    return df_to_records(df)


# compare skill mentions in recent vs older time window
@app.get("/api/trending_skills")
def trending_skills(days_back: int = 30, top_n: int = 20):
    conn = get_conn()
    time_col = TIME_COLUMN
    mid_date = datetime.now(timezone.utc) - timedelta(days=days_back // 2)
    start_date = datetime.now(timezone.utc) - timedelta(days=days_back)

    df = conn.execute(
        f"""
        WITH skill_periods AS (
            SELECT
                TRIM(skill) AS skill,
                CASE WHEN {time_col} >= ? THEN 'recent' ELSE 'older' END AS period,
                COUNT(*) AS mentions
            FROM jobs,
                 UNNEST(string_split(skills, ',')) AS t(skill)
            WHERE skills IS NOT NULL
              AND skills <> ''
              AND {time_col} >= ?
              AND TRIM(skill) <> ''
            GROUP BY TRIM(skill), period
        ),
        skill_comparison AS (
            SELECT
                skill,
                MAX(CASE WHEN period = 'recent' THEN mentions ELSE 0 END) AS recent_mentions,
                MAX(CASE WHEN period = 'older'  THEN mentions ELSE 0 END) AS older_mentions
            FROM skill_periods
            GROUP BY skill
            HAVING MAX(CASE WHEN period = 'older' THEN mentions ELSE 0 END) > 5
        )
        SELECT
            skill,
            recent_mentions,
            older_mentions,
            recent_mentions - older_mentions AS change,
            CASE
                WHEN older_mentions = 0 THEN 100 
                ELSE ((recent_mentions - older_mentions) * 100.0 / older_mentions)
            END AS change_percent,
            CASE
                WHEN recent_mentions > older_mentions THEN 'growing'
                WHEN recent_mentions < older_mentions THEN 'declining'
                ELSE 'stable'
            END AS trend
        FROM skill_comparison
        ORDER BY ABS(change_percent) DESC
        LIMIT ?;
        """,
        [mid_date, start_date, top_n],
    ).fetchdf()
    conn.close()
    return df_to_records(df)


# work mode percentages per week over time
@app.get("/api/remote_evolution")
def remote_evolution(days: int = 180):
    conn = get_conn()
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    df = conn.execute(
        """
        WITH weekly AS (
            SELECT
                DATE_TRUNC('week', time_posted_parsed) AS week,
                COALESCE(work_mode, 'Unknown')       AS work_mode,
                COUNT(*)                             AS cnt
            FROM jobs
            WHERE time_posted_parsed IS NOT NULL
              AND time_posted_parsed >= ?
            GROUP BY 1, 2
        ),
        totals AS (
            SELECT week, SUM(cnt) AS total_cnt
            FROM weekly
            GROUP BY week
        )
        SELECT
            w.week,
            w.work_mode,
            CASE WHEN t.total_cnt > 0
                 THEN 100.0 * w.cnt / t.total_cnt
                 ELSE 0
            END AS percentage
        FROM weekly w
        JOIN totals t USING (week)
        ORDER BY w.week, w.work_mode;
        """,
        [cutoff],
    ).fetchdf()
    conn.close()
    df["week"] = df["week"].astype(str)
    return df_to_records(df)


# count culture keywords in job descriptions
@app.get("/api/culture_keywords")
def culture_keywords(limit: int = 20):
    conn = get_conn()
    keywords = [
        "inclusive", "diverse", "collaborative", "remote",
        "flexible", "supportive", "growth", "learning",
        "ownership", "mentorship", "autonomy",
        "work-life balance", "transparent", "mission-driven",
        "innovative", "fast-paced", "team-first",
        "customer obsessed", "impact", "hybrid"
    ]

    df = conn.execute(
        "SELECT job_description FROM jobs WHERE job_description IS NOT NULL;"
    ).fetchdf()
    conn.close()

    total = len(df)
    if total == 0:
        return []

    desc_series = df["job_description"].astype(str)

    results: List[Dict[str, Any]] = []
    for kw in keywords:
        count = int(desc_series.str.contains(kw, case=False, regex=False).sum())
        if count > 0:
            results.append(
                {
                    "keyword": kw,
                    "count": count,
                    "percentage": round(100.0 * count / total, 1),
                }
            )

    results.sort(key=lambda x: x["count"], reverse=True)
    return results[:limit]


# jobs posted in last hour and last 24 hours with trending info
@app.get("/api/pulse_metrics")
def pulse_metrics():
    conn = get_conn()
    time_col = TIME_COLUMN

    now = datetime.now(timezone.utc)
    last_hour = now - timedelta(hours=1)
    last_24h = now - timedelta(hours=24)
    last_week = now - timedelta(days=7)

    row = conn.execute(
        f"""
        SELECT 
            COUNT(CASE WHEN {time_col} >= ? THEN 1 END) AS last_hour_jobs,
            COUNT(CASE WHEN {time_col} >= ? THEN 1 END) AS last_24h_jobs,
            COUNT(CASE WHEN {time_col} >= ? THEN 1 END) / (7.0 * 24) AS weekly_avg_per_hour,
            MODE() WITHIN GROUP (
                ORDER BY CASE WHEN {time_col} >= ? THEN location END
            ) AS hottest_location,
            MODE() WITHIN GROUP (
                ORDER BY CASE WHEN {time_col} >= ? THEN job_function END
            ) AS hottest_function,
            MAX(CASE WHEN {time_col} >= ? THEN num_applicants_int END) AS max_applicants_recent,
            AVG(CASE WHEN {time_col} >= ? THEN num_applicants_int END) AS avg_applicants_recent
        FROM jobs
        WHERE {time_col} IS NOT NULL;
        """,
        [
            last_hour,
            last_24h,
            last_week,
            last_24h,
            last_24h,
            last_24h,
            last_24h,
        ],
    ).fetchone()

    conn.close()

    last_hour_jobs = row[0] or 0
    last_24h_jobs = row[1] or 0
    weekly_avg = row[2] or 1.0
    hottest_location = row[3] or "Unknown"
    hottest_function = row[4] or "Unknown"
    max_applicants_recent = row[5] or 0
    avg_applicants_recent = float(row[6] or 0.0)

    hour_change = (
        ((last_hour_jobs - weekly_avg) / weekly_avg * 100.0) if weekly_avg > 0 else 0.0
    )

    return {
        "last_hour": {
            "job_count": last_hour_jobs,
            "vs_weekly_avg": round(hour_change, 1),
            "trend": "up" if hour_change > 0 else "down" if hour_change < 0 else "stable",
        },
        "last_24h": {
            "job_count": last_24h_jobs,
            "hottest_location": hottest_location,
            "hottest_function": hottest_function,
            "max_applicants": max_applicants_recent,
            "avg_applicants": round(avg_applicants_recent, 1),
        },
    }


# list all available endpoints
@app.get("/")
async def root():
    return {
        "message": "Job Market Analytics API - Advanced",
        "endpoints": [
            "/api/overview",
            "/api/jobs_by_function",
            "/api/work_mode",
            "/api/top_skills",
            "/api/daily_counts",
            "/api/hourly_counts",
            "/api/beeswarm_jobs",
            "/api/map_jobs",
            "/api/competition_heatmap",
            "/api/skills_network",
            "/api/company_velocity",
            "/api/job_lifecycle",
            "/api/trending_skills",
            "/api/remote_evolution",
            "/api/culture_keywords",
            "/api/pulse_metrics",
        ],
    }
