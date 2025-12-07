from typing import Optional, List, Dict, Any
from decimal import Decimal
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
import traceback
import pandas as pd

load_dotenv()

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


# get postgresql connection
def get_conn():
    supabase_pwd = os.getenv("SUPABASE_PWD")
    supabase_host = os.getenv("SUPABASE_HOST", "aws-1-us-east-1.pooler.supabase.com")
    supabase_port = os.getenv("SUPABASE_PORT", "5432")
    
    if not supabase_pwd:
        raise RuntimeError("SUPABASE_PWD environment variable not set")
    
    db_url = f"postgresql://postgres.wtiopnzppsyjxecrogik:{supabase_pwd}@{supabase_host}:{supabase_port}/postgres"
    return psycopg2.connect(db_url)


# execute query and return pandas dataframe
def query_to_df(sql: str, params: tuple = None) -> pd.DataFrame:
    conn = get_conn()
    try:
        df = pd.read_sql(sql, conn, params=params)
        return df
    finally:
        conn.close()


# execute query and return results as list of dicts
def query_db(sql: str, params: tuple = None) -> List[Dict[str, Any]]:
    conn = get_conn()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cursor.execute(sql, params)
        results = cursor.fetchall()
        return [dict(row) for row in results]
    finally:
        cursor.close()
        conn.close()


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
    """Convert DataFrame rows to JSON-safe records (no NaN/inf/Decimal)."""
    import numpy as np
    import math

    df = df.replace({pd.NA: None, pd.NaT: None, np.nan: None, np.inf: None, -np.inf: None})
    df = df.where(pd.notnull(df), None)

    records = df.to_dict(orient="records")

    def clean_value(value: Any):
        if value is None:
            return None
        if isinstance(value, Decimal):
            value = float(value)
        if isinstance(value, (float, int, np.floating)):
            return value if math.isfinite(float(value)) else None
        if isinstance(value, str) and value.lower() in {"nan", "inf", "-inf"}:
            return None
        return value

    for record in records:
        for key, value in list(record.items()):
            record[key] = clean_value(value)

    return records


def classify_company_industry(company_name: Any) -> str:
    """Classify a company into broad industry buckets based on name keywords."""
    name = str(company_name or "").lower()

    tech_giants = {
        "google", "alphabet", "microsoft", "meta", "facebook", "amazon", "apple",
        "netflix", "nvidia", "adobe", "salesforce", "oracle", "ibm", "tesla"
    }
    tech_mid = {
        "snowflake", "databricks", "palantir", "stripe", "block", "square", "twilio", "cloudflare",
        "shopify", "atlassian", "zendesk", "mongodb", "datadog", "okta", "servicenow",
        "airbnb", "uber", "lyft"
    }
    investment_banks = {
        "goldman", "morgan stanley", "jp morgan", "j.p. morgan", "bank of america", "bofa", "barclays",
        "credit suisse", "ubs", "deutsche bank", "jefferies", "evercore", "piper sandler", "lazard",
        "centerview", "moelis", "guggenheim", "rbc capital", "nomura", "mizuho", "hsbc", "citigroup",
        "citi", "bnpp", "bnp paribas", "wells fargo securities"
    }
    finance = {
        "jpmorgan", "chase", "capital one", "american express", "visa", "mastercard", "discover",
        "blackrock", "fidelity", "two sigma", "citadel", "point72", "aig", "state street",
        "pnc", "ally", "regions bank", "us bank", "charles schwab"
    }
    retail = {
        "walmart", "target", "costco", "home depot", "lowe's", "lowes", "best buy", "kroger",
        "walgreens", "cvs", "tesco", "aldi", "lidl", "ikea", "macy", "kohls", "nordstrom", "wayfair"
    }
    healthcare = {
        "johnson", "pfizer", "merck", "abbvie", "amgen", "novartis", "roche", "eli lilly",
        "bristol myers", "gsk", "sanofi", "astrazeneca", "unitedhealth", "cigna", "anthem", "elevance"
    }
    automotive = {"ford", "gm", "general motors", "toyota", "honda", "bmw", "mercedes", "volkswagen", "stellantis"}
    energy = {"chevron", "exxon", "exxonmobil", "shell", "bp", "total", "conocophillips", "duke energy"}

    def contains(keywords: set[str]) -> bool:
        return any(k in name for k in keywords)

    if contains(tech_giants):
        return "Tech - Giant"
    if contains(tech_mid):
        return "Tech - Mid"
    if contains(investment_banks):
        return "Investment Banking"
    if contains(finance):
        return "Finance"
    if contains(retail):
        return "Retail"
    if contains(healthcare):
        return "Healthcare / Pharma"
    if contains(automotive):
        return "Automotive / Manufacturing"
    if contains(energy):
        return "Energy / Utilities"
    if name and any(hint in name for hint in ["labs", "ventures", "ai", "analytics", "systems", "technologies", "solutions"]):
        return "Tech - Startup"
    return "Other"


def to_float(value: Any, default: float = 0.0) -> float:
    """Convert database numeric values (including Decimal) to float safely."""
    if value is None:
        return default
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


# basic stats like total jobs, unique companies, date range
@app.get("/api/overview")
def overview():
    sql = """
    SELECT
        COUNT(*)                           AS total_jobs,
        COUNT(DISTINCT company_name)       AS unique_companies,
        COUNT(DISTINCT location)           AS unique_locations,
        MIN(time_posted_parsed)            AS earliest_posting,
        MAX(time_posted_parsed)            AS latest_posting
    FROM "job-market-stream";
    """
    results = query_db(sql)
    if results:
        return results[0]
    return {}


# count jobs by function like data analyst, software engineer, etc
@app.get("/api/jobs_by_function")
def jobs_by_function(days: Optional[int] = None):
    if days is None:
        sql = """
        SELECT
            COALESCE(job_function, 'Unknown') AS job_function,
            COUNT(*) AS count
        FROM "job-market-stream"
        GROUP BY 1
        ORDER BY count DESC;
        """
        df = query_to_df(sql)
    else:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        sql = """
        SELECT
            COALESCE(job_function, 'Unknown') AS job_function,
            COUNT(*) AS count
        FROM "job-market-stream"
        WHERE time_posted_parsed >= %s
        GROUP BY 1
        ORDER BY count DESC;
        """
        df = query_to_df(sql, (cutoff,))
    return df_to_records(df)


# count jobs by work mode like remote, hybrid, onsite
@app.get("/api/work_mode")
def work_mode(days: Optional[int] = None):
    if days is None:
        sql = """
        SELECT
            COALESCE(work_mode, 'Unknown') AS work_mode,
            COUNT(*) AS count
        FROM "job-market-stream"
        GROUP BY 1
        ORDER BY count DESC;
        """
        df = query_to_df(sql)
    else:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        sql = """
        SELECT
            COALESCE(work_mode, 'Unknown') AS work_mode,
            COUNT(*) AS count
        FROM "job-market-stream"
        WHERE time_posted_parsed >= %s
        GROUP BY 1
        ORDER BY count DESC;
        """
        df = query_to_df(sql, (cutoff,))
    return df_to_records(df)


# top skills across all jobs, split by comma in the skills column
@app.get("/api/top_skills")
def top_skills(limit: int = 30, days: Optional[int] = None):
    base_query = """
        WITH exploded AS (
            SELECT
                TRIM(s) AS skill
            FROM "job-market-stream",
            LATERAL UNNEST(STRING_TO_ARRAY(skills, ',')) AS s
            WHERE skills IS NOT NULL AND skills <> ''
    """

    params: List[Any] = []

    if days is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        base_query += " AND time_posted_parsed >= %s "
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
        LIMIT %s;
    """
    params.append(limit)

    df = query_to_df(base_query, tuple(params))
    return df_to_records(df)


# job counts per day for time series charts
@app.get("/api/daily_counts")
def daily_counts(days: int = 180):
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    sql = """
    SELECT
        DATE(time_posted_parsed) AS day,
        COUNT(*) AS job_count
    FROM "job-market-stream"
    WHERE time_posted_parsed IS NOT NULL
      AND time_posted_parsed >= %s
    GROUP BY 1
    ORDER BY day;
    """
    df = query_to_df(sql, (cutoff,))
    df["day"] = df["day"].astype(str)
    return df_to_records(df)


# job counts per hour for recent activity
@app.get("/api/hourly_counts")
def hourly_counts(hours: int = 24):
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    sql = """
    SELECT
        DATE_TRUNC('hour', time_posted_parsed) AS hour,
        COUNT(*) AS job_count
    FROM "job-market-stream"
    WHERE time_posted_parsed IS NOT NULL
      AND time_posted_parsed >= %s
    GROUP BY 1
    ORDER BY hour;
    """
    df = query_to_df(sql, (cutoff,))
    df["hour"] = df["hour"].astype(str)
    return df_to_records(df)


# shared query for beeswarm and map visualizations
def _raw_beeswarm_query(limit: int, hours: int):
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    sql = """
    SELECT
      job_id,
      job_title,
      job_description AS summary,
      company_name,
            COALESCE(industry, '') AS "Industries",
      location,
      job_function AS "Job Function",
      skills AS skills_desired,
      degree_requirement AS degree_qualifications,
      visa_sponsorship,
      time_posted_parsed,
      application_link,
      application_link AS job_link,
      num_applicants_int AS num_applicants,
      work_mode,
      latitude,
      longitude
    FROM "job-market-stream"
    WHERE time_posted_parsed IS NOT NULL
      AND time_posted_parsed >= %s
    ORDER BY time_posted_parsed DESC
    LIMIT %s;
    """
    
    df = query_to_df(sql, (cutoff, limit))
    df["time_posted"] = df["time_posted_parsed"].apply(friendly_age)
    df["time_posted_parsed"] = df["time_posted_parsed"].astype(str)

    records = df_to_records(df)

    # Ensure industry is populated even if source data is blank
    for rec in records:
        company = rec.get("company_name")
        inferred = classify_company_industry(company)
        existing = rec.get("Industries") or rec.get("industry")
        rec["Industries"] = existing or inferred
        rec["industry"] = rec["Industries"]

    return records


# jobs for beeswarm chart
@app.get("/api/beeswarm_jobs")
def beeswarm_jobs(
    limit: int = Query(2000, ge=1, le=5000),
    hours: int = Query(24, ge=1, le=24),
):
    return _raw_beeswarm_query(limit=limit, hours=hours)


# jobs for map visualization, same data as beeswarm
@app.get("/api/map_jobs")
def map_jobs_alias(
    limit: int = Query(2000, ge=1, le=5000),
    hours: int = Query(24, ge=1, le=24),
):
    return _raw_beeswarm_query(limit=limit, hours=hours)


# heatmap of average applicants by day of week and hour
@app.get("/api/competition_heatmap")
def competition_heatmap(days: int = 30):
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    sql = """
    SELECT 
        EXTRACT(DOW FROM time_posted_parsed)::INT AS day_of_week,
        EXTRACT(HOUR FROM time_posted_parsed)::INT AS hour,
        AVG(COALESCE(num_applicants_int, 0)) AS avg_applicants,
        COUNT(*) AS job_count
    FROM "job-market-stream"
    WHERE time_posted_parsed >= %s
      AND time_posted_parsed IS NOT NULL
    GROUP BY 1, 2
    ORDER BY day_of_week, hour;
    """
    df = query_to_df(sql, (cutoff,))
    return df_to_records(df)


# skill nodes for network graph visualization
@app.get("/api/skills_network")
def skills_network(limit: int = 50, days: int = 30):
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    sql = """
    WITH exploded AS (
        SELECT job_id, TRIM(s) AS skill
        FROM "job-market-stream",
        LATERAL UNNEST(STRING_TO_ARRAY(skills, ',')) AS s
        WHERE skills IS NOT NULL
          AND skills <> ''
          AND time_posted_parsed >= %s
    )
    SELECT
        skill,
        COUNT(DISTINCT job_id) AS frequency
    FROM exploded
    WHERE skill <> ''
    GROUP BY skill
    ORDER BY frequency DESC
    LIMIT %s;
    """
    
    df = query_to_df(sql, (cutoff, limit))

    nodes = [
        {"id": row["skill"], "label": row["skill"], "size": int(row["frequency"])}
        for _, row in df.iterrows()
    ]

    return {"nodes": nodes, "edges": []}


# top companies and how fast they post jobs over time
@app.get("/api/company_velocity")
def company_velocity(days: int = 30, top_n: int = 20):
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    sql = """
    WITH company_daily AS (
        SELECT
            company_name,
            DATE_TRUNC('day', time_posted_parsed) AS day,
            COUNT(*) AS daily_posts
        FROM "job-market-stream"
        WHERE time_posted_parsed >= %s
          AND company_name IS NOT NULL
        GROUP BY company_name, day
    ),
    company_totals AS (
        SELECT company_name, SUM(daily_posts) AS total_posts
        FROM company_daily
        GROUP BY company_name
        ORDER BY total_posts DESC
        LIMIT %s
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
    """
    df = query_to_df(sql, (cutoff, top_n))
    df["day"] = df["day"].astype(str)
    return df_to_records(df)


# group jobs by age buckets like new, fresh, stale
@app.get("/api/job_lifecycle")
def job_lifecycle():
    sql = """
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
        FROM "job-market-stream"
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
    df = query_to_df(sql)
    return df_to_records(df)


# compare skill mentions in recent vs older time window
@app.get("/api/trending_skills")
def trending_skills(days_back: int = 30, top_n: int = 20):
    mid_date = datetime.now(timezone.utc) - timedelta(days=days_back // 2)
    start_date = datetime.now(timezone.utc) - timedelta(days=days_back)

    sql = """
    WITH skill_periods AS (
        SELECT
            TRIM(s) AS skill,
            CASE WHEN time_posted_parsed >= %s THEN 'recent' ELSE 'older' END AS period,
            COUNT(*) AS mentions
        FROM "job-market-stream",
        LATERAL UNNEST(STRING_TO_ARRAY(skills, ',')) AS s
        WHERE skills IS NOT NULL
          AND skills <> ''
          AND time_posted_parsed >= %s
          AND TRIM(s) <> ''
        GROUP BY TRIM(s), period
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
    ORDER BY CASE
        WHEN older_mentions = 0 THEN 100 
        ELSE ABS((recent_mentions - older_mentions) * 100.0 / older_mentions)
    END DESC
    LIMIT %s;
    """
    df = query_to_df(sql, (mid_date, start_date, top_n))
    return df_to_records(df)


# work mode percentages per week over time
@app.get("/api/remote_evolution")
def remote_evolution(days: int = 180):
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    sql = """
    WITH weekly AS (
        SELECT
            DATE_TRUNC('week', time_posted_parsed) AS week,
            COALESCE(work_mode, 'Unknown') AS work_mode,
            COUNT(*) AS cnt
        FROM "job-market-stream"
        WHERE time_posted_parsed IS NOT NULL
          AND time_posted_parsed >= %s
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
    JOIN totals t ON w.week = t.week
    ORDER BY w.week, w.work_mode;
    """
    df = query_to_df(sql, (cutoff,))
    df["week"] = df["week"].astype(str)
    return df_to_records(df)


# count culture keywords in job descriptions
@app.get("/api/culture_keywords")
def culture_keywords(limit: int = 20):
    keywords = [
        "inclusive", "diverse", "collaborative", "remote",
        "flexible", "supportive", "growth", "learning",
        "ownership", "mentorship", "autonomy",
        "work-life balance", "transparent", "mission-driven",
        "innovative", "fast-paced", "team-first",
        "customer obsessed", "impact", "hybrid"
    ]

    sql = "SELECT job_description FROM \"job-market-stream\" WHERE job_description IS NOT NULL;"
    df = query_to_df(sql)

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
    now = datetime.now(timezone.utc)
    last_hour = now - timedelta(hours=1)
    last_24h = now - timedelta(hours=24)
    last_week = now - timedelta(days=7)

    sql = """
    SELECT 
        COUNT(CASE WHEN time_posted_parsed >= %s THEN 1 END) AS last_hour_jobs,
        COUNT(CASE WHEN time_posted_parsed >= %s THEN 1 END) AS last_24h_jobs,
        COUNT(CASE WHEN time_posted_parsed >= %s THEN 1 END) / (7.0 * 24) AS weekly_avg_per_hour,
        MODE() WITHIN GROUP (
            ORDER BY CASE WHEN time_posted_parsed >= %s THEN location END
        ) AS hottest_location,
        MODE() WITHIN GROUP (
            ORDER BY CASE WHEN time_posted_parsed >= %s THEN job_function END
        ) AS hottest_function,
        MAX(CASE WHEN time_posted_parsed >= %s THEN num_applicants_int END) AS max_applicants_recent,
        AVG(CASE WHEN time_posted_parsed >= %s THEN num_applicants_int END) AS avg_applicants_recent
    FROM "job-market-stream"
    WHERE time_posted_parsed IS NOT NULL;
    """

    results = query_db(sql, (last_hour, last_24h, last_week, last_24h, last_24h, last_24h, last_24h))
    
    if not results:
        return {}
    
    row = results[0]

    # Convert all numeric values to float/int to handle Decimal types from PostgreSQL
    last_hour_jobs = to_float(row.get("last_hour_jobs"))
    last_24h_jobs = to_float(row.get("last_24h_jobs"))
    weekly_avg = to_float(row.get("weekly_avg_per_hour"))
    hottest_location = row.get("hottest_location") or "Unknown"
    hottest_function = row.get("hottest_function") or "Unknown"
    max_applicants_recent = to_float(row.get("max_applicants_recent"))
    avg_applicants_recent = to_float(row.get("avg_applicants_recent"))

    hour_change = (
        ((last_hour_jobs - weekly_avg) / weekly_avg * 100.0) if weekly_avg > 0 else 0.0
    )

    return {
        "last_hour": {
            "job_count": int(last_hour_jobs),
            "vs_weekly_avg": round(hour_change, 1),
            "trend": "up" if hour_change > 0 else "down" if hour_change < 0 else "stable",
        },
        "last_24h": {
            "job_count": int(last_24h_jobs),
            "hottest_location": hottest_location,
            "hottest_function": hottest_function,
            "max_applicants": int(max_applicants_recent),
            "avg_applicants": round(avg_applicants_recent, 1),
        },
    }


# get unique degree requirements
@app.get("/api/degree")
def get_degrees():
    sql = """
    SELECT DISTINCT COALESCE(degree_requirement, 'Unknown') AS degree_requirement
    FROM "job-market-stream"
    WHERE degree_requirement IS NOT NULL
    ORDER BY degree_requirement;
    """
    df = query_to_df(sql)
    return df_to_records(df)


# get unique job functions
@app.get("/api/valid_functions")
def get_valid_functions():
    sql = """
    SELECT DISTINCT COALESCE(job_function, 'Unknown') AS job_function
    FROM "job-market-stream"
    WHERE job_function IS NOT NULL
    ORDER BY job_function;
    """
    df = query_to_df(sql)
    return df_to_records(df)


# get top job titles
@app.get("/api/top_titles")
def top_titles(limit: int = 30, days: Optional[int] = None):
    if days is None:
        sql = """
        SELECT 
            COALESCE(job_title, 'Unknown') AS job_title,
            COUNT(*) AS count
        FROM "job-market-stream"
        GROUP BY 1
        ORDER BY count DESC
        LIMIT %s;
        """
        df = query_to_df(sql, (limit,))
    else:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        sql = """
        SELECT 
            COALESCE(job_title, 'Unknown') AS job_title,
            COUNT(*) AS count
        FROM "job-market-stream"
        WHERE time_posted_parsed >= %s
        GROUP BY 1
        ORDER BY count DESC
        LIMIT %s;
        """
        df = query_to_df(sql, (cutoff, limit))
    return df_to_records(df)


# get top companies
@app.get("/api/top_companies")
def top_companies(limit: int = 30, days: Optional[int] = None):
    if days is None:
        sql = """
        SELECT 
            COALESCE(company_name, 'Unknown') AS company_name,
            COUNT(*) AS count
        FROM "job-market-stream"
        GROUP BY 1
        ORDER BY count DESC
        LIMIT %s;
        """
        df = query_to_df(sql, (limit,))
    else:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        sql = """
        SELECT 
            COALESCE(company_name, 'Unknown') AS company_name,
            COUNT(*) AS count
        FROM "job-market-stream"
        WHERE time_posted_parsed >= %s
        GROUP BY 1
        ORDER BY count DESC
        LIMIT %s;
        """
        df = query_to_df(sql, (cutoff, limit))
    return df_to_records(df)


# get unique locations
@app.get("/api/locations")
def get_locations():
    sql = """
    SELECT DISTINCT COALESCE(location, 'Unknown') AS location
    FROM "job-market-stream"
    WHERE location IS NOT NULL
    ORDER BY location;
    """
    df = query_to_df(sql)
    return df_to_records(df)


# search jobs by title/description/company
@app.get("/api/search")
def search_jobs(q: str = "", limit: int = 50):
    sql = """
    SELECT
        job_id,
        job_title,
        job_description AS summary,
        company_name,
        location,
        job_function AS "Job Function",
        skills AS skills_desired,
        time_posted_parsed,
        application_link,
        num_applicants_int AS num_applicants,
        work_mode
    FROM "job-market-stream"
    WHERE (
        job_title ILIKE %s OR
        company_name ILIKE %s OR
        job_description ILIKE %s OR
        job_function ILIKE %s
    )
    LIMIT %s;
    """
    search_term = f"%{q}%"
    df = query_to_df(sql, (search_term, search_term, search_term, search_term, limit))
    return df_to_records(df)


# get time range of data
@app.get("/api/time_range")
def get_time_range():
    sql = """
    SELECT
        MIN(time_posted_parsed) AS min_date,
        MAX(time_posted_parsed) AS max_date
    FROM "job-market-stream"
    WHERE time_posted_parsed IS NOT NULL;
    """
    results = query_db(sql)
    if results:
        row = results[0]
        return {
            "min_date": str(row.get("min_date") or ""),
            "max_date": str(row.get("max_date") or ""),
        }
    return {"min_date": "", "max_date": ""}


# beeswarm data (alias for compatibility)
@app.get("/api/beeswarm")
def beeswarm_compat(
    limit: int = Query(2000, ge=1, le=5000),
    hours: int = Query(24, ge=1, le=24 * 7),
):
    return _raw_beeswarm_query(limit=limit, hours=hours)


# pulse endpoint (alias)
@app.get("/api/pulse")
def pulse_compat():
    return pulse_metrics()


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
            "/api/beeswarm",
            "/api/map_jobs",
            "/api/competition_heatmap",
            "/api/skills_network",
            "/api/company_velocity",
            "/api/job_lifecycle",
            "/api/trending_skills",
            "/api/remote_evolution",
            "/api/culture_keywords",
            "/api/pulse_metrics",
            "/api/pulse",
            "/api/degree",
            "/api/valid_functions",
            "/api/top_titles",
            "/api/top_companies",
            "/api/locations",
            "/api/search",
            "/api/time_range",
        ],
    }
