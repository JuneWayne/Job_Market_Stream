from typing import Optional, List, Dict, Any
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import psycopg2
import psycopg2.extras
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
import traceback

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


# get postgresql connection from environment
def get_db_connection():
    supabase_pwd = os.getenv("SUPABASE_PWD")
    supabase_host = os.getenv("SUPABASE_HOST", "aws-1-us-east-1.pooler.supabase.com")
    supabase_port = os.getenv("SUPABASE_PORT", "5432")
    
    if not supabase_pwd:
        raise RuntimeError("SUPABASE_PWD environment variable not set")
    
    db_url = f"postgresql://postgres.wtiopnzppsyjxecrogik:{supabase_pwd}@{supabase_host}:{supabase_port}/postgres"
    return psycopg2.connect(db_url)


# execute query and return results as list of dicts
def query_db(sql: str, params: tuple = None) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cursor.execute(sql, params)
        results = cursor.fetchall()
        return [dict(row) for row in results]
    finally:
        cursor.close()
        conn.close()


# count jobs by function like data analyst, software engineer, etc
@app.get("/api/jobs_by_function")
async def jobs_by_function():
    sql = """
    SELECT job_function, COUNT(*) as count
    FROM "job-market-stream"
    WHERE function IS NOT NULL
    GROUP BY function
    ORDER BY count DESC;
    """
    results = query_db(sql)
    return {"functions": results}


# get jobs grouped by work mode like remote, hybrid, on-site
@app.get("/api/work_mode")
async def work_mode():
    sql = """
    SELECT work_mode, COUNT(*) as count
    FROM "job-market-stream"
    WHERE work_mode IS NOT NULL
    GROUP BY work_mode
    ORDER BY count DESC;
    """
    results = query_db(sql)
    return {"work_modes": results}


# get top skills from job descriptions
@app.get("/api/top_skills")
async def top_skills(limit: int = 20):
    sql = """
    SELECT skill, COUNT(*) as count
    FROM (
        SELECT TRIM(UNNEST(STRING_TO_ARRAY(skills, ','))) as skill
        FROM "job-market-stream"
        WHERE skills IS NOT NULL AND skills <> ''
    ) t
    WHERE skill <> ''
    GROUP BY skill
    ORDER BY count DESC
    LIMIT %s;
    """
    results = query_db(sql, (limit,))
    return {"skills": results}


# count jobs posted each day
@app.get("/api/daily_counts")
async def daily_counts():
    sql = """
    SELECT DATE(time_posted_parsed) as day, COUNT(*) as count
    FROM "job-market-stream"
    WHERE time_posted_parsed IS NOT NULL
    GROUP BY DATE(time_posted_parsed)
    ORDER BY day ASC;
    """
    results = query_db(sql)
    return {"daily_counts": results}


# get jobs grouped by required degree like bachelors, masters, phd, etc
@app.get("/api/degree")
async def degree():
    sql = """
    SELECT degree_requirement, COUNT(*) as count
    FROM "job-market-stream"
    WHERE degree IS NOT NULL
    GROUP BY degree
    ORDER BY count DESC;
    """
    results = query_db(sql)
    return {"degrees": results}


# get top job titles
@app.get("/api/top_titles")
async def top_titles(limit: int = 20):
    sql = """
    SELECT job_title, COUNT(*) as count
    FROM "job-market-stream"
    WHERE title IS NOT NULL
    GROUP BY title
    ORDER BY count DESC
    LIMIT %s;
    """
    results = query_db(sql, (limit,))
    return {"titles": results}


# get top companies hiring
@app.get("/api/top_companies")
async def top_companies(limit: int = 20):
    sql = """
    SELECT company_name, COUNT(*) as count
    FROM "job-market-stream"
    WHERE company IS NOT NULL
    GROUP BY company
    ORDER BY count DESC
    LIMIT %s;
    """
    results = query_db(sql, (limit,))
    return {"companies": results}


# get jobs grouped by location
@app.get("/api/locations")
async def locations(limit: int = 20):
    sql = """
    SELECT location, COUNT(*) as count
    FROM "job-market-stream"
    WHERE location IS NOT NULL
    GROUP BY location
    ORDER BY count DESC
    LIMIT %s;
    """
    results = query_db(sql, (limit,))
    return {"locations": results}


# search jobs by keyword
@app.get("/api/search")
async def search(q: str, limit: int = 50):
    sql = """
    SELECT * FROM "job-market-stream"
    WHERE title ILIKE %s
       OR company ILIKE %s
       OR job_description ILIKE %s
       OR skills ILIKE %s
    LIMIT %s;
    """
    search_term = f"%{q}%"
    results = query_db(sql, (search_term, search_term, search_term, search_term, limit))
    return {"results": results}


# get jobs with salary ranges for beeswarm chart
@app.get("/api/beeswarm")
async def beeswarm(function: Optional[str] = None, limit: int = 500):
    if function:
        sql = """
        SELECT job_title, company, function, salary_min, salary_max, location
        FROM "job-market-stream"
        WHERE function = %s
        LIMIT %s;
        """
        results = query_db(sql, (function, limit))
    else:
        sql = """
        SELECT job_title, company, function, salary_min, salary_max, location
        FROM "job-market-stream"
        LIMIT %s;
        """
        results = query_db(sql, (limit,))
    
    return {"jobs": results}


# get list of valid job functions for filtering
@app.get("/api/valid_functions")
async def valid_functions():
    sql = """
    SELECT DISTINCT function
    FROM "job-market-stream"
    WHERE function IS NOT NULL
    ORDER BY function ASC;
    """
    results = query_db(sql)
    functions = [r["function"] for r in results]
    return {"functions": functions}


# get pulse metrics on job market health
@app.get("/api/pulse")
async def pulse(days: int = 7):
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
    sql = """
    SELECT 
        COUNT(*) as total_jobs,
        COUNT(DISTINCT company) as unique_companies,
        COUNT(DISTINCT function) as unique_functions
    FROM "job-market-stream"
    WHERE time_posted_parsed >= %s;
    """
    results = query_db(sql, (cutoff_date,))
    if results:
        return results[0]
    return {"total_jobs": 0, "unique_companies": 0, "unique_functions": 0}


# get time range of data in the database
@app.get("/api/time_range")
async def time_range():
    sql = """
    SELECT 
        MIN(time_posted_parsed) as min_date,
        MAX(time_posted_parsed) as max_date
    FROM "job-market-stream";
    """
    results = query_db(sql)
    if results:
        return results[0]
    return {"min_date": None, "max_date": None}


# count jobs posted each hour for hourly trends
@app.get("/api/hourly_counts")
async def hourly_counts(days: int = 1):
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
    sql = """
    SELECT 
        DATE_TRUNC('hour', time_posted_parsed) as hour,
        COUNT(*) as count
    FROM "job-market-stream"
    WHERE time_posted_parsed >= %s
    GROUP BY DATE_TRUNC('hour', time_posted_parsed)
    ORDER BY hour ASC;
    """
    results = query_db(sql, (cutoff_date,))
    return {"hourly_counts": results}
