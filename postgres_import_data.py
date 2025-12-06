import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv

load_dotenv()

# build connection string from environment
supabase_pwd = os.getenv("SUPABASE_PWD")
supabase_host = os.getenv("SUPABASE_HOST", "aws-1-us-east-1.pooler.supabase.com")
supabase_port = os.getenv("SUPABASE_PORT", "5432")

if not supabase_pwd:
    raise ValueError("SUPABASE_PWD not set in .env")

db_url = f"postgresql://postgres.wtiopnzppsyjxecrogik:{supabase_pwd}@{supabase_host}:{supabase_port}/postgres"

# connect to supabase postgresql
try:
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()
    print("Connected to Supabase PostgreSQL")
except Exception as e:
    print(f"Failed to connect: {e}")
    exit(1)

# load csv data
try:
    df = pd.read_csv("data/parsed_jobs.csv")
    print(f"Loaded {len(df)} rows from CSV")
except Exception as e:
    print(f"Failed to load CSV: {e}")
    exit(1)

# create jobs table if it doesn't exist
create_table_sql = """
CREATE TABLE IF NOT EXISTS "job-market-stream" (
    job_id TEXT PRIMARY KEY,
    job_title TEXT,
    job_description TEXT,
    company_name TEXT,
    location TEXT,
    job_function TEXT,
    skills TEXT,
    degree_requirement TEXT,
    time_posted_parsed TIMESTAMP WITH TIME ZONE,
    application_link TEXT,
    num_applicants_int INTEGER,
    work_mode TEXT,
    scraped_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

try:
    cursor.execute(create_table_sql)
    conn.commit()
    print("Created jobs table")
except Exception as e:
    print(f"Failed to create table: {e}")
    conn.close()
    exit(1)

# prepare data for insertion
records = []
for _, row in df.iterrows():
    record = (
        str(row.get("job_id", "")),
        str(row.get("job_title", "")),
        str(row.get("job_description", "")),
        str(row.get("company_name", "")),
        str(row.get("location", "")),
        str(row.get("job_function", "")),
        str(row.get("skills", "")),
        str(row.get("degree_requirement", "")),
        pd.to_datetime(row.get("time_posted_parsed")) if pd.notna(row.get("time_posted_parsed")) else None,
        str(row.get("application_link", "")),
        int(row.get("num_applicants_int")) if pd.notna(row.get("num_applicants_int")) else None,
        str(row.get("work_mode", "")),
        pd.to_datetime(row.get("scraped_at")) if pd.notna(row.get("scraped_at")) else None,
    )
    records.append(record)

# insert data using execute_values for efficiency
insert_sql = """
INSERT INTO "job-market-stream" (job_id, job_title, job_description, company_name, location, job_function, 
                 skills, degree_requirement, time_posted_parsed, application_link, 
                 num_applicants_int, work_mode, scraped_at)
VALUES %s
ON CONFLICT (job_id) DO NOTHING;
"""

try:
    execute_values(cursor, insert_sql, records, page_size=1000)
    conn.commit()
    print(f"Inserted {len(records)} job records into PostgreSQL")
except Exception as e:
    print(f"Failed to insert data: {e}")
    conn.close()
    exit(1)

# create index on job_id for faster queries
try:
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_job_id ON \"job-market-stream\"(job_id);")
    conn.commit()
    print("Created index on job_id")
except Exception as e:
    print(f"Failed to create index: {e}")

# close connection
cursor.close()
conn.close()
print("Import complete")
