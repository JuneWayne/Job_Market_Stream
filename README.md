# Job Market Stream

Job Market Stream is a **live-ish job market analytics pipeline** that scrapes postings, streams them through a lightweight Kafka-compatible broker, lands them in DuckDB, and serves a fully static D3.js dashboard (hosted on GitHub Pages) for interactive exploration.

The goal is to treat the job market—especially **data science / analytics internships**—as a live time series: you can see **how many jobs appear per day**, what functions are trending, how work modes are distributed, and where the roles are **geographically concentrated** on a map and in a beeswarm visualization.

---

## Project Overview

At a high level, this project does three things:

1. **Continuously collect job postings**  
   - A Python scraper pulls new jobs from Indeed (and potentially other sources).
   - A Kafka-compatible broker (Redpanda) acts as the message bus.
   - A producer pushes scraped jobs into a `job_postings` topic.
   - A consumer subscribes to this topic, cleans / normalizes the payloads, and writes them to a CSV staging file.

2. **Build an analytics store with DuckDB + FastAPI**  
   - A dedicated `duckdb_ingestion.py` job reads the staging CSV (`data/parsed_jobs.csv`), creates a **deduplicated** `jobs` table, and refreshes a `jobs_sorted` view/table.
   - A separate `geo_encode.py` job geocodes distinct locations into a `geo_locations` table using OpenStreetMap’s Nominatim service.
   - A FastAPI service (`fast_api_analytics.py`) exposes JSON endpoints (overview stats, daily counts, job function distributions, work mode, beeswarm + map jobs, 24-hour hourly trend, etc.) from DuckDB.

3. **Render a live dashboard in a single static HTML file**  
   - The dashboard (`index.html`) is a static site using **D3.js**, **Leaflet**, and **MarkerCluster**,
     consuming only FastAPI JSON.
   - It displays:
     - Overall counts and unique companies/locations.
     - A **180-day daily job postings** line chart.
     - Job function breakdown (all time + last 7 days).
     - Work mode distribution (remote vs. hybrid vs. on-site).
     - A **24-hour bubble plot** of new jobs per hour.
     - A map view of locations with cluster bubbles.
     - A **beeswarm view** where each bubble is a job, grouped by function, industry, company bucket, degree, skills, or time posted.

Everything is containerized with Docker Compose so the core services (Redpanda, producer, consumer, DuckDB refresher, FastAPI API) can be spun up together.

---

## Architecture & Data Flow

### High-Level Pipeline

```mermaid
flowchart LR
    A[Scraper\n(Indeed / others)] --> B[Kafka Producer\nproducer.py]
    B --> C[(Redpanda\nKafka broker)]
    C --> D[Kafka Consumer\nconsumer.py]
    D --> E[Staging CSV\n data/parsed_jobs.csv]
    E --> F[DuckDB Ingestion\n duckdb_ingestion.py]
    F --> G[(DuckDB\n data/jobs.duckdb)]
    G --> H[Geo Encoding\n geo_encode.py]
    G --> I[FastAPI Service\n fast_api_analytics.py]
    I --> J[Static Dashboard\n index.html + D3.js + Leaflet]
