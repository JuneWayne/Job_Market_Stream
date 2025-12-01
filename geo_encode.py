import time
from pathlib import Path

import duckdb
import pandas as pd
import requests

DB_PATH = Path("data/jobs.duckdb")
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"


def get_conn():
    return duckdb.connect(str(DB_PATH))


def geocode(location_name: str):
    """
    Use the Nominatim API to look up latitude and longitude
    for a given location name
    """
    params = {
        "q": f"{location_name}, United States",
        "format": "json",
        "limit": 1,
    }

    resp = requests.get(
        NOMINATIM_URL,
        params=params,
        headers={"User-Agent": "USMapBot/1.0"},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()

    if data:
        return float(data[0]["lat"]), float(data[0]["lon"])
    return None, None


def update_geo_locations():
    """
    Create or update the geo_locations table in DuckDB.
    """
    con = get_conn()

    # Make sure the geo_locations table exists
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS geo_locations (
            location   VARCHAR PRIMARY KEY,
            latitude   DOUBLE,
            longitude  DOUBLE
        );
        """
    )

    # Get all unique locations from the jobs table
    jobs_df = con.execute(
        "SELECT DISTINCT location FROM jobs WHERE location IS NOT NULL;"
    ).fetchdf()
    job_locations = jobs_df["location"].tolist()

    # Get locations that already have coordinates
    existing_df = con.execute("SELECT location FROM geo_locations;").fetchdf()
    existing = set(existing_df["location"].tolist()) if not existing_df.empty else set()

    # Only geocode locations that are not already stored
    to_geocode = [loc for loc in job_locations if loc not in existing]

    print(f"[geo] Total distinct locations in jobs: {len(job_locations)}")
    print(f"[geo] Already in geo_locations:        {len(existing)}")
    print(f"[geo] Need to geocode:                 {len(to_geocode)}")

    rows = []
    for loc in to_geocode:
        print(f"[geo] Geocoding {loc}...")
        try:
            lat, lon = geocode(loc)
        except Exception as e:
            print(f"[geo]  Error for {loc}: {e}")
            continue

        # Pause between requests to avoid rate limitation
        time.sleep(1)

        if lat is not None and lon is not None:
            print(f"[geo]   Result: {lat}, {lon}")
            rows.append((loc, lat, lon))
        else:
            print(f"[geo]   No coordinates found for: {loc}")

    # Insert new coordinates into the geo_locations table
    if rows:
        df = pd.DataFrame(rows, columns=["location", "latitude", "longitude"])
        con.execute("INSERT INTO geo_locations SELECT * FROM df")
        print(f"[geo] Inserted {len(rows)} new rows into geo_locations.")
    else:
        print("[geo] No new locations to insert.")

    con.close()


if __name__ == "__main__":
    update_geo_locations()
