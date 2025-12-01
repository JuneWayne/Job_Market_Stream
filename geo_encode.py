# geocode_locations_to_duckdb.py

import time
from pathlib import Path

import duckdb
import pandas as pd
import requests

DB_PATH = Path("data/jobs.duckdb")

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
USER_AGENT = "JobMarketStream/1.0 (contact: you@example.com)"  # put your email/domain


def get_conn():
    return duckdb.connect(str(DB_PATH))


def geocode(location_name: str):
    """Call Nominatim and return (lat, lon) or (None, None)."""
    params = {
        "q": f"{location_name}, United States",
        "format": "json",
        "limit": 1,
    }
    resp = requests.get(
        NOMINATIM_URL,
        params=params,
        headers={"User-Agent": USER_AGENT},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    if data:
        return float(data[0]["lat"]), float(data[0]["lon"])
    return None, None


def update_geo_locations():
    """
    Idempotent: looks at jobs.location, geocodes any NEW locations
    not already in geo_locations, and inserts them.

    Safe to call at the end of each DuckDB refresh.
    """
    con = get_conn()

    # 1) Ensure table exists
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS geo_locations (
            location   VARCHAR PRIMARY KEY,
            latitude   DOUBLE,
            longitude  DOUBLE
        );
        """
    )

    # 2) Distinct locations from jobs
    jobs_locs = (
        con.execute(
            "SELECT DISTINCT location FROM jobs WHERE location IS NOT NULL;"
        )
        .fetchdf()["location"]
        .tolist()
    )

    # 3) Already-geocoded locations to skip
    existing_df = con.execute("SELECT location FROM geo_locations;").fetchdf()
    existing = set(existing_df["location"].tolist())

    to_geocode = [loc for loc in jobs_locs if loc not in existing]
    print(f"[geo] Total distinct locations: {len(jobs_locs)}")
    print(f"[geo] Already geocoded:        {len(existing)}")
    print(f"[geo] Need to geocode:         {len(to_geocode)}")

    rows = []
    for loc in to_geocode:
        print(f"[geo] Geocoding: {loc} ...")
        try:
            lat, lon = geocode(loc)
        except Exception as e:
            print(f"[geo]  Error for {loc}: {e}")
            continue

        # Respect Nominatim rate limits
        time.sleep(1)

        if lat is not None and lon is not None:
            rows.append((loc, lat, lon))
            print(f"[geo]   -> {lat}, {lon}")
        else:
            print(f"[geo]   Could not geocode: {loc}")

    if rows:
        df = pd.DataFrame(rows, columns=["location", "latitude", "longitude"])
        con.execute("INSERT INTO geo_locations SELECT * FROM df")
        print(f"[geo] Inserted {len(rows)} new rows into geo_locations.")
    else:
        print("[geo] No new locations to insert.")

    con.close()


if __name__ == "__main__":
    # optional: allow this to be run manually, too
    update_geo_locations()
