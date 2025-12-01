# geocode_locations_to_duckdb.py

import time
from pathlib import Path

import duckdb
import pandas as pd
import requests

DB_PATH = Path("data/jobs.duckdb")


def get_conn():
    return duckdb.connect(str(DB_PATH))


def geocode(location_name: str):
    """
    Call Nominatim and return (lat, lon) or (None, None),
    using the same style as your original script.
    """
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": f"{location_name}, United States",
        "format": "json",
        "limit": 1,
    }
    response = requests.get(url, params=params, headers={"User-Agent": "USMapBot/1.0"})
    data = response.json()
    if data:
        return float(data[0]["lat"]), float(data[0]["lon"])
    return None, None


def update_geo_locations():
    """
    Looks at jobs.location in DuckDB, geocodes any NEW locations
    not already in geo_locations, and inserts them.

    Safe to call at the end of each DuckDB refresh.
    """
    con = get_conn()

    # 1) Ensure geo_locations exists
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
    jobs_df = con.execute(
        "SELECT DISTINCT location FROM jobs WHERE location IS NOT NULL;"
    ).fetchdf()
    jobs_locs = jobs_df["location"].tolist()

    # 3) Already-geocoded locations
    existing_df = con.execute("SELECT location FROM geo_locations;").fetchdf()
    existing = set(existing_df["location"].tolist()) if not existing_df.empty else set()

    to_geocode = [loc for loc in jobs_locs if loc not in existing]

    print(f"[geo] Total distinct locations: {len(jobs_locs)}")
    print(f"[geo] Already geocoded:        {len(existing)}")
    print(f"[geo] Need to geocode:         {len(to_geocode)}")

    rows = []
    for loc_name in to_geocode:
        print(f"[geo] Geocoding {loc_name}...")
        lat, lon = geocode(loc_name)
        time.sleep(1)  # be nice to Nominatim, like in your original script

        if lat is not None and lon is not None:
            geo_doc = {
                "location": loc_name,
                "latitude": lat,
                "longitude": lon,
            }
            rows.append(geo_doc)
            print(f"[geo]   -> {lat}, {lon}")
        else:
            print(f"[geo]   Could not geocode: {loc_name}")

    if rows:
        df = pd.DataFrame(rows, columns=["location", "latitude", "longitude"])
        # Insert all new rows into DuckDB
        con.execute("INSERT INTO geo_locations SELECT * FROM df")
        print(f"[geo] Inserted {len(rows)} geo rows into geo_locations.")
    else:
        print("[geo] No new geo data to insert.")

    con.close()


if __name__ == "__main__":
    update_geo_locations()
