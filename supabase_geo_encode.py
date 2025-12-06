"""
Geocode job locations and update Supabase PostgreSQL database with latitude/longitude.
Uses OpenStreetMap Nominatim API with rate limiting.
"""
import time
import os
import psycopg2
from dotenv import load_dotenv
import requests

load_dotenv()

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"


def get_conn():
    """Get PostgreSQL connection to Supabase."""
    supabase_pwd = os.getenv("SUPABASE_PWD")
    supabase_host = os.getenv("SUPABASE_HOST", "aws-1-us-east-1.pooler.supabase.com")
    supabase_port = os.getenv("SUPABASE_PORT", "5432")
    
    if not supabase_pwd:
        raise RuntimeError("SUPABASE_PWD environment variable not set")
    
    db_url = f"postgresql://postgres.wtiopnzppsyjxecrogik:{supabase_pwd}@{supabase_host}:{supabase_port}/postgres"
    return psycopg2.connect(db_url)


def geocode(location_name: str):
    """
    Use the Nominatim API to look up latitude and longitude
    for a given location name.
    """
    params = {
        "q": f"{location_name}, United States",
        "format": "json",
        "limit": 1,
    }

    try:
        resp = requests.get(
            NOMINATIM_URL,
            params=params,
            headers={"User-Agent": "JobMarketStreamBot/1.0 (Educational Project)"},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        if data and len(data) > 0:
            return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception as e:
        print(f"[geo] Error geocoding {location_name}: {e}")
    
    return None, None


def update_geo_locations():
    """
    Update the job-market-stream table with latitude/longitude for locations
    that don't have coordinates yet.
    """
    conn = get_conn()
    cursor = conn.cursor()

    try:
        # Get distinct locations that need geocoding (where lat/lon is NULL)
        cursor.execute("""
            SELECT DISTINCT location 
            FROM "job-market-stream" 
            WHERE location IS NOT NULL 
              AND location != '' 
              AND (latitude IS NULL OR longitude IS NULL)
            LIMIT 100;
        """)
        
        locations_to_geocode = [row[0] for row in cursor.fetchall()]
        
        print(f"[geo] Found {len(locations_to_geocode)} locations to geocode")

        if not locations_to_geocode:
            print("[geo] All locations already have coordinates!")
            return

        geocoded_count = 0
        for location in locations_to_geocode:
            print(f"[geo] Geocoding: {location}...")
            
            lat, lon = geocode(location)
            
            if lat is not None and lon is not None:
                print(f"[geo]   Found: {lat}, {lon}")
                
                # Update all jobs with this location
                cursor.execute("""
                    UPDATE "job-market-stream"
                    SET latitude = %s, longitude = %s
                    WHERE location = %s
                      AND (latitude IS NULL OR longitude IS NULL);
                """, (lat, lon, location))
                
                geocoded_count += 1
                print(f"[geo]   Updated jobs with location '{location}'")
            else:
                print(f"[geo]   No coordinates found for: {location}")
            
            # Rate limiting: 1 request per second to respect Nominatim usage policy
            time.sleep(1.1)

        conn.commit()
        print(f"[geo] Successfully geocoded {geocoded_count} locations")

    except Exception as e:
        print(f"[geo] Error during geocoding: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    print("[geo] Starting geocoding process...")
    update_geo_locations()
    print("[geo] Geocoding complete!")
