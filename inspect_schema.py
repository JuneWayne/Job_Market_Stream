# inspect_schema.py
import duckdb
from pathlib import Path

DB_PATH = Path("data/jobs.duckdb")

con = duckdb.connect(str(DB_PATH), read_only=True)

print("=== Tables ===")
print(con.execute("PRAGMA show_tables;").fetchdf())

print("\n=== jobs schema ===")
print(con.execute("PRAGMA table_info('jobs');").fetchdf())
