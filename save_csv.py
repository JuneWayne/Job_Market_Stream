import csv
from pathlib import Path

OUTPUT_FILE = Path("data/parsed_jobs.csv")

FIELDNAMES = [
    "job_id",
    "job_title",
    "job_description",
    "company_name",
    "location",
    "job_function",
    "skills",
    "degree_requirement",
    "time_posted_parsed",
    "application_link",
    "num_applicants_int",
    "work_mode",
]

def append_parsed_job(parsed):
    # Make sure we only write known fields
    row = {field: parsed.get(field) for field in FIELDNAMES}

    file_exists = OUTPUT_FILE.exists()

    with OUTPUT_FILE.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)

        # Write header
        if not file_exists:
            writer.writeheader()

        writer.writerow(row)
