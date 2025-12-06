import csv
from pathlib import Path

OUTPUT_FILE = Path("data/parsed_jobs.csv")

FIELDNAMES = [
    "job_id",
    "job_title",
    "job_description",
    "company_name",
    "industry",
    "location",
    "latitude",
    "longitude",
    "job_function",
    "skills",
    "degree_requirement",
    "time_posted_parsed",
    "application_link",
    "num_applicants_int",
    "work_mode",
    "scraped_at",
]

def append_parsed_job(parsed):
    # Make sure we only write known fields
    row = {field: parsed.get(field) for field in FIELDNAMES}

    file_exists = OUTPUT_FILE.exists()

    with OUTPUT_FILE.open("a", newline="", encoding="utf-8") as f:
        # Use QUOTE_ALL to ensure all fields are properly quoted
        # This prevents issues with commas/quotes in job descriptions
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, quoting=csv.QUOTE_ALL)

        # Write header
        if not file_exists:
            writer.writeheader()

        writer.writerow(row)
