import requests
from bs4 import BeautifulSoup, Comment
import pandas as pd
import time
from datetime import datetime
from job_parser import parse_job_postings
import re
import random
import logging

logger = logging.getLogger(__name__)

# Rotate user agents to reduce detection
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
]

def get_headers():
    """Return headers with a random user agent."""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }

def request_with_retry(url, max_retries=3, base_delay=5):
    """Make a GET request with exponential backoff retry."""
    for attempt in range(max_retries):
        try:
            headers = get_headers()
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code == 200:
                return response
            elif response.status_code == 429:
                # Rate limited - wait longer
                delay = base_delay * (2 ** attempt) + random.uniform(1, 5)
                logger.warning(f"Rate limited (429), waiting {delay:.1f}s before retry...")
                time.sleep(delay)
            else:
                logger.warning(f"Got status {response.status_code} for {url}")
                return None
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
            delay = base_delay * (2 ** attempt) + random.uniform(1, 3)
            logger.warning(f"Request failed ({type(e).__name__}), retry {attempt+1}/{max_retries} after {delay:.1f}s")
            time.sleep(delay)
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None
    logger.error(f"Failed after {max_retries} retries: {url}")
    return None

# Keep HEADERS for backward compatibility
HEADERS = {
    "User-Agent": USER_AGENTS[0]
}
def get_num_applicants(detail_soup):
    """
    Grab the text for number-of-applicants from any element that has
    class 'num-applicants__caption' (span, figcaption, etc.).
    """
    # This matches ANY tag with that class
    tag = detail_soup.find(class_="num-applicants__caption")
    if tag:
        return tag.get_text(strip=True)
    return None


def extract_application_link(job_soup, job_id):

    code = job_soup.find("code", id="applyUrl")
    if code:
        comment = code.find(string=lambda s: isinstance(s, Comment))
        if comment:
            url = comment.strip().strip('"')
            if url:
                return url

    apply_anchor = job_soup.select_one("a.apply-button[href]")
    if apply_anchor and apply_anchor.get("href"):
        return apply_anchor["href"]

    # Fallback
    apply_anchor = job_soup.find(
        "a",
        attrs={"data-tracking-control-name": re.compile("apply-link")}
    )
    if apply_anchor and apply_anchor.get("href"):
        return apply_anchor["href"]

    easy_button = job_soup.select_one("button.apply-button")
    if easy_button:
        # Best we can do is link to the canonical job page
        return f"https://www.linkedin.com/jobs/view/{job_id}"

    # No luck
    return None


def scrape_linkedin(keywords, location, geo_id,
                    f_tpr="r86400", max_results=2000):
    """
    Scrape LinkedIn jobs using the public 'jobs-guest' API.

    Parameters:
    - keywords: job search keywords (e.g. "Data intern")
    - location: job location text (e.g. "United States")
    - geo_id: LinkedIn geoId for the region (e.g. "103644278" for US)
    - f_tpr: time posted range (e.g. "r86400" = past day)
    - max_results: safety limit so we don't scrape unlimited jobs

    Returns:
    - A pandas DataFrame with the collected job data.
    """
    base_url = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
    job_list = []   # will store dict for each job
    seen_ids = set()
    start = 0       # offset for pagination

    while True:
        # Parameters for the search request
        params = {
            "keywords": keywords,
            "location": location,
            "geoId": geo_id,
            "f_TPR": f_tpr,
            "start": start,
        }

        # Get the list of job cards with retry
        url = f"{base_url}?keywords={params['keywords']}&location={params['location']}&geoId={params['geoId']}&f_TPR={params['f_TPR']}&start={params['start']}"
        response = request_with_retry(url, max_retries=3, base_delay=5)
        if response is None:
            logger.warning(f"Failed to fetch page at start={start}, skipping...")
            start += 25
            time.sleep(5)
            continue

        # parse the current page's html structure
        soup = BeautifulSoup(response.text, "html.parser")
        page_jobs = soup.find_all("li")
        logger.info(f"start={start}, jobs_on_page={len(page_jobs)}")
        print("start={}, jobs_on_page={}".format(start, len(page_jobs)))

        # If no jobs returned, break
        if not page_jobs:
            break

        # Collect job IDs from this batch
        id_list = []
        for job in page_jobs:
            base_card_div = job.find("div", {"class": "base-card"})
            if base_card_div and base_card_div.get("data-entity-urn"):
                job_id = base_card_div.get("data-entity-urn").split(":")[-1]
                if job_id not in seen_ids:
                    seen_ids.add(job_id)
                    id_list.append(job_id)

        # Random pause to avoid rate limiting (3-6 seconds)
        time.sleep(random.uniform(3, 6))

        # For each job ID, request the detailed job posting with retry
        for job_id in id_list:
            detail_url = "https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{}".format(job_id)
            detail_response = request_with_retry(detail_url, max_retries=3, base_delay=3)

            if detail_response is None:
                continue

            detail_soup = BeautifulSoup(detail_response.text, "html.parser")
            job_post = {"job_id": job_id}

            # helper function to retrieve text
            def safe_text(selector, attrs=None):
                if attrs is None:
                    attrs = {}
                tag = detail_soup.find(selector, attrs)
                if tag:
                    return tag.get_text(strip=True)
                else:
                    return None

            # Extract basic fields
            job_post["job_title"] = safe_text("h2", {"class": "top-card-layout__title"})
            job_post["company_name"] = safe_text("a", {"class": "topcard__org-name-link"})
            job_post["location"] = safe_text(
                "span", {"class": "topcard__flavor topcard__flavor--bullet"}
            )
            job_post["time_posted"] = safe_text("span", {"class": "posted-time-ago__text"})
            # job_post["num_applicants"] = safe_text(
            #     "span", {"class": "num-applicants__caption"}
            # )
            job_post["num_applicants"] = get_num_applicants(detail_soup)


            # Job description
            desc_div = detail_soup.find("div", {"class": "decorated-job-posting__details"})
            if desc_div:
                job_post["job_description"] = desc_div.get_text(strip=True)
            else:
                job_post["job_description"] = None

            job_list.append(job_post)
            
            # Small delay between job detail requests
            time.sleep(random.uniform(1, 2))


            job_post["application_link"] = extract_application_link(detail_soup, job_id)


            # Stop if max search limit hit
            if len(job_list) >= max_results:
                print("Hit max_results cap.")
                break

            # small delay between detail requests
            time.sleep(2)

        if len(job_list) >= max_results:
            break

        # scroll by 5 pages at a time
        start += 5

    # convert list of job dicts into a DataFrame
    df = pd.DataFrame(job_list)

    # drop rows that don't have a job title
    if not df.empty and "job_title" in df.columns:
        df = df.dropna(subset=["job_title"])

    return df


if __name__ == "__main__":
    df_jobs = scrape_linkedin(
        keywords="Data intern",
        location="United States",
        geo_id="103644278",
        f_tpr="r86400",  # past day: r86400, past week: r604800, past month: r2592000, anytime: ""
        max_results=10000,
    )

    if df_jobs.empty:
        print("No jobs found.")
    else:
        print("Found {} jobs\n".format(len(df_jobs)))

    enriched_jobs = df_jobs.apply(parse_job_postings, axis=1, result_type="expand")
    df_jobs = pd.DataFrame(enriched_jobs)
    df_jobs.to_csv("Job_Data.csv", index=False)
    print("Saved to Job_Data.csv")