#Steps involved

#1. Pulls Datasets from the CMS Provider Data Metastore using the link provided in the assessment.
#2. Applied filters to keep datasets with Theme as 'Hospitals'
#3. Downloading the datasets using ThreadPoolExecutor library
#4. Converting all the column names to snake case.
#5. Tracking the last run timestamp in the Metadata, so that only the modified datasets are downloaded on future runs
#6. The script is designed to run daily and can be scheduled using CRON or Task Scheduler.

#Importing necessary libraries
import requests
import os
import re
import csv
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

#Defining the required parameters for the CMS URL, Metadata and directory
CMS_API_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
DOWNLOAD_DIR = "cms_hospital_datasets"
METADATA_FILE = "metadata.json"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

#Loading metadata
if os.path.exists(METADATA_FILE):
    with open(METADATA_FILE, "r") as f:
        metadata = json.load(f)
else:
    metadata = {"last_run": None, "downloaded_files": []}

last_run = metadata.get("last_run")
last_run_dt = datetime.fromisoformat(last_run) if last_run else None

#Fetching CMS datasets
print("Fetching CMS datasets...")
response = requests.get(CMS_API_URL)
response.raise_for_status()
datasets = response.json()

# Filtering datasets with theme 'Hospitals' and modified since last run
new_datasets = []
for ds in datasets:
    themes = ds.get("theme", [])
    modified = ds.get("modified")
    if "hospitals" in [t.lower() for t in themes]:
        if modified:
            modified_dt = datetime.fromisoformat(modified.replace("Z", "+00:00"))
            if not last_run_dt or modified_dt < last_run_dt: # Logic to fetch for files modified after last run date.
                new_datasets.append(ds)

print(f"Found {len(new_datasets)} new/updated hospital datasets.")

# Snake case conversion for the Column names
def to_snake_case(name):
    name = re.sub(r'[^a-zA-Z0-9\s]', '', name)  # remove special chars
    name = name.strip().lower().replace(" ", "_")
    return name

# Downloading and processing CSV files in parallel.
def download_and_process(ds):
    title = ds.get("title", "untitled").replace(" ", "_")
    distributions = ds.get("distribution", [])
    for dist in distributions:
        url = dist.get("downloadURL")
        if url and url.endswith(".csv"):
            file_path = os.path.join(DOWNLOAD_DIR, f"{title}.csv")
            print(f"Downloading {title} from {url}...")
            resp = requests.get(url)
            resp.raise_for_status()
            # Process CSV
            lines = resp.content.decode("utf-8").splitlines()
            reader = csv.reader(lines)
            headers = next(reader)
            headers = [to_snake_case(h) for h in headers]
            rows = list(reader)
            # Save processed CSV
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)
            metadata["downloaded_files"].append(file_path)

# Parallel execution
with ThreadPoolExecutor(max_workers=5) as executor:
    executor.map(download_and_process, new_datasets)

# Update metadata
metadata["last_run"] = datetime.utcnow().isoformat()
with open(METADATA_FILE, "w") as f:
    json.dump(metadata, f, indent=2)

print("Successfully processed the files!")
