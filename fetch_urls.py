import os
import requests
import urllib.parse
import time
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo import MongoClient
from datetime import datetime, timezone

# MongoDB setup
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://root:password@localhost:27017/")
MONGO_DB = os.environ.get("MONGO_DB", "pinterest")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "ideas")

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
ideas_col = db[MONGO_COLLECTION]

BATCH_SIZE = 20

# Pinterest API request parameters
API_URL = "https://pinterest.com/resource/InterestResource/get/"
HEADERS = {
    "X-Pinterest-PWS-Handler": "www/ideas/[interest]/[id].js",
}

def build_request(url):
    match = re.search(r'/(\d+)/?$', url)
    interest_id = match.group(1) if match else "933065609551"
    data_payload = {
        "options": {
            "field_set_key": "ideas_hub",
            "get_page_metadata": True,
            "interest": interest_id,
            "is_internal_preview": False
        },
        "context": {}
    }
    params = {
        "data": urllib.parse.quote(json.dumps(data_payload)),
        "source_url": urllib.parse.quote(url)
    }
    query = f"data={params['data']}&source_url={params['source_url']}"
    full_url = f"{API_URL}?{query}"
    return full_url

def fetch_url(url):
    for attempt in range(3):
        try:
            full_url = build_request(url)
            response = requests.get(full_url, headers=HEADERS)
            if response.status_code != 200:
                print(f"Status: {response.status_code} for {url}")
                continue
            data = response.json()
            rr = data.get("resource_response", {})
            d = rr.get("data", {})
            if not d or "seo_canonical_display_name" not in d:
                print(f"Expected data missing in response for {url}")
                continue
            row = {
                "seo_canonical_display_name": d.get("seo_canonical_display_name", ""),
                "follower_count": d.get("follower_count", 0),
                "internal_search_count": d.get("internal_search_count", 0),
                "canonical_term_id": (d.get("canonical_term") or {}).get("id", "")
            }
            return (url, row, None)
        except Exception as e:
            print(f"Attempt {attempt+1} failed for {url}: {e}")
            continue
    return (url, None, "Failed after 3 attempts")

def process_batch(batch):
    results = []
    with ThreadPoolExecutor(max_workers=BATCH_SIZE) as executor:
        future_to_doc = {executor.submit(fetch_url, doc["url"]): doc for doc in batch}
        for future in as_completed(future_to_doc):
            doc = future_to_doc[future]
            url, info, error = future.result()
            if error:
                print(f"Error for url {url}: {error}. Skipping.")
                continue
            results.append((doc["_id"], info))
    return results

def main():
    total_to_process = ideas_col.count_documents({"status": "unprocessed"})
    if total_to_process == 0:
        print("No unprocessed ideas found.")
        return

    processed_count = 0
    start_time = time.time()

    while True:
        batch = list(ideas_col.find({"status": "unprocessed"}).limit(BATCH_SIZE))
        if not batch:
            print("No more unprocessed ideas.")
            break

        results = process_batch(batch)
        now = datetime.now(timezone.utc)
        for _id, info in results:
            ideas_col.update_one(
                {"_id": _id},
                {
                    "$set": {
                        "info": info,
                        "status": "processed",
                        "processed_at": now
                    }
                }
            )
        processed_count += len(results)

        elapsed = time.time() - start_time
        speed = processed_count / elapsed * 60 if elapsed > 0 else 0
        remaining = total_to_process - processed_count
        eta = (remaining / speed) if speed > 0 else 0
        avg_speed = (processed_count / elapsed * 60) if elapsed > 0 else 0

        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
              f"{processed_count}/{total_to_process} processed | elapsed: {elapsed:.1f}s | "
              f"speed: {speed:.1f}/min (avg {avg_speed:.1f}/min) | est left: {eta:.1f} min")

if __name__ == "__main__":
    main()
