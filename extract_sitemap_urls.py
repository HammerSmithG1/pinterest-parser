import os
import glob
import csv
import requests
import gzip
import io
import xml.etree.ElementTree as ET
from datetime import datetime
from pymongo import MongoClient, UpdateOne
from entities.url_entity import IdeaURL


def find_latest_csv(folder, pattern):
    files = glob.glob(os.path.join(folder, pattern))
    if not files:
        raise FileNotFoundError("No matching CSV files found.")
    latest = max(files, key=os.path.getmtime)
    return latest


def fetch_url(url):
    resp = requests.get(url)
    resp.raise_for_status()
    if url.endswith('.gz'):
        with gzip.GzipFile(fileobj=io.BytesIO(resp.content)) as f:
            return f.read()
    return resp.content


def parse_sitemap_xml(xml_content):
    ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    root = ET.fromstring(xml_content)
    return [loc.text for loc in root.findall('.//ns:loc', ns)]


def main():
    output_dir = 'output'
    latest_csv = find_latest_csv(output_dir, 'sitemap_batches_*.csv')
    with open(latest_csv, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        urls = [row[0] for row in reader if row]

    all_urls = []
    total = 0

    # Connect to MongoDB
    client = MongoClient("mongodb://root:password@localhost:27017/")
    db = client["pinterest"]
    ideas_col = db["ideas"]

    for i, url in enumerate(urls, 1):
        try:
            xml_content = fetch_url(url)
            urls_in_xml = parse_sitemap_xml(xml_content)
            all_urls.extend(urls_in_xml)
            total += len(urls_in_xml)
            print(f"[{i}/{len(urls)}] {os.path.basename(url)} - {len(urls_in_xml)} urls / {total} total")
            # Efficient batch upsert: insert only new ids
            ideas = [IdeaURL(url=u) for u in urls_in_xml]
            ids = [idea.id for idea in ideas if idea.id]
            # Query existing ids in one call
            existing_ids = set()
            if ids:
                existing_ids = set(doc["id"] for doc in ideas_col.find({"id": {"$in": ids}}, {"id": 1}))
            # Prepare docs for only new ids
            new_docs = [idea.to_dict() for idea in ideas if idea.id and idea.id not in existing_ids]
            if new_docs:
                ideas_col.insert_many(new_docs)
        except Exception as e:
            print(f"Error processing {url}: {e}")

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_path = os.path.join(output_dir, f'sitemap_urls_{timestamp}.csv')
    with open(out_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        for url in all_urls:
            writer.writerow([url])
    print(f"Saved {len(all_urls)} URLs to {out_path}")
    print(f"Inserted {len(all_urls)} URLs into MongoDB collection 'ideas'.")


if __name__ == "__main__":
    main()
if __name__ == "__main__":
    main()
