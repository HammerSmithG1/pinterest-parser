import requests
import xml.etree.ElementTree as ET
import os
import csv
from datetime import datetime

# Hardcoded sitemap URLs
sitemap_urls = [
    "https://ru.pinterest.com/v3_sitemaps/ideas_hub_expansion_v2_sitemap_ru.pinterest.com.xml"
]

def fetch_sitemap(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.content

def parse_sitemap(xml_content):
    root = ET.fromstring(xml_content)
    # Namespace handling
    ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    locs = [loc.text for loc in root.findall('.//ns:loc', ns)]
    return locs

def main():
    all_urls = []
    for url in sitemap_urls:
        xml_content = fetch_sitemap(url)
        urls = parse_sitemap(xml_content)
        all_urls.extend(urls)

    # Prepare output directory
    os.makedirs('output', exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f'output/sitemap_batches_{timestamp}.csv'

    # Write to CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        for url in all_urls:
            clean_url = url.strip()  # Remove spaces and tabs
            writer.writerow([clean_url])
    print(f"Saved {len(all_urls)} URLs to {output_file}")

if __name__ == "__main__":
    main()
