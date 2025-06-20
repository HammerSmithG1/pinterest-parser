import xml.etree.ElementTree as ET

def extract_urls_from_file(filename):
    ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    tree = ET.parse(filename)
    root = tree.getroot()
    urls = [loc.text for loc in root.findall('.//sm:loc', ns)]
    return urls

if __name__ == "__main__":
    urls = extract_urls_from_file("ideas_sitemap.xml")
    for url in urls:
        print(url)
    print(f"Total URLs: {len(urls)}")