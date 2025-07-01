import requests
from bs4 import BeautifulSoup
import json
import os
from concurrent.futures import ThreadPoolExecutor

BASE_URL = "https://dergipark.org.tr"
JSON_FILE = "parallel_papers.json"

def get_issue_links():
    archive_url = f"{BASE_URL}/tr/pub/politeknik/archive"
    response = requests.get(archive_url)
    soup = BeautifulSoup(response.content, "html.parser")

    issue_links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("//dergipark.org.tr/tr/pub/politeknik/issue/"):
            full_url = "https:" + href
            issue_links.add(full_url)

    return issue_links

def get_paper_links(issue_link):
    response = requests.get(issue_link)
    soup = BeautifulSoup(response.content, "html.parser")

    paper_links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith(issue_link + "/"):
            paper_links.add(href)

    return paper_links

def get_paper_attributes(paper_link):
    try:
        response = requests.get(paper_link)
        soup = BeautifulSoup(response.content, "html.parser")

        article_div = soup.find("div", id="article_en")
        if article_div is None:
            print(f"[!] Skipping {paper_link} — 'article_en' not found.")
            return None

        title_tag = article_div.find("h3", class_="article-title")
        title = title_tag.get_text(strip=True) if title_tag else "N/A"

        abstract_div = article_div.find("div", class_="article-abstract")
        abstract_p = abstract_div.find("p") if abstract_div else None
        abstract = abstract_p.get_text(strip=True) if abstract_p else "N/A"

        keywords_div = article_div.find("div", class_="article-keywords")
        keywords = [a.get_text(strip=True) for a in keywords_div.find_all("a")] if keywords_div else []

        author_meta_tags = soup.find_all("meta", attrs={"name": "citation_author"})
        authors = [tag["content"] for tag in author_meta_tags if tag.has_attr("content")]

        return {
            "title": title,
            "abstract": abstract,
            "keywords": keywords,
            "url": paper_link,  # Adding URL for reference
            "authors": authors
        }
    except Exception as e:
        print(f"[!] Error processing {paper_link}: {str(e)}")
        return None

def save_json_batch(data_batch):
    existing_data = []
    if os.path.exists(JSON_FILE):
        try:
            with open(JSON_FILE, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            existing_data = []

    existing_data.extend(data_batch)

    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(existing_data, f, indent=4, ensure_ascii=False)

    print(f"Saved {len(data_batch)} papers to {JSON_FILE}")

def main():
    # Get all issue links
    issue_links = get_issue_links()
    print(f"Found {len(issue_links)} issues")

    # Get all paper links (parallel)
    paper_links = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(get_paper_links, issue) for issue in issue_links]
        for future in futures:
            paper_links.extend(future.result())
    print(f"Found {len(paper_links)} papers")

    # Process papers in parallel with batching
    batch_size = 10
    current_batch = []
    
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(get_paper_attributes, paper): paper for paper in paper_links}
        
        for future in futures:
            result = future.result()
            if result:
                current_batch.append(result)
                if len(current_batch) >= batch_size:
                    save_json_batch(current_batch)
                    current_batch = []
    
    # Save any remaining papers
    if current_batch:
        save_json_batch(current_batch)

if __name__ == "__main__":
    main()
