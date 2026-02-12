# TODO:
# Copy Project

import os
import re
import csv
import time
import json
import hashlib
from dataclasses import dataclass, asdict
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


BASE = "https://firmen.wko.at"
BRANCH_LETTERS = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

DATA_DIR = "data"
SITES_DIR = os.path.join(DATA_DIR, "sites")
STATE_DIR = os.path.join(DATA_DIR, "state")
OUT_DIR = os.path.join(DATA_DIR, "out")
CSV_LETTERS_DIR = os.path.join(OUT_DIR, "by_letter")
os.makedirs(SITES_DIR, exist_ok=True)
os.makedirs(STATE_DIR, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(CSV_LETTERS_DIR, exist_ok=True)

SEEN_URLS_PATH = os.path.join(STATE_DIR, "seen_urls.jsonl")
QUEUE_PATH = os.path.join(STATE_DIR, "queue.jsonl")
RESULTS_CSV = os.path.join(OUT_DIR, "companies.csv")

# Match the company details links you showed:
# /a-haas---schrott-und-metalle-gmbh/salzburg/?firmaid=...&suchbegriff=...
DETAIL_LINK_RE = re.compile(r"/.+/.+/\?.*firmaid=", re.IGNORECASE)

# Also crawl listing pagination links that use ?Page=...
PAGE_RE = re.compile(r"[?&]Page=\d+", re.IGNORECASE)


@dataclass
class CompanyRecord:
    url: str
    firmaid: str | None = None
    firmenname: str | None = None
    firmenbuchnummer: str | None = None
    firmengericht: str | None = None
    gln: str | None = None


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def safe_filename_from_url(url: str) -> str:
    """
    Store pages as: {sha1}_{host}_{path-ish}.html (bounded length)
    """
    u = urlparse(url)
    host = u.netloc.replace(":", "_")
    path = (u.path.strip("/") or "root").replace("/", "_")
    if u.query:
        path = f"{path}__{sha1(u.query)[:10]}"
    name = f"{sha1(url)[:12]}_{host}_{path}"
    return (name[:180] + ".html")


def save_html(url: str, html: str) -> str:
    fn = safe_filename_from_url(url)
    path = os.path.join(SITES_DIR, fn)
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    return path


def append_jsonl(path: str, obj: dict):
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def load_seen_set(path: str) -> set[str]:
    seen = set()
    if not os.path.exists(path):
        return seen
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                seen.add(json.loads(line)["url"])
            except Exception:
                pass
    return seen


def load_queue(path: str) -> list[str]:
    q = []
    if not os.path.exists(path):
        return q
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                q.append(json.loads(line)["url"])
            except Exception:
                pass
    return q


def write_company_row(csv_path: str, rec: CompanyRecord):
    exists = os.path.exists(csv_path)
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(asdict(rec).keys()))
        if not exists:
            w.writeheader()
        w.writerow(asdict(rec))


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; research-crawler/1.0; +https://example.org)",
        "Accept-Language": "de-AT,de;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    return s


def fetch(session: requests.Session, url: str, timeout: int = 30, max_retries: int = 4, backoff: float = 1.5) -> str | None:
    """
    Robust-ish fetch with retry/backoff for transient errors.
    """
    delay = 1.0
    for attempt in range(max_retries):
        try:
            r = session.get(url, timeout=timeout)
            # Handle rate limiting
            if r.status_code in (429, 503):
                time.sleep(delay)
                delay *= backoff
                continue
            r.raise_for_status()
            return r.text
        except Exception:
            time.sleep(delay)
            delay *= backoff
    return None


def extract_branch_links(html: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    out = []
    for a in soup.select("a.link[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue
        # Many are absolute already; ensure absolute:
        abs_url = href if href.startswith("http") else urljoin(BASE, href)
        # Filter only firmen.wko.at:
        if urlparse(abs_url).netloc.endswith("firmen.wko.at"):
            out.append(abs_url)
    return sorted(set(out))


def extract_all_links(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    urls = []
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href or href.startswith("#") or href.lower().startswith("javascript:"):
            continue
        abs_url = href if href.startswith("http") else urljoin(base_url, href)
        if urlparse(abs_url).netloc.endswith("firmen.wko.at"):
            urls.append(abs_url)
    return urls


def is_detail_url(url: str) -> bool:
    u = urlparse(url)
    if not u.netloc.endswith("firmen.wko.at"):
        return False
    # Some are relative; by now should be absolute
    return bool(DETAIL_LINK_RE.search(u.path + ("?" + u.query if u.query else "")))


def is_pagination_or_listing_url(url: str) -> bool:
    # Heuristic: keep result pages and pagination.
    u = urlparse(url)
    if not u.netloc.endswith("firmen.wko.at"):
        return False
    s = u.path + ("?" + u.query if u.query else "")
    return ("Ergebnis.aspx" in s) or bool(PAGE_RE.search(s))


def parse_company_detail(html: str, url: str) -> CompanyRecord:
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text("\n", strip=True)

    # firmaid from query
    u = urlparse(url)
    firmaid = None
    if u.query:
        m = re.search(r"firmaid=([^&]+)", u.query, re.IGNORECASE)
        if m:
            firmaid = m.group(1)

    # Firmenname: from the <h3> within the title link, or page h1/h2 fallback
    firmenname = None
    h1 = soup.find(["h1"])
    if h1 and h1.get_text(strip=True):
        firmenname = h1.get_text(strip=True)
    if not firmenname:
        h3 = soup.find("h3")
        if h3 and h3.get_text(strip=True):
            firmenname = h3.get_text(strip=True)

    # Flexible label extraction: find a label line and take the next non-empty line
    def value_after_label(label: str) -> str | None:
        # Look for label with/without colon
        patterns = [
            label + ":",
            label
        ]
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        for i, ln in enumerate(lines):
            for p in patterns:
                if ln == p or ln.startswith(p + ":"):
                    # if line is exactly label, take next line; if "label: value", split
                    if ":" in ln and ln.split(":", 1)[0].strip() == label and ln.split(":", 1)[1].strip():
                        return ln.split(":", 1)[1].strip()
                    # otherwise take next non-empty
                    if i + 1 < len(lines):
                        return lines[i + 1].strip()
        return None

    rec = CompanyRecord(
        url=url,
        firmaid=firmaid,
        firmenname=value_after_label("Firmenname") or firmenname,
        firmenbuchnummer=value_after_label("Firmenbuchnummer"),
        firmengericht=value_after_label("Firmengericht"),
        gln=value_after_label("GLN"),
    )
    return rec


def main():
    session = make_session()

    seen = load_seen_set(SEEN_URLS_PATH)
    queue = load_queue(QUEUE_PATH)

    # Track which letter/branch each URL belongs to for intermediate CSV storage
    url_to_letter = {}

    # Seed queue with branch-letter pages if empty
    if not queue:
        for letter in BRANCH_LETTERS:
            seed_url = f"{BASE}/Branchen.aspx?Branche={letter}"
            queue.append(seed_url)
            url_to_letter[seed_url] = letter

    # We run a BFS crawl with heuristics:
    # - From Branchen.aspx?Branche=X: collect a.link => branch URLs
    # - From branch URLs: follow listing/pagination URLs + detail URLs
    # - From listing/pagination pages: follow pagination + detail URLs
    # - From detail pages: parse firm fields and still save HTML
    pbar = tqdm(total=0, unit="page", dynamic_ncols=True)
    i = 0

    # Use index-based loop so we can append to queue while iterating
    while i < len(queue):
        url = queue[i]
        i += 1

        if url in seen:
            continue

        html = fetch(session, url)
        if html is None:
            # mark as seen to avoid infinite retries; comment out if you prefer re-trying later
            seen.add(url)
            append_jsonl(SEEN_URLS_PATH, {"url": url, "status": "fetch_failed"})
            continue

        save_html(url, html)

        seen.add(url)
        append_jsonl(SEEN_URLS_PATH, {"url": url, "status": "ok"})
        pbar.total += 1
        pbar.update(1)

        # polite delay (adjust as needed)
        time.sleep(0.35)

        # Get current letter context
        current_letter = url_to_letter.get(url)

        # If this is a branch-letter page, extract branch links and enqueue
        if "Branchen.aspx" in url and "Branche=" in urlparse(url).query:
            # Extract letter from this page
            m = re.search(r"Branche=([A-Z])", url, re.IGNORECASE)
            if m:
                current_letter = m.group(1).upper()
                url_to_letter[url] = current_letter
            
            for b in extract_branch_links(html):
                if b not in seen:
                    queue.append(b)
                    append_jsonl(QUEUE_PATH, {"url": b})
                    if current_letter:
                        url_to_letter[b] = current_letter
            continue

        # Extract links from any page and decide what to enqueue
        links = extract_all_links(html, url)

        # If detail page => parse and write record
        if is_detail_url(url):
            rec = parse_company_detail(html, url)
            # Write to main CSV
            write_company_row(RESULTS_CSV, rec)
            # Also write to letter-specific CSV if we know the letter
            if current_letter:
                letter_csv = os.path.join(CSV_LETTERS_DIR, f"{current_letter}.csv")
                write_company_row(letter_csv, rec)
            continue

        # Otherwise: enqueue detail links + pagination/listing links
        for lk in links:
            if lk in seen:
                continue
            if is_detail_url(lk) or is_pagination_or_listing_url(lk):
                queue.append(lk)
                append_jsonl(QUEUE_PATH, {"url": lk})
                # Propagate letter context
                if current_letter:
                    url_to_letter[lk] = current_letter

    pbar.close()
    print(f"Done. Saved HTML to: {SITES_DIR}")
    print(f"Companies CSV: {RESULTS_CSV}")
    print(f"Letter-specific CSVs: {CSV_LETTERS_DIR}/")
    print(f"Seen log: {SEEN_URLS_PATH}")
    print(f"Queue log: {QUEUE_PATH}")


if __name__ == "__main__":
    main()
