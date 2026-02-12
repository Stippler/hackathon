#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import csv
import json
import time
import random
from html import unescape
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# -----------------------
# Config
# -----------------------
BRANCH_MAP_JSON = "filtered_branches_name_to_url.json"

OUT_DIR = os.path.join("data", "out")
os.makedirs(OUT_DIR, exist_ok=True)
OUT_CSV = os.path.join(OUT_DIR, "companies_urls.csv")

REQUEST_TIMEOUT = 45
MAX_RETRIES = 4
BACKOFF = 1.8

BASE_DELAY = 0.9
JITTER = 0.35

MAX_PAGES_PER_BRANCH = 4000
MAX_SECONDS_PER_BRANCH = 25 * 60  # 25 Minuten

ONLY_FIRST_N_BRANCHES = None  # z.B. 1 zum Test


# -----------------------
# Helpers
# -----------------------
def polite_sleep():
    time.sleep(max(0.0, BASE_DELAY + random.uniform(-JITTER, JITTER)))


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "de-AT,de;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://firmen.wko.at/",
    })
    return s


def fetch_get(session: requests.Session, url: str) -> tuple[int, str | None]:
    delay = 1.0
    for _ in range(MAX_RETRIES):
        try:
            r = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            if r.status_code in (429, 503):
                time.sleep(delay)
                delay *= BACKOFF
                continue
            if r.status_code == 200:
                return 200, r.text
            return r.status_code, None
        except Exception:
            time.sleep(delay)
            delay *= BACKOFF
    return 0, None


def extract_detail_urls(html: str, base_url: str) -> set[str]:
    """
    Nimmt a.title-link[href], unescape't &amp; und filtert robust auf firmaid=
    (in deinem HTML sind die Detail-Links genau so aufgebaut). [file:279]
    """
    soup = BeautifulSoup(html, "lxml")
    out = set()

    for a in soup.select("a.title-link[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue

        href = unescape(href)  # &amp; -> &
        abs_url = urljoin(base_url, href)

        if "firmaid=" in href.lower() or "firmaid=" in abs_url.lower():
            out.add(abs_url)

    return out


def get_next_page_url(html: str, base_url: str) -> str | None:
    """
    Offizielle Pagination: <link rel="next" href=".../?page=2"> [file:279]
    """
    soup = BeautifulSoup(html, "lxml")

    # BeautifulSoup kann rel als Liste liefern; daher beides abfangen
    link = soup.find("link", attrs={"rel": re.compile(r"\bnext\b", re.I)})
    if not link:
        # Alternative Suche falls rel als Liste gespeichert ist
        for ln in soup.find_all("link"):
            rel = ln.get("rel")
            if not rel:
                continue
            if isinstance(rel, list) and any(str(x).lower() == "next" for x in rel):
                link = ln
                break

    if not link:
        return None

    href = (link.get("href") or "").strip()
    if not href:
        return None

    return urljoin(base_url, href)


def ensure_csv():
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(["branche", "url"])


def append_rows(rows: list[tuple[str, str]]):
    if not rows:
        return
    with open(OUT_CSV, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows(rows)


# -----------------------
# Core crawling
# -----------------------
def crawl_branch(session: requests.Session, branche: str, start_url: str, debug: bool = False) -> set[str]:
    t0 = time.time()
    found: set[str] = set()

    current_url = start_url
    visited = set()
    page_no = 0

    while current_url:
        if time.time() - t0 > MAX_SECONDS_PER_BRANCH:
            print("Branch timeout:", branche)
            break
        if page_no >= MAX_PAGES_PER_BRANCH:
            print("Max pages reached:", branche)
            break
        if current_url in visited:
            # Loop guard (falls next mal zurückzeigt)
            break

        visited.add(current_url)
        page_no += 1

        st, html = fetch_get(session, current_url)
        if st != 200 or not html:
            print("GET failed:", st, current_url)
            break

        before = len(found)
        found |= extract_detail_urls(html, current_url)
        nxt = get_next_page_url(html, current_url)

        if debug:
            print(f"debug branche: {branche}")
            print(f"debug page {page_no}: {current_url}")
            print(f"debug +{len(found) - before} (total {len(found)})")
            print("debug next:", nxt)

        if not nxt:
            break

        current_url = nxt
        polite_sleep()

    return found


def main():
    with open(BRANCH_MAP_JSON, "r", encoding="utf-8") as f:
        branch_map: dict[str, str] = json.load(f)

    items = list(branch_map.items())
    if ONLY_FIRST_N_BRANCHES is not None:
        items = items[:ONLY_FIRST_N_BRANCHES]

    ensure_csv()
    session = make_session()

    for i, (branche, url) in enumerate(tqdm(items, desc="Branches", dynamic_ncols=True)):
        urls = crawl_branch(session, branche, url, debug=(i == 0))
        append_rows([(branche, u) for u in sorted(urls)])
        print(f"{branche}: wrote {len(urls)} urls")
        polite_sleep()

    print("Done:", OUT_CSV)


if __name__ == "__main__":
    main()

