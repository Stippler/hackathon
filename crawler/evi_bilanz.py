#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode, urljoin

import requests
from bs4 import BeautifulSoup

BASE_ORIGIN = "https://www.evi.gv.at"
SEARCH_PATH = "/s"
DEFAULT_QUERY = "Bilanz"
DEFAULT_OUTPUT = os.path.join("data", "out", "evi_bilanz.jsonl")
DEFAULT_TIMEOUT_SECONDS = 20
DEFAULT_DELAY_SECONDS = 0.25

DATE_RE = re.compile(r"Ver[oö]ffentlicht auf EVI am\s+(\d{2}\.\d{2}\.\d{4})", re.IGNORECASE)
COMPANY_FB_RE = re.compile(r"^(?P<name>.+?)\s*\((?P<firmenbuchnummer>[^()]+)\)\s*$")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = " ".join(value.split()).strip()
    return cleaned or None


def build_search_url(query: str, page: int) -> str:
    params: dict[str, Any] = {"suche": query}
    if page > 1:
        params["page"] = page
    return f"{BASE_ORIGIN}{SEARCH_PATH}?{urlencode(params)}"


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "de-AT,de;q=0.9,en;q=0.8",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": f"{BASE_ORIGIN}/",
        }
    )
    return session


def parse_card(anchor) -> dict[str, Any] | None:
    href = clean_text(anchor.get("href"))
    if not href:
        return None

    article = anchor.select_one("article")
    if not article:
        return None

    paragraphs = article.select("p")
    if len(paragraphs) < 3:
        return None

    publication_line = clean_text(paragraphs[0].get_text(" ", strip=True))
    publication_type = clean_text(paragraphs[1].get_text(" ", strip=True))
    company_line = clean_text(paragraphs[2].get_text(" ", strip=True))
    if not company_line:
        return None

    publication_date = None
    if publication_line:
        match = DATE_RE.search(publication_line)
        if match:
            publication_date = match.group(1)

    company_name = company_line
    firmenbuchnummer = None
    match = COMPANY_FB_RE.match(company_line)
    if match:
        company_name = clean_text(match.group("name"))
        firmenbuchnummer = clean_text(match.group("firmenbuchnummer"))

    detail_url = urljoin(BASE_ORIGIN, href)
    record = {
        "query": DEFAULT_QUERY,
        "publication_date": publication_date,
        "publication_type": publication_type,
        "detail_url": detail_url,
        "company_name": company_name,
        "firmenbuchnummer": firmenbuchnummer,
        "source_item_path": href,
        "crawled_at": now_iso(),
    }
    return record


def extract_cards(html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    out: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for anchor in soup.select("a.group[href]"):
        record = parse_card(anchor)
        if not record:
            continue
        detail_url = record.get("detail_url")
        if not detail_url or detail_url in seen_urls:
            continue
        seen_urls.add(detail_url)
        out.append(record)
    return out


def append_jsonl(path: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def crawl_evi_bilanz(
    query: str = DEFAULT_QUERY,
    output_path: str = DEFAULT_OUTPUT,
    max_pages: int | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    delay_seconds: float = DEFAULT_DELAY_SECONDS,
) -> dict[str, Any]:
    session = make_session()
    page = 1
    total_rows = 0
    page_stats: list[dict[str, Any]] = []
    global_seen_urls: set[str] = set()

    while True:
        if max_pages is not None and page > max_pages:
            break

        url = build_search_url(query, page)
        resp = session.get(url, timeout=timeout_seconds)
        resp.raise_for_status()

        rows = extract_cards(resp.text)
        if not rows:
            break

        new_rows: list[dict[str, Any]] = []
        for row in rows:
            detail_url = row.get("detail_url")
            if not detail_url or detail_url in global_seen_urls:
                continue
            row["query"] = query
            row["source_search_url"] = url
            global_seen_urls.add(detail_url)
            new_rows.append(row)

        if not new_rows:
            break

        append_jsonl(output_path, new_rows)
        total_rows += len(new_rows)
        page_stats.append({"page": page, "rows": len(new_rows), "url": url})
        page += 1

        if delay_seconds > 0:
            time.sleep(delay_seconds)

    return {
        "meta": {
            "query": query,
            "total_rows": total_rows,
            "pages_crawled": len(page_stats),
            "generated_at": now_iso(),
            "output_path": output_path,
        },
        "pages": page_stats,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crawl EVI Bilanz search results with pagination.")
    parser.add_argument("--query", type=str, default=DEFAULT_QUERY, help="Search term, default: Bilanz")
    parser.add_argument("--output", type=str, default=DEFAULT_OUTPUT, help="Output JSONL path")
    parser.add_argument("--max-pages", type=int, default=None, help="Optional page limit for testing")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS, help="HTTP timeout in seconds")
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=DEFAULT_DELAY_SECONDS,
        help="Sleep between page fetches",
    )
    parser.add_argument(
        "--truncate-output",
        action="store_true",
        help="Delete existing output file before writing new rows",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.truncate_output and os.path.exists(args.output):
        os.remove(args.output)

    payload = crawl_evi_bilanz(
        query=args.query,
        output_path=args.output,
        max_pages=args.max_pages,
        timeout_seconds=args.timeout,
        delay_seconds=max(0.0, args.delay_seconds),
    )
    print(
        f"Crawled query='{payload['meta']['query']}' pages={payload['meta']['pages_crawled']} "
        f"rows={payload['meta']['total_rows']} -> {payload['meta']['output_path']}",
        flush=True,
    )


if __name__ == "__main__":
    main()
