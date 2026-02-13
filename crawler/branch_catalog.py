#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import random
import time
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BRANCH_INDEX_URL = "https://firmen.wko.at/branchen.aspx"
CATALOG_PATH = os.path.join("data", "wko_branch_catalog.json")
LETTERS = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
REQUEST_TIMEOUT_SECONDS = 12
BETWEEN_LETTERS_SECONDS = 0.35
MAX_RETRIES = 4
MAX_DENIED_BACKOFF_SECONDS = 60


def _make_session():
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept-Language": "de-AT,de;q=0.9,en;q=0.8",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://firmen.wko.at/",
        }
    )
    return s


def _extract_form_fields(soup):
    form = soup.select_one("form#aspnetForm")
    if not form:
        return None, None
    fields = {}
    for inp in form.select("input[name]"):
        inp_type = (inp.get("type") or "").lower()
        if inp_type in {"submit", "button", "image", "reset", "file"}:
            continue
        fields[inp.get("name")] = inp.get("value", "")
    action = form.get("action") or ""
    return fields, action


def _extract_branch_links(soup):
    rows = []
    for a in soup.select("ul.link-list a.link[href]"):
        name = a.get_text(" ", strip=True)
        href = (a.get("href") or "").strip()
        if not name or not href:
            continue
        # Branch links are absolute on this page, keep robust urljoin for safety.
        url = urljoin(BRANCH_INDEX_URL, href)
        if not url.startswith("https://firmen.wko.at/"):
            continue
        rows.append((name, url))
    return rows


def _post_letter(session, letter):
    resp = _request_with_backoff(session, "GET", BRANCH_INDEX_URL)
    if resp is None:
        raise RuntimeError("GET branchen.aspx failed after retries")
    soup = BeautifulSoup(resp.text, "lxml")
    fields, action = _extract_form_fields(soup)
    if not fields:
        raise RuntimeError("missing aspnet form on branchen page")

    btn = soup.select_one(f"input[name$='letterButton'][value='{letter}']")
    if not btn:
        raise RuntimeError(f"missing letter button for '{letter}'")
    fields[btn.get("name")] = letter

    target_url = urljoin(BRANCH_INDEX_URL, action) if action else BRANCH_INDEX_URL
    resp = _request_with_backoff(session, "POST", target_url, data=fields)
    if resp is None:
        raise RuntimeError(f"POST letter={letter} failed after retries")
    return BeautifulSoup(resp.text, "lxml")


def _request_with_backoff(session, method, url, data=None):
    denied_streak = 0
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if method == "GET":
                resp = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
            else:
                resp = session.post(url, data=data, timeout=REQUEST_TIMEOUT_SECONDS)
        except requests.RequestException:
            sleep_s = min(4.0, 0.5 * attempt) + random.uniform(0.0, 0.15)
            time.sleep(sleep_s)
            continue

        body = resp.text or ""
        denied = resp.status_code == 403 or "access denied" in body.lower()
        if denied:
            denied_streak += 1
            wait_s = min(MAX_DENIED_BACKOFF_SECONDS, 2 ** denied_streak)
            time.sleep(wait_s)
            continue

        if resp.status_code >= 400:
            sleep_s = min(6.0, 0.6 * attempt) + random.uniform(0.0, 0.2)
            time.sleep(sleep_s)
            continue
        return resp
    return None


def discover_branches(
    branch_index_url: str = BRANCH_INDEX_URL,
    catalog_path: str = CATALOG_PATH,
):
    if branch_index_url != BRANCH_INDEX_URL:
        raise ValueError("custom branch_index_url not yet supported in this crawler")

    session = _make_session()
    seen = {}
    branches = []
    failed_letters = []
    for letter in LETTERS:
        try:
            soup = _post_letter(session, letter)
        except Exception:
            failed_letters.append(letter)
            continue
        rows = _extract_branch_links(soup)
        for name, url in rows:
            # Keep first seen letter assignment, dedupe by exact pair.
            key = (name, url)
            if key in seen:
                continue
            seen[key] = True
            branches.append({"branche": name, "url": url, "letter": letter})
        time.sleep(BETWEEN_LETTERS_SECONDS + random.uniform(0.0, 0.15))

    branches.sort(key=lambda x: (x["letter"], x["branche"]))
    payload = {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "source": branch_index_url,
            "count": len(branches),
            "letters_crawled": len(LETTERS),
            "failed_letters": failed_letters,
        },
        "branches": branches,
    }

    os.makedirs(os.path.dirname(catalog_path), exist_ok=True)
    with open(catalog_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    return payload


def main():
    payload = discover_branches()
    print(f"Discovered {payload['meta']['count']} branches -> {CATALOG_PATH}", flush=True)


if __name__ == "__main__":
    main()

