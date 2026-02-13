#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import pathlib
import re
import shutil
import time
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

BRANCH_MAP_JSON = "filtered_branches_name_to_url.json"
OUT_DIR = os.path.join("data", "out")
OUT_JSONL = os.path.join(OUT_DIR, "companies.jsonl")
DEBUG_DIR = "debug"

ONLY_FIRST_N_BRANCHES = None
REQUEST_TIMEOUT_SECONDS = 2
MAX_RETRIES = 3
RETRY_SLEEP_SECONDS = 0.15
MAX_SECONDS_PER_BRANCH = 25 * 60
MAX_LOAD_MORE_CLICKS = 500
CLICK_PAUSE_SECONDS = 0.05
POST_403_RETRIES = 2
POST_403_SLEEP_SECONDS = 0.3

CARD_SELECTOR = "article.search-result-article"
DETAIL_LINK_SELECTOR = "a.title-link[href]"
PHONE_SELECTOR = 'a[itemprop="telephone"]'
EMAIL_SELECTOR = 'a[itemprop="email"]'
WEB_SELECTOR = 'a[itemprop="url"]'
STREET_SELECTOR = ".address .street"
PLACE_SELECTOR = ".address .place"
FORM_SELECTOR = "form#aspnetForm"
LOAD_MORE_NAME = "ctl00$ContentPlaceHolder1$nextPageButton"


def ts():
    return datetime.now().strftime("%H:%M:%S")


def log(msg: str):
    print(f"[{ts()}] {msg}", flush=True)


def ensure_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)
    pathlib.Path(DEBUG_DIR).mkdir(exist_ok=True)


def append_jsonl(records):
    if not records:
        return
    with open(OUT_JSONL, "a", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        f.flush()


def snapshot_output(label: str):
    if not os.path.exists(OUT_JSONL):
        return
    ts = int(time.time())
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", label)[:80]
    dst = os.path.join(DEBUG_DIR, f"{ts}_{safe}.jsonl")
    shutil.copyfile(OUT_JSONL, dst)
    log(f"[debug] wrote {dst}")


def dump_html(label: str, html: str):
    ts = int(time.time())
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", label)[:80]
    html_path = os.path.join(DEBUG_DIR, f"{ts}_{safe}.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    log(f"[debug] wrote {html_path}")


def clean_text(s):
    if s is None:
        return None
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


def make_session():
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


def fetch_with_retry(session, method, url, data=None):
    last_exc = None
    for attempt in range(MAX_RETRIES):
        req_t0 = time.perf_counter()
        try:
            if method == "GET":
                resp = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
            else:
                resp = session.post(url, data=data, timeout=REQUEST_TIMEOUT_SECONDS)
            took = time.perf_counter() - req_t0
            log(f"{method} try={attempt + 1}/{MAX_RETRIES} status={resp.status_code} took={took:.2f}s")
            if resp.status_code == 200:
                return resp
            if resp.status_code in (403, 429, 503) and attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_SLEEP_SECONDS)
                continue
            return resp
        except requests.RequestException as e:
            last_exc = e
            took = time.perf_counter() - req_t0
            log(f"{method} try={attempt + 1}/{MAX_RETRIES} error={e.__class__.__name__} took={took:.2f}s")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_SLEEP_SECONDS)
                continue
            raise
    if last_exc:
        raise last_exc
    raise RuntimeError("unreachable fetch retry state")


def parse_hidden_form_fields(soup):
    form = soup.select_one(FORM_SELECTOR)
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


def has_load_more(soup):
    return soup.select_one(f"input[name='{LOAD_MORE_NAME}'], input[name$='nextPageButton']") is not None


def extract_cards_from_html(html, branche: str, base_url: str):
    soup = BeautifulSoup(html, "lxml")
    cards = soup.select(CARD_SELECTOR)
    out = []
    for card in cards:
        detail_link = card.select_one(DETAIL_LINK_SELECTOR)
        href = detail_link.get("href") if detail_link else None
        wko_detail_url = urljoin(base_url, href) if href else None

        name = clean_text(detail_link.get_text(" ", strip=True)) if detail_link else None
        phone = clean_text(card.select_one(PHONE_SELECTOR).get_text(" ", strip=True)) if card.select_one(PHONE_SELECTOR) else None
        email = card.select_one(EMAIL_SELECTOR).get("href") if card.select_one(EMAIL_SELECTOR) else None
        if email and email.lower().startswith("mailto:"):
            email = email[7:]

        website_node = card.select_one(f"{WEB_SELECTOR} span") or card.select_one(WEB_SELECTOR)
        company_website = clean_text(website_node.get_text(" ", strip=True)) if website_node else None

        street = clean_text(card.select_one(STREET_SELECTOR).get_text(" ", strip=True)) if card.select_one(STREET_SELECTOR) else None
        zip_city = clean_text(card.select_one(PLACE_SELECTOR).get_text(" ", strip=True)) if card.select_one(PLACE_SELECTOR) else None

        out.append(
            {
                "branche": branche,
                "name": name,
                "wko_detail_url": wko_detail_url,
                "company_website": company_website,
                "email": email,
                "phone": phone,
                "street": street,
                "zip_city": zip_city,
                "source_list_url": base_url,
            }
        )
    return out, soup


def crawl_branch(session, branche: str, start_url: str):
    t0 = time.time()
    all_rows = 0
    seen_detail_urls = set()
    log(f"[{branche}] start")

    try:
        get_t0 = time.perf_counter()
        resp = fetch_with_retry(session, "GET", start_url)
        log(f"[{branche}] initial GET done in {time.perf_counter() - get_t0:.2f}s")
    except requests.RequestException as e:
        log(f"[{branche}] GET error: {e!r}")
        return all_rows
    if resp.status_code != 200:
        log(f"[{branche}] GET failed status={resp.status_code}")
        return all_rows
    html = resp.text
    current_url = resp.url or start_url

    for click_idx in range(MAX_LOAD_MORE_CLICKS + 1):
        branch_elapsed = time.time() - t0
        if branch_elapsed > MAX_SECONDS_PER_BRANCH:
            log(f"[{branche}] timeout branch after {branch_elapsed:.1f}s")
            return all_rows

        parse_t0 = time.perf_counter()
        rows, soup = extract_cards_from_html(html, branche, current_url)
        parse_took = time.perf_counter() - parse_t0
        unique_rows = []
        for row in rows:
            detail = row.get("wko_detail_url")
            if not detail or detail in seen_detail_urls:
                continue
            seen_detail_urls.add(detail)
            unique_rows.append(row)
        append_jsonl(unique_rows)
        all_rows += len(unique_rows)
        log(
            f"[{branche}] step={click_idx + 1} cards_on_page={len(rows)} new_rows={len(unique_rows)} "
            f"total_rows={all_rows} parse+write={parse_took:.2f}s elapsed={branch_elapsed:.1f}s"
        )

        if not has_load_more(soup):
            log(f"[{branche}] done: no 'Mehr laden' button")
            return all_rows
        if click_idx >= MAX_LOAD_MORE_CLICKS:
            log(f"[{branche}] stop: reached max clicks={MAX_LOAD_MORE_CLICKS}")
            return all_rows

        fields, action = parse_hidden_form_fields(soup)
        if not fields:
            dump_html(f"{branche}_missing_form", html)
            log(f"[{branche}] stop: missing form fields for postback")
            return all_rows
        fields[LOAD_MORE_NAME] = "Mehr laden"

        post_url = urljoin(current_url, action) if action else current_url
        time.sleep(CLICK_PAUSE_SECONDS)
        try:
            post_t0 = time.perf_counter()
            resp = fetch_with_retry(session, "POST", post_url, data=fields)
            log(f"[{branche}] postback took {time.perf_counter() - post_t0:.2f}s")
        except requests.RequestException as e:
            log(f"[{branche}] POST error: {e!r}")
            return all_rows
        if resp.status_code == 403:
            retry_ok = False
            for _ in range(POST_403_RETRIES):
                time.sleep(POST_403_SLEEP_SECONDS)
                try:
                    resp = fetch_with_retry(session, "POST", post_url, data=fields)
                except requests.RequestException:
                    continue
                if resp.status_code == 200:
                    retry_ok = True
                    break
            if not retry_ok:
                log(f"[{branche}] POST blocked with 403")
                return all_rows
        if resp.status_code != 200:
            log(f"[{branche}] POST failed status={resp.status_code}")
            return all_rows

        new_html = resp.text
        if new_html == html:
            log(f"[{branche}] stop: response html unchanged")
            return all_rows
        html = new_html
        current_url = resp.url or current_url

    return all_rows


def main():
    ensure_dirs()
    with open(BRANCH_MAP_JSON, "r", encoding="utf-8") as f:
        branch_map = json.load(f)

    items = list(branch_map.items())
    if ONLY_FIRST_N_BRANCHES is not None:
        items = items[:ONLY_FIRST_N_BRANCHES]

    session = make_session()
    for idx, (branche, url) in tqdm(enumerate(items, start=1), total=len(items)):
        branch_t0 = time.perf_counter()
        log(f"[{idx}/{len(items)}] {branche} -> {url}")
        try:
            wrote = crawl_branch(session, branche, url)
            log(f"[{idx}/{len(items)}] {branche}: wrote {wrote} in {time.perf_counter() - branch_t0:.1f}s")
        except Exception as e:
            log(f"[{idx}/{len(items)}] ERROR {branche}: {e!r}")
            snapshot_output(f"{branche}_branch_error")
            continue

    log(f"Done: {OUT_JSONL}")


if __name__ == "__main__":
    main()
