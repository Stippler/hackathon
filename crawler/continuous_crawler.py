#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import random
import re
import sqlite3
import time
import unicodedata
from datetime import datetime, timezone
from hashlib import sha1
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from dotenv import find_dotenv, load_dotenv
from supabase import create_client

from crawler.branch_catalog import CATALOG_PATH, discover_branches
from crawler.branch_rating import RATINGS_PATH, generate_ratings

OUT_JSONL = os.path.join("data", "out", "companies_continuous.jsonl")
STATE_PATH = os.path.join("data", "crawl_state.json")
DEDUPE_DB_PATH = os.path.join("data", "out", "companies_dedupe.sqlite")

REQUEST_TIMEOUT_SECONDS = 6
MAX_RETRIES = 3
RETRY_SLEEP_SECONDS = 0.5
MAX_LOAD_MORE_CLICKS = 500
BASE_BETWEEN_REQUESTS_SECONDS = 0.15
MAX_DENIED_BACKOFF_SECONDS = 60
MAX_ERROR_BACKOFF_SECONDS = 300
SUPABASE_BATCH_SIZE = 500

CARD_SELECTOR = "article.search-result-article"
DETAIL_LINK_SELECTOR = "a.title-link[href]"
PHONE_SELECTOR = 'a[itemprop="telephone"]'
EMAIL_SELECTOR = 'a[itemprop="email"]'
WEB_SELECTOR = 'a[itemprop="url"]'
STREET_SELECTOR = ".address .street"
PLACE_SELECTOR = ".address .place"
FORM_SELECTOR = "form#aspnetForm"
LOAD_MORE_NAME = "ctl00$ContentPlaceHolder1$nextPageButton"

UMLAUT_TRANSLATION = str.maketrans(
    {
        "ä": "ae",
        "ö": "oe",
        "ü": "ue",
        "ß": "ss",
        "Ä": "ae",
        "Ö": "oe",
        "Ü": "ue",
    }
)


def _now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _now_ts():
    return datetime.now().strftime("%H:%M:%S")


def log(msg):
    print(f"[{_now_ts()}] {msg}", flush=True)


def _load_json(path, default):
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_json(path, payload):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _clean_text(s):
    if s is None:
        return None
    return " ".join(str(s).split()).strip() or None


def _norm_for_key(s):
    s = _clean_text(s) or ""
    return s.lower()


def _dedupe_key(row):
    addr = f"{row.get('street') or ''} {row.get('zip_city') or ''}".strip()
    return f"{_norm_for_key(row.get('name'))}|{_norm_for_key(addr)}"


def _normalize_text(value):
    if value is None:
        return ""
    text = str(value).strip().lower().translate(UMLAUT_TRANSLATION)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _as_text(value):
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _build_wko_key(name_norm, address_norm):
    return sha1(f"{name_norm}|{address_norm}".encode("utf-8")).hexdigest()


def _create_supabase_client_from_env():
    env_path = find_dotenv(usecwd=True)
    load_dotenv(env_path if env_path else None, override=False)
    url = os.getenv("SUPABASE_URL")
    service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not service_role_key:
        log("Supabase env vars missing; DB auto-upsert disabled.")
        return None
    return create_client(url, service_role_key)


def _ensure_wko_companies_table_ready(client):
    client.table("wko_companies").select(
        "id,wko_key,branche,name,street,zip_city,address,wko_detail_url,search_text,raw_row",
        count="exact",
    ).limit(1).execute()


def _prepare_company_rows_for_db(rows):
    payload = []
    for row in rows:
        name = _as_text(row.get("name"))
        street = _as_text(row.get("street"))
        zip_city = _as_text(row.get("zip_city"))
        address = _as_text(" ".join(x for x in [street or "", zip_city or ""] if x))
        branche = _as_text(row.get("branche"))
        wko_detail_url = _as_text(row.get("wko_detail_url"))

        name_norm = _normalize_text(name)
        address_norm = _normalize_text(address)
        if not name_norm and not address_norm:
            continue

        search_text = _normalize_text(
            " ".join(
                x
                for x in [
                    name or "",
                    branche or "",
                    address or "",
                    _as_text(row.get("company_website")) or "",
                    _as_text(row.get("email")) or "",
                    _as_text(row.get("phone")) or "",
                ]
                if x
            )
        )
        payload.append(
            {
                "wko_key": _build_wko_key(name_norm, address_norm),
                "branche": branche,
                "name": name,
                "street": street,
                "zip_city": zip_city,
                "address": address,
                "wko_detail_url": wko_detail_url,
                "company_website": _as_text(row.get("company_website")),
                "email": _as_text(row.get("email")),
                "phone": _as_text(row.get("phone")),
                "source_list_url": _as_text(row.get("source_list_url")),
                "crawled_at": _as_text(row.get("crawled_at")),
                "search_text": search_text,
                "raw_row": row,
            }
        )
    return payload


def _upsert_rows_to_supabase(client, rows):
    if not client or not rows:
        return 0
    payload = _prepare_company_rows_for_db(rows)
    total = 0
    for idx in range(0, len(payload), SUPABASE_BATCH_SIZE):
        batch = payload[idx : idx + SUPABASE_BATCH_SIZE]
        client.table("wko_companies").upsert(batch, on_conflict="wko_key").execute()
        total += len(batch)
    return total


class DedupeStore:
    def __init__(self, path):
        self.path = path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.conn = sqlite3.connect(path)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dedupe (
              dedupe_key TEXT PRIMARY KEY,
              first_seen_at TEXT NOT NULL,
              branche TEXT,
              name TEXT,
              street TEXT,
              zip_city TEXT,
              wko_detail_url TEXT
            )
            """
        )
        self.conn.commit()

    def add_if_new(self, row):
        key = _dedupe_key(row)
        try:
            self.conn.execute(
                """
                INSERT INTO dedupe (dedupe_key, first_seen_at, branche, name, street, zip_city, wko_detail_url)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    key,
                    _now_iso(),
                    row.get("branche"),
                    row.get("name"),
                    row.get("street"),
                    row.get("zip_city"),
                    row.get("wko_detail_url"),
                ),
            )
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def close(self):
        self.conn.close()


class BackoffController:
    def __init__(self):
        self.denied_streak = 0
        self.error_streak = 0
        self.last_wait = 0

    def before_request(self):
        delay = BASE_BETWEEN_REQUESTS_SECONDS + random.uniform(0.0, 0.15)
        time.sleep(delay)

    def on_success(self):
        self.denied_streak = 0
        self.error_streak = 0
        self.last_wait = 0

    def on_denied(self):
        self.denied_streak += 1
        self.error_streak = 0
        wait = min(MAX_DENIED_BACKOFF_SECONDS, 2 ** self.denied_streak)
        self.last_wait = wait
        log(f"Access denied/backoff: streak={self.denied_streak}, waiting {wait}s")
        time.sleep(wait)
        return wait

    def on_error(self, context="request"):
        self.error_streak += 1
        wait = min(MAX_ERROR_BACKOFF_SECONDS, 2 ** self.error_streak)
        # Small jitter helps avoid hammering in lockstep after repeated failures.
        wait = wait + random.uniform(0.0, 0.5)
        self.last_wait = wait
        log(f"{context} error/backoff: streak={self.error_streak}, waiting {wait:.1f}s")
        time.sleep(wait)
        return wait


def _append_jsonl(path, rows):
    if not rows:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


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


def _fetch_with_retry(session, method, url, data=None):
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        t0 = time.perf_counter()
        try:
            if method == "GET":
                resp = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
            else:
                resp = session.post(url, data=data, timeout=REQUEST_TIMEOUT_SECONDS)
            log(f"{method} {resp.status_code} try={attempt}/{MAX_RETRIES} took={time.perf_counter() - t0:.2f}s")
            return resp
        except requests.RequestException as e:
            last_exc = e
            log(f"{method} error={e.__class__.__name__} try={attempt}/{MAX_RETRIES}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SLEEP_SECONDS)
    raise last_exc


def _extract_cards_and_soup(html, branche, base_url):
    soup = BeautifulSoup(html, "lxml")
    cards = soup.select(CARD_SELECTOR)
    out = []
    for card in cards:
        detail_link = card.select_one(DETAIL_LINK_SELECTOR)
        href = detail_link.get("href") if detail_link else None
        wko_detail_url = urljoin(base_url, href) if href else None

        email = card.select_one(EMAIL_SELECTOR).get("href") if card.select_one(EMAIL_SELECTOR) else None
        if email and email.lower().startswith("mailto:"):
            email = email[7:]

        website_node = card.select_one(f"{WEB_SELECTOR} span") or card.select_one(WEB_SELECTOR)
        out.append(
            {
                "branche": branche,
                "name": _clean_text(detail_link.get_text(" ", strip=True)) if detail_link else None,
                "wko_detail_url": wko_detail_url,
                "company_website": _clean_text(website_node.get_text(" ", strip=True)) if website_node else None,
                "email": email,
                "phone": _clean_text(card.select_one(PHONE_SELECTOR).get_text(" ", strip=True)) if card.select_one(PHONE_SELECTOR) else None,
                "street": _clean_text(card.select_one(STREET_SELECTOR).get_text(" ", strip=True)) if card.select_one(STREET_SELECTOR) else None,
                "zip_city": _clean_text(card.select_one(PLACE_SELECTOR).get_text(" ", strip=True)) if card.select_one(PLACE_SELECTOR) else None,
                "source_list_url": base_url,
                "crawled_at": _now_iso(),
            }
        )
    return out, soup


def _parse_form_fields(soup):
    form = soup.select_one(FORM_SELECTOR)
    if not form:
        return None, None
    fields = {}
    for inp in form.select("input[name]"):
        inp_type = (inp.get("type") or "").lower()
        if inp_type in {"submit", "button", "image", "reset", "file"}:
            continue
        fields[inp.get("name")] = inp.get("value", "")
    return fields, form.get("action") or ""


def _has_load_more(soup):
    return soup.select_one(f"input[name='{LOAD_MORE_NAME}'], input[name$='nextPageButton']") is not None


def crawl_branch(session, backoff, dedupe_store, branche, start_url, out_jsonl=OUT_JSONL, supabase_client=None):
    inserted = 0
    load_more_steps = 0
    access_denied = False
    t0 = time.perf_counter()

    backoff.before_request()
    try:
        resp = _fetch_with_retry(session, "GET", start_url)
    except requests.RequestException as exc:
        wait = backoff.on_error(context=f"[{branche}] GET")
        log(f"[{branche}] branch_start_error={exc.__class__.__name__}: {exc}")
        return {
            "inserted": 0,
            "steps": 0,
            "access_denied": False,
            "transient_error": True,
            "waited_s": wait,
            "duration_s": time.perf_counter() - t0,
        }
    body = resp.text or ""
    if resp.status_code == 403 or "access denied" in body.lower():
        access_denied = True
        wait = backoff.on_denied()
        return {"inserted": 0, "steps": 0, "access_denied": True, "waited_s": wait, "duration_s": time.perf_counter() - t0}
    backoff.on_success()

    html = body
    current_url = resp.url or start_url
    for _ in range(MAX_LOAD_MORE_CLICKS + 1):
        rows, soup = _extract_cards_and_soup(html, branche, current_url)
        new_rows = []
        for row in rows:
            if dedupe_store.add_if_new(row):
                new_rows.append(row)
        _append_jsonl(out_jsonl, new_rows)
        if new_rows and supabase_client:
            try:
                db_upserts = _upsert_rows_to_supabase(supabase_client, new_rows)
                log(f"[{branche}] db_upserts={db_upserts}")
            except Exception as exc:
                log(f"[{branche}] db_upsert_error={exc.__class__.__name__}: {exc}")
        inserted += len(new_rows)
        load_more_steps += 1
        log(f"[{branche}] step={load_more_steps} page_rows={len(rows)} new={len(new_rows)} total_new={inserted}")

        if not _has_load_more(soup):
            break

        fields, action = _parse_form_fields(soup)
        if not fields:
            break
        fields[LOAD_MORE_NAME] = "Mehr laden"
        post_url = urljoin(current_url, action) if action else current_url

        backoff.before_request()
        try:
            resp = _fetch_with_retry(session, "POST", post_url, data=fields)
        except requests.RequestException as exc:
            wait = backoff.on_error(context=f"[{branche}] POST")
            log(f"[{branche}] load_more_error={exc.__class__.__name__}: {exc}")
            return {
                "inserted": inserted,
                "steps": load_more_steps,
                "access_denied": False,
                "transient_error": True,
                "waited_s": wait,
                "duration_s": time.perf_counter() - t0,
            }
        body = resp.text or ""
        if resp.status_code == 403 or "access denied" in body.lower():
            access_denied = True
            wait = backoff.on_denied()
            return {
                "inserted": inserted,
                "steps": load_more_steps,
                "access_denied": True,
                "waited_s": wait,
                "duration_s": time.perf_counter() - t0,
            }
        backoff.on_success()
        if body == html:
            break
        html = body
        current_url = resp.url or current_url

    return {
        "inserted": inserted,
        "steps": load_more_steps,
        "access_denied": access_denied,
        "transient_error": False,
        "waited_s": 0,
        "duration_s": time.perf_counter() - t0,
    }


def _select_next_branch(state, ratings):
    now = datetime.now(timezone.utc)
    branches_state = state.setdefault("branches", {})
    for row in ratings:
        branche = row["branche"]
        stats = branches_state.get(branche, {})
        next_allowed = stats.get("next_allowed_at")
        if next_allowed:
            try:
                next_dt = datetime.fromisoformat(next_allowed.replace("Z", "+00:00"))
                if next_dt > now:
                    continue
            except ValueError:
                pass
        return row
    return None


def run_continuous(max_cycles=None):
    if not os.path.exists(CATALOG_PATH):
        discover_branches()

    dedupe_store = DedupeStore(DEDUPE_DB_PATH)
    session = _make_session()
    backoff = BackoffController()
    supabase_client = _create_supabase_client_from_env()
    if supabase_client:
        try:
            _ensure_wko_companies_table_ready(supabase_client)
            log("Supabase auto-upsert enabled (wko_companies reachable).")
        except Exception as exc:
            log(f"Supabase preflight failed; DB auto-upsert disabled. error={exc}")
            supabase_client = None
    state = _load_json(STATE_PATH, {"meta": {"created_at": _now_iso()}, "branches": {}})
    branches_state = state["branches"]

    cycle = 0
    loop_error_streak = 0
    try:
        while True:
            try:
                cycle += 1
                ratings_payload = generate_ratings(catalog_path=CATALOG_PATH, state_path=STATE_PATH, out_path=RATINGS_PATH)
                ratings = ratings_payload["ratings"]
                if not ratings:
                    log("No branches available. Sleeping 30s.")
                    time.sleep(30)
                    continue

                selected = _select_next_branch(state, ratings)
                if not selected:
                    log("All branches are cooling down. Sleeping 20s.")
                    time.sleep(20)
                    continue

                branche, url = selected["branche"], selected["url"]
                log(f"[cycle={cycle}] crawling branche='{branche}' score={selected['score']}")
                result = crawl_branch(session, backoff, dedupe_store, branche, url, OUT_JSONL, supabase_client=supabase_client)

                st = branches_state.setdefault(branche, {})
                st["crawl_count"] = int(st.get("crawl_count", 0)) + 1
                st["last_rows"] = int(result["inserted"])
                st["total_rows_inserted"] = int(st.get("total_rows_inserted", 0)) + int(result["inserted"])
                st["last_steps"] = int(result["steps"])
                st["last_duration_s"] = round(float(result["duration_s"]), 2)
                st["last_crawled_at"] = _now_iso()

                if result["access_denied"]:
                    st["access_denied_count"] = int(st.get("access_denied_count", 0)) + 1
                    wait_s = float(result["waited_s"] or 10)
                    st["next_allowed_at"] = datetime.fromtimestamp(time.time() + wait_s, tz=timezone.utc).isoformat().replace("+00:00", "Z")
                elif result.get("transient_error"):
                    st["error_count"] = int(st.get("error_count", 0)) + 1
                    wait_s = float(result.get("waited_s") or 10)
                    st["next_allowed_at"] = datetime.fromtimestamp(time.time() + wait_s, tz=timezone.utc).isoformat().replace("+00:00", "Z")
                else:
                    st["next_allowed_at"] = None

                state["meta"]["updated_at"] = _now_iso()
                _save_json(STATE_PATH, state)
                log(
                    f"[cycle={cycle}] done branche='{branche}' new={result['inserted']} "
                    f"steps={result['steps']} denied={result['access_denied']} "
                    f"transient_error={result.get('transient_error', False)} duration={result['duration_s']:.1f}s"
                )
                loop_error_streak = 0

                if max_cycles is not None and cycle >= max_cycles:
                    log(f"Reached max cycles={max_cycles}.")
                    break
            except Exception as exc:
                loop_error_streak += 1
                wait_s = min(MAX_ERROR_BACKOFF_SECONDS, 2 ** min(loop_error_streak, 10))
                wait_s = wait_s + random.uniform(0.0, 0.5)
                log(f"[cycle={cycle}] unexpected_error={exc.__class__.__name__}: {exc}")
                log(f"[cycle={cycle}] global recovery backoff {wait_s:.1f}s")
                state["meta"]["updated_at"] = _now_iso()
                _save_json(STATE_PATH, state)
                time.sleep(wait_s)
                continue
    finally:
        dedupe_store.close()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Adaptive continuous WKO crawler.")
    parser.add_argument("--max-cycles", type=int, default=None, help="Stop after N branch cycles (default: endless).")
    args = parser.parse_args()
    run_continuous(max_cycles=args.max_cycles)


if __name__ == "__main__":
    main()

