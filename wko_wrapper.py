#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import re
import sys
from datetime import datetime

import requests
from bs4 import BeautifulSoup

import crawler2

SEARCH_COMPLEX_URL = "https://firmen.wko.at/SearchComplex.aspx"
BRANCH_MAP_PATH = "filtered_branches_name_to_url.json"
CATALOG_OUT_PATH = os.path.join("data", "wko_catalog.json")
DEFAULT_ON_DEMAND_OUT = os.path.join("data", "out", "companies_on_demand.jsonl")


def ts():
    return datetime.now().strftime("%H:%M:%S")


def log(msg):
    print(f"[{ts()}] {msg}", flush=True)


def load_branch_map(path=BRANCH_MAP_PATH):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def fetch_searchcomplex_html():
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept-Language": "de-AT,de;q=0.9,en;q=0.8",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://firmen.wko.at/",
        }
    )
    resp = session.get(SEARCH_COMPLEX_URL, timeout=10)
    resp.raise_for_status()
    return resp.text


def extract_postback_terms(html):
    soup = BeautifulSoup(html, "lxml")
    out = []
    seen = set()

    for a in soup.select("a[href^='javascript:__doPostBack']"):
        text = a.get_text(" ", strip=True)
        href = a.get("href") or ""
        if not text:
            continue
        m = re.search(r"__doPostBack\('([^']+)'", href)
        target = m.group(1) if m else None
        if not target:
            continue
        if "searchProdukteModal" not in target and "searchBranchenModal" not in target and "searchKombinationModal" not in target:
            continue
        key = (text, target)
        if key in seen:
            continue
        seen.add(key)
        out.append({"label": text, "event_target": target})

    return out


def build_catalog():
    log("Fetching SearchComplex catalog page...")
    html = fetch_searchcomplex_html()
    postback_terms = extract_postback_terms(html)
    branch_map = load_branch_map()

    branch_terms = [{"label": k, "url": v} for k, v in branch_map.items()]
    branch_by_label = {row["label"]: row["url"] for row in branch_terms}

    for row in postback_terms:
        if row["label"] in branch_by_label:
            row["url"] = branch_by_label[row["label"]]
        else:
            row["url"] = None

    catalog = {
        "meta": {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "source": SEARCH_COMPLEX_URL,
            "branch_map_file": BRANCH_MAP_PATH,
            "postback_term_count": len(postback_terms),
            "branch_term_count": len(branch_terms),
        },
        "postback_terms": postback_terms,
        "branch_terms": branch_terms,
    }
    os.makedirs(os.path.dirname(CATALOG_OUT_PATH), exist_ok=True)
    with open(CATALOG_OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(catalog, f, ensure_ascii=False, indent=2)
    log(f"Wrote catalog: {CATALOG_OUT_PATH}")
    log(
        f"postback_terms={len(postback_terms)} branch_terms={len(branch_terms)} "
        f"with_url_in_postback={sum(1 for t in postback_terms if t.get('url'))}"
    )


def load_catalog(path=CATALOG_OUT_PATH):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def search_terms(query, limit=40):
    q = query.strip().lower()
    if not q:
        return []
    catalog = load_catalog()
    pool = []
    for row in catalog.get("branch_terms", []):
        pool.append({"kind": "branch", **row})
    for row in catalog.get("postback_terms", []):
        pool.append({"kind": "postback", **row})
    hits = [r for r in pool if q in r.get("label", "").lower()]
    hits.sort(key=lambda r: (0 if r["kind"] == "branch" else 1, r.get("label", "")))
    return hits[:limit]


def resolve_branch_url(term):
    branch_map = load_branch_map()
    if term in branch_map:
        return term, branch_map[term]

    normalized = term.strip().lower()
    exact_ci = [(k, v) for k, v in branch_map.items() if k.lower() == normalized]
    if exact_ci:
        return exact_ci[0]

    contains = [(k, v) for k, v in branch_map.items() if normalized in k.lower()]
    if len(contains) == 1:
        return contains[0]
    if len(contains) > 1:
        raise ValueError(
            "ambiguous term; candidates:\n- " + "\n- ".join(k for k, _ in contains[:20])
        )
    raise ValueError("term not found in branch map")


def crawl_on_demand(term, out_path=DEFAULT_ON_DEMAND_OUT):
    label, url = resolve_branch_url(term)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    crawler2.OUT_JSONL = out_path
    crawler2.REQUEST_TIMEOUT_SECONDS = 3
    crawler2.MAX_RETRIES = 4
    crawler2.RETRY_SLEEP_SECONDS = 0.25
    crawler2.CLICK_PAUSE_SECONDS = 0.12
    crawler2.POST_403_RETRIES = 3
    crawler2.POST_403_SLEEP_SECONDS = 0.6
    crawler2.MAX_SECONDS_PER_BRANCH = 20 * 60

    session = crawler2.make_session()
    log(f"on-demand crawl term='{label}' url={url}")
    wrote = crawler2.crawl_branch(session, label, url)
    log(f"done term='{label}' wrote={wrote} out={out_path}")


def build_cli():
    parser = argparse.ArgumentParser(
        description="WKO wrapper: discover terms, search terms, crawl on demand."
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("discover", help="Fetch SearchComplex and build local term catalog")

    p_search = sub.add_parser("search", help="Search local catalog terms")
    p_search.add_argument("query", help="Search string")
    p_search.add_argument("--limit", type=int, default=40, help="Max results")

    p_crawl = sub.add_parser("crawl", help="Crawl one term on demand")
    p_crawl.add_argument("term", help="Exact or partial branch term label")
    p_crawl.add_argument(
        "--out",
        default=DEFAULT_ON_DEMAND_OUT,
        help=f"JSONL output path (default: {DEFAULT_ON_DEMAND_OUT})",
    )
    return parser


def main():
    parser = build_cli()
    args = parser.parse_args()

    try:
        if args.cmd == "discover":
            build_catalog()
            return
        if args.cmd == "search":
            hits = search_terms(args.query, limit=args.limit)
            for h in hits:
                print(
                    json.dumps(
                        {
                            "kind": h.get("kind"),
                            "label": h.get("label"),
                            "url": h.get("url"),
                            "event_target": h.get("event_target"),
                        },
                        ensure_ascii=False,
                    )
                )
            log(f"search hits={len(hits)}")
            return
        if args.cmd == "crawl":
            crawl_on_demand(args.term, out_path=args.out)
            return
    except KeyboardInterrupt:
        log("interrupted by user")
        sys.exit(130)
    except Exception as e:
        log(f"error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
