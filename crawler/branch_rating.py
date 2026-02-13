#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import math
import os
from datetime import datetime, timezone

CATALOG_PATH = os.path.join("data", "wko_branch_catalog.json")
STATE_PATH = os.path.join("data", "crawl_state.json")
RATINGS_PATH = os.path.join("data", "wko_branch_ratings.json")

# High-level relevance hints. Tune as needed.
KEYWORD_WEIGHTS = {
    "industrie": 2.0,
    "großhandel": 1.8,
    "grosshandel": 1.8,
    "handel": 1.2,
    "maschinen": 1.3,
    "technik": 1.3,
    "pharma": 2.0,
    "chemie": 1.5,
    "metall": 1.4,
    "elektro": 1.4,
    "it": 1.2,
    "software": 1.2,
    "medizin": 1.4,
    "energie": 1.4,
    "transport": 1.2,
    "bau": 1.1,
}


def _now_utc():
    return datetime.now(timezone.utc)


def _parse_iso(ts):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None


def _days_since(ts):
    dt = _parse_iso(ts)
    if not dt:
        return 365.0
    delta = _now_utc() - dt
    return max(0.0, delta.total_seconds() / 86400.0)


def _text_score(branche):
    text = (branche or "").lower()
    score = 1.0
    for key, weight in KEYWORD_WEIGHTS.items():
        if key in text:
            score += weight
    return score


def _priority_score(branche, stats):
    text = _text_score(branche)
    crawl_count = max(0, int(stats.get("crawl_count", 0)))
    last_days = _days_since(stats.get("last_crawled_at"))
    last_rows = max(0, int(stats.get("last_rows", 0)))

    # Prefer rarely crawled branches and ones that produced rows last time.
    freshness_boost = min(3.0, math.log1p(last_days))
    rarity_boost = 1.0 / (1.0 + crawl_count * 0.35)
    yield_boost = min(2.0, math.log1p(last_rows) / 3.0)
    denied_penalty = min(0.7, 0.1 * int(stats.get("access_denied_count", 0)))

    return max(0.05, text * (1.0 + freshness_boost + yield_boost) * rarity_boost - denied_penalty)


def generate_ratings(
    catalog_path: str = CATALOG_PATH,
    state_path: str = STATE_PATH,
    out_path: str = RATINGS_PATH,
):
    with open(catalog_path, "r", encoding="utf-8") as f:
        catalog = json.load(f)
    state = {}
    if os.path.exists(state_path):
        with open(state_path, "r", encoding="utf-8") as f:
            state = json.load(f)

    rows = []
    branch_state = state.get("branches", {})
    for item in catalog.get("branches", []):
        branche = item["branche"]
        stats = branch_state.get(branche, {})
        score = _priority_score(branche, stats)
        rows.append(
            {
                "branche": branche,
                "url": item["url"],
                "score": round(score, 4),
                "crawl_count": int(stats.get("crawl_count", 0)),
                "last_rows": int(stats.get("last_rows", 0)),
                "last_crawled_at": stats.get("last_crawled_at"),
            }
        )

    rows.sort(key=lambda x: x["score"], reverse=True)
    payload = {
        "meta": {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "count": len(rows),
            "catalog_path": catalog_path,
            "state_path": state_path,
        },
        "ratings": rows,
    }

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return payload


def main():
    payload = generate_ratings()
    print(f"Generated {payload['meta']['count']} ratings -> {RATINGS_PATH}", flush=True)


if __name__ == "__main__":
    main()

