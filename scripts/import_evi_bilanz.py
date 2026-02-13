#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import find_dotenv, load_dotenv
from supabase import Client, create_client

SCHEMA_SQL_PATH = Path(__file__).resolve().parent / "setup_evi_schema.sql"
DEFAULT_JSONL_CANDIDATES = (
    Path("data/out/evi_bilanz.jsonl"),
    Path("data/evi_bilanz.jsonl"),
)

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


def as_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower().translate(UMLAUT_TRANSLATION)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def to_iso_timestamptz(value: Any) -> str | None:
    txt = as_text(value)
    if not txt:
        return None
    try:
        dt = datetime.fromisoformat(txt.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).isoformat()
    except ValueError:
        return None


def to_iso_date(value: Any) -> str | None:
    txt = as_text(value)
    if not txt:
        return None
    if re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", txt):
        try:
            dt = datetime.strptime(txt, "%d.%m.%Y")
            return dt.date().isoformat()
        except ValueError:
            return None
    try:
        dt = datetime.fromisoformat(txt.replace("Z", "+00:00"))
        return dt.date().isoformat()
    except ValueError:
        return None


def split_sql_statements(sql_text: str) -> list[str]:
    statements: list[str] = []
    for part in sql_text.split(";"):
        stmt = part.strip()
        if not stmt:
            continue
        if stmt.startswith("--"):
            continue
        statements.append(f"{stmt};")
    return statements


def is_missing_rpc_function_error(exc: Exception) -> bool:
    message = str(exc)
    return "PGRST202" in message or "Could not find the function" in message


def try_bootstrap_schema_via_rpc(client: Client) -> bool:
    if not SCHEMA_SQL_PATH.exists():
        raise RuntimeError(f"Schema file not found: {SCHEMA_SQL_PATH}")

    statements = split_sql_statements(SCHEMA_SQL_PATH.read_text(encoding="utf-8"))
    if not statements:
        raise RuntimeError(f"Schema file is empty: {SCHEMA_SQL_PATH}")

    for rpc_name in ("exec_sql", "run_sql", "sql"):
        try:
            for stmt in statements:
                client.rpc(rpc_name, {"sql": stmt}).execute()
            print(f"Auto schema bootstrap succeeded via rpc('{rpc_name}').")
            return True
        except Exception as exc:
            if is_missing_rpc_function_error(exc):
                continue
            raise RuntimeError(f"Schema bootstrap via rpc('{rpc_name}') failed: {exc}") from exc
    return False


def ensure_evi_table_ready(client: Client) -> None:
    required = (
        "id,evi_key,publication_date,publication_type,detail_url,source_item_path,"
        "source_search_url,company_name,company_name_norm,firmenbuchnummer,search_text,"
        "crawled_at,raw_row"
    )
    try:
        client.table("evi_bilanz_publications").select(required, count="exact").limit(1).execute()
    except Exception as exc:
        message = str(exc)
        if "PGRST205" in message or "Could not find the table 'public.evi_bilanz_publications'" in message:
            print("evi_bilanz_publications table missing, attempting automatic schema bootstrap...")
            if try_bootstrap_schema_via_rpc(client):
                client.table("evi_bilanz_publications").select("id", count="exact").limit(1).execute()
                return
        raise RuntimeError(
            "Table 'evi_bilanz_publications' is missing or malformed. "
            "Run scripts/setup_evi_schema.sql in Supabase SQL Editor, then rerun."
        ) from exc


def create_client_from_env() -> Client:
    env_path = find_dotenv(usecwd=True)
    load_dotenv(env_path if env_path else None, override=False)
    url = os.getenv("SUPABASE_URL")
    service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not service_role_key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env")
    return create_client(url, service_role_key)


def locate_jsonl(explicit_path: str | None) -> Path:
    if explicit_path:
        path = Path(explicit_path).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"EVI JSONL not found: {path}")
        return path
    for candidate in DEFAULT_JSONL_CANDIDATES:
        if candidate.exists():
            return candidate.resolve()
    raise FileNotFoundError("Could not find evi_bilanz.jsonl. Pass --jsonl.")


def build_evi_key(detail_url: str | None, company_name: str | None, publication_date: str | None) -> str:
    key_material = "|".join([detail_url or "", company_name or "", publication_date or ""])
    return hashlib.sha1(key_material.encode("utf-8")).hexdigest()


def prepare_records(jsonl_path: Path) -> tuple[list[dict[str, Any]], int]:
    records: list[dict[str, Any]] = []
    skipped = 0
    with jsonl_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                skipped += 1
                continue

            detail_url = as_text(row.get("detail_url"))
            company_name = as_text(row.get("company_name"))
            publication_type = as_text(row.get("publication_type"))
            firmenbuchnummer = as_text(row.get("firmenbuchnummer"))
            publication_date = to_iso_date(row.get("publication_date"))
            crawled_at = to_iso_timestamptz(row.get("crawled_at"))
            company_name_norm = normalize_text(company_name)

            if not detail_url and not company_name:
                skipped += 1
                continue

            search_text = normalize_text(
                " ".join(
                    token
                    for token in [
                        company_name or "",
                        firmenbuchnummer or "",
                        publication_type or "",
                        publication_date or "",
                        detail_url or "",
                    ]
                    if token
                )
            )

            records.append(
                {
                    "evi_key": build_evi_key(detail_url, company_name, publication_date),
                    "publication_date": publication_date,
                    "publication_type": publication_type,
                    "detail_url": detail_url,
                    "source_item_path": as_text(row.get("source_item_path")),
                    "source_search_url": as_text(row.get("source_search_url")),
                    "company_name": company_name,
                    "company_name_norm": company_name_norm,
                    "firmenbuchnummer": firmenbuchnummer,
                    "search_text": search_text,
                    "crawled_at": crawled_at,
                    "raw_row": row,
                }
            )
    return records, skipped


def batch_upsert(client: Client, records: list[dict[str, Any]], batch_size: int) -> int:
    total = 0
    for idx in range(0, len(records), batch_size):
        batch = records[idx : idx + batch_size]
        client.table("evi_bilanz_publications").upsert(batch, on_conflict="evi_key").execute()
        total += len(batch)
        print(f"Upserted batch {idx // batch_size + 1}: {len(batch)} rows")
    return total


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import EVI Bilanz JSONL into Supabase")
    parser.add_argument("--jsonl", type=str, default=None, help="Path to evi_bilanz.jsonl")
    parser.add_argument("--batch-size", type=int, default=500, help="Rows per upsert batch")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    jsonl_path = locate_jsonl(args.jsonl)
    print(f"Using EVI JSONL: {jsonl_path}")

    client = create_client_from_env()
    ensure_evi_table_ready(client)
    print("Schema preflight passed: evi_bilanz_publications table is reachable.")

    records, skipped = prepare_records(jsonl_path)
    print(f"Rows prepared for upsert: {len(records)}")
    if skipped:
        print(f"Rows skipped (invalid JSON/empty): {skipped}")

    if records:
        upserted = batch_upsert(client, records, max(1, args.batch_size))
    else:
        upserted = 0

    print(f"Rows upserted: {upserted}")
    print("EVI import finished successfully.")


if __name__ == "__main__":
    main()
