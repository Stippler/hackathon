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

SCHEMA_SQL_PATH = Path(__file__).resolve().parent / "setup_wko_schema.sql"
DEFAULT_COMPANY_JSONL_CANDIDATES = (
    Path("data/out/companies_continuous.jsonl"),
    Path("data/out/companies.jsonl"),
    Path("data/out/companies_on_demand.jsonl"),
)
DEFAULT_BRANCH_CATALOG = Path("data/wko_branch_catalog.json")

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


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower().translate(UMLAUT_TRANSLATION)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def as_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def to_iso_timestamptz(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        txt = value.strip()
        if not txt:
            return None
        try:
            dt = datetime.fromisoformat(txt.replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc).isoformat()
        except ValueError:
            return None
    return None


def split_sql_statements(sql_text: str) -> list[str]:
    statements: list[str] = []
    for part in sql_text.split(";"):
        statement = part.strip()
        if not statement:
            continue
        if statement.startswith("--"):
            continue
        statements.append(f"{statement};")
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


def ensure_wko_tables_ready(client: Client) -> None:
    try:
        client.table("wko_companies").select(
            "id,wko_key,branche,name,street,zip_city,address,wko_detail_url,search_text,raw_row",
            count="exact",
        ).limit(1).execute()
        client.table("wko_branches").select(
            "id,branche,branch_url,letter,source,discovered_at",
            count="exact",
        ).limit(1).execute()
    except Exception as exc:
        message = str(exc)
        if "PGRST205" in message or "Could not find the table 'public.wko_companies'" in message:
            print("wko tables missing, attempting automatic schema bootstrap...")
            if try_bootstrap_schema_via_rpc(client):
                client.table("wko_companies").select("id", count="exact").limit(1).execute()
                client.table("wko_branches").select("id", count="exact").limit(1).execute()
                return
        raise RuntimeError(
            "Tables 'wko_companies' / 'wko_branches' are missing or malformed. "
            "Run scripts/setup_wko_schema.sql in Supabase SQL Editor, then rerun."
        ) from exc


def create_client_from_env() -> Client:
    env_path = find_dotenv(usecwd=True)
    load_dotenv(env_path if env_path else None, override=False)
    url = os.getenv("SUPABASE_URL")
    service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not service_role_key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env")
    return create_client(url, service_role_key)


def locate_company_jsonl(explicit_path: str | None) -> Path:
    if explicit_path:
        path = Path(explicit_path).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"Companies JSONL not found: {path}")
        return path
    for candidate in DEFAULT_COMPANY_JSONL_CANDIDATES:
        if candidate.exists():
            return candidate.resolve()
    raise FileNotFoundError(
        "Could not find companies JSONL. Pass --companies-jsonl or create one in data/out/."
    )


def locate_branch_catalog(explicit_path: str | None) -> Path | None:
    if explicit_path:
        path = Path(explicit_path).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"Branch catalog not found: {path}")
        return path
    if DEFAULT_BRANCH_CATALOG.exists():
        return DEFAULT_BRANCH_CATALOG.resolve()
    return None


def build_wko_key(name_norm: str, address_norm: str) -> str:
    return hashlib.sha1(f"{name_norm}|{address_norm}".encode("utf-8")).hexdigest()


def prepare_company_records(companies_jsonl_path: Path) -> tuple[list[dict[str, Any]], int]:
    records: list[dict[str, Any]] = []
    skipped = 0

    with companies_jsonl_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                skipped += 1
                continue

            name = as_text(row.get("name"))
            street = as_text(row.get("street"))
            zip_city = as_text(row.get("zip_city"))
            address = as_text(" ".join(x for x in [street or "", zip_city or ""] if x))
            branche = as_text(row.get("branche"))
            wko_detail_url = as_text(row.get("wko_detail_url"))

            name_norm = normalize_text(name)
            address_norm = normalize_text(address)
            if not name_norm and not address_norm:
                skipped += 1
                continue

            search_text = normalize_text(
                " ".join(
                    x
                    for x in [
                        name or "",
                        branche or "",
                        address or "",
                        as_text(row.get("company_website")) or "",
                        as_text(row.get("email")) or "",
                        as_text(row.get("phone")) or "",
                    ]
                    if x
                )
            )
            wko_key = build_wko_key(name_norm, address_norm)

            records.append(
                {
                    "wko_key": wko_key,
                    "branche": branche,
                    "name": name,
                    "street": street,
                    "zip_city": zip_city,
                    "address": address,
                    "wko_detail_url": wko_detail_url,
                    "company_website": as_text(row.get("company_website")),
                    "email": as_text(row.get("email")),
                    "phone": as_text(row.get("phone")),
                    "source_list_url": as_text(row.get("source_list_url")),
                    "crawled_at": to_iso_timestamptz(row.get("crawled_at")),
                    "search_text": search_text,
                    "raw_row": row,
                }
            )
    return records, skipped


def prepare_branch_records(branch_catalog_path: Path) -> list[dict[str, Any]]:
    payload = json.loads(branch_catalog_path.read_text(encoding="utf-8"))
    source = as_text(payload.get("meta", {}).get("source"))
    discovered_at = to_iso_timestamptz(payload.get("meta", {}).get("generated_at"))
    out: list[dict[str, Any]] = []
    for row in payload.get("branches", []):
        branche = as_text(row.get("branche"))
        branch_url = as_text(row.get("url"))
        if not branche or not branch_url:
            continue
        out.append(
            {
                "branche": branche,
                "branch_url": branch_url,
                "letter": as_text(row.get("letter")),
                "source": source,
                "discovered_at": discovered_at,
            }
        )
    return out


def batch_upsert(client: Client, table: str, records: list[dict[str, Any]], on_conflict: str, batch_size: int) -> int:
    total = 0
    for idx in range(0, len(records), batch_size):
        batch = records[idx : idx + batch_size]
        client.table(table).upsert(batch, on_conflict=on_conflict).execute()
        total += len(batch)
        print(f"{table}: upserted batch {idx // batch_size + 1} ({len(batch)} rows)")
    return total


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import WKO branch/company data into Supabase")
    parser.add_argument(
        "--companies-jsonl",
        type=str,
        default=None,
        help="Path to companies JSONL (defaults to data/out/companies_continuous.jsonl etc.)",
    )
    parser.add_argument(
        "--branch-catalog",
        type=str,
        default=None,
        help="Path to wko_branch_catalog.json (defaults to data/wko_branch_catalog.json if present)",
    )
    parser.add_argument("--batch-size", type=int, default=500, help="Rows per upsert batch")
    parser.add_argument(
        "--companies-only",
        action="store_true",
        help="Import only wko_companies and skip wko_branches",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    companies_jsonl = locate_company_jsonl(args.companies_jsonl)
    branch_catalog = locate_branch_catalog(args.branch_catalog)
    print(f"Using companies JSONL: {companies_jsonl}")
    if branch_catalog:
        print(f"Using branch catalog: {branch_catalog}")
    else:
        print("No branch catalog found; branch import will be skipped.")

    client = create_client_from_env()
    ensure_wko_tables_ready(client)
    print("Schema preflight passed: wko tables are reachable.")

    company_records, skipped = prepare_company_records(companies_jsonl)
    print(f"Company rows prepared for upsert: {len(company_records)}")
    if skipped:
        print(f"Company rows skipped (invalid/empty): {skipped}")

    if company_records:
        upserted_companies = batch_upsert(
            client,
            table="wko_companies",
            records=company_records,
            on_conflict="wko_key",
            batch_size=max(1, args.batch_size),
        )
    else:
        upserted_companies = 0

    upserted_branches = 0
    if not args.companies_only and branch_catalog:
        branch_records = prepare_branch_records(branch_catalog)
        print(f"Branch rows prepared for upsert: {len(branch_records)}")
        if branch_records:
            upserted_branches = batch_upsert(
                client,
                table="wko_branches",
                records=branch_records,
                on_conflict="branche,branch_url",
                batch_size=max(1, args.batch_size),
            )

    print(f"Rows upserted into wko_companies: {upserted_companies}")
    print(f"Rows upserted into wko_branches: {upserted_branches}")
    print("WKO import finished successfully.")


if __name__ == "__main__":
    main()

