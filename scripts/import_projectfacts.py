#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import os
import re
import unicodedata
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import find_dotenv, load_dotenv
from supabase import Client, create_client

REQUIRED_CANONICAL_FIELDS = ("name", "street", "plz", "city", "country")

COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "name": ("Name",),
    "ort": ("Ort",),
    "last_changed": ("Letzte Änderung",),
    "segment_country": ("Land (Kundensegmentierung)",),
    "last_activity": ("Letzter Vorgang",),
    "industries": ("Branchen",),
    "size": ("Größe",),
    "street": ("Straße / Nr.",),
    "plz": ("PLZ",),
    "city": ("Stadt",),
    "state": ("Bundesland",),
    "country": ("Land",),
    "company_address": ("Firmenadresse", "Firmenaddresse"),
    "raw_addresses": ("Adressen",),
}

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
SCHEMA_SQL_PATH = Path(__file__).resolve().parent / "setup_projectfacts_schema.sql"

def normalize_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).replace("_x000D_", "\n").strip().lower().translate(UMLAUT_TRANSLATION)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def as_text(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, float) and value.is_integer():
        cleaned = str(int(value))
    else:
        cleaned = str(value)
    cleaned = cleaned.replace("_x000D_", "\n").strip()
    return cleaned if cleaned else None


def to_iso_timestamptz(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    ts = pd.to_datetime(value, errors="coerce", dayfirst=True, utc=True)
    if pd.isna(ts):
        return None
    return ts.isoformat()


def build_address_norm(street: str, plz: str, city: str, country: str) -> str:
    parts = [street, plz, city, country]
    return normalize_text(" ".join(part for part in parts if part))


def build_pf_key(name_norm: str, address_norm: str) -> str:
    digest_input = f"{name_norm}|{address_norm}".encode("utf-8")
    return hashlib.sha1(digest_input).hexdigest()


def uniquify_pf_key(base_pf_key: str, occurrence: int) -> str:
    if occurrence == 1:
        return base_pf_key
    digest_input = f"{base_pf_key}|dup:{occurrence}".encode("utf-8")
    return hashlib.sha1(digest_input).hexdigest()


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

    sql_text = SCHEMA_SQL_PATH.read_text(encoding="utf-8")
    statements = split_sql_statements(sql_text)
    if not statements:
        raise RuntimeError(f"Schema file is empty: {SCHEMA_SQL_PATH}")

    rpc_candidates = ("exec_sql", "run_sql", "sql")
    for rpc_name in rpc_candidates:
        try:
            for statement in statements:
                client.rpc(rpc_name, {"sql": statement}).execute()
            print(f"Auto schema bootstrap succeeded via rpc('{rpc_name}').")
            return True
        except Exception as exc:
            if is_missing_rpc_function_error(exc):
                continue
            raise RuntimeError(
                f"Schema bootstrap via rpc('{rpc_name}') failed: {exc}"
            ) from exc
    return False


def resolve_columns(df: pd.DataFrame) -> dict[str, str]:
    resolved: dict[str, str] = {}
    for canonical, aliases in COLUMN_ALIASES.items():
        match = next((alias for alias in aliases if alias in df.columns), None)
        if match:
            resolved[canonical] = match
    missing_required = [name for name in REQUIRED_CANONICAL_FIELDS if name not in resolved]
    if missing_required:
        raise ValueError(
            f"Missing required Excel columns for fields: {', '.join(missing_required)}. "
            f"Available columns: {', '.join(map(str, df.columns))}"
        )
    return resolved


def ensure_projectfacts_table_ready(client: Client) -> None:
    required_columns = (
        "id,pf_key,name,name_norm,street,plz,city,city_norm,country,ort,state,"
        "segment_country,industries,size,last_changed_at,last_activity_at,"
        "company_address,raw_addresses,address_norm,search_text,raw_row"
    )
    try:
        client.table("projectfacts").select(required_columns, count="exact").limit(1).execute()
    except Exception as exc:
        message = str(exc)
        if "PGRST205" in message or "Could not find the table 'public.projectfacts'" in message:
            print("projectfacts table missing, attempting automatic schema bootstrap...")
            bootstrapped = try_bootstrap_schema_via_rpc(client)
            if bootstrapped:
                client.table("projectfacts").select(required_columns, count="exact").limit(1).execute()
                return
        raise RuntimeError(
            "Table 'projectfacts' is missing or does not have the required columns. "
            "Automatic bootstrap could not run because no SQL RPC function is exposed. "
            "Run scripts/setup_projectfacts_schema.sql once in Supabase SQL Editor, then rerun."
        ) from exc


def locate_excel(explicit_path: str | None = None) -> Path:
    if explicit_path:
        path = Path(explicit_path).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"Excel file not found: {path}")
        return path

    candidates = [
        Path("data/projectfacts.xlsx"),
        Path("data/out/projectfacts.xlsx"),
        Path(__file__).resolve().parent.parent / "projectfacts.xlsx",
        Path(__file__).resolve().parent.parent / "data" / "projectfacts.xlsx",
        Path(__file__).resolve().parent.parent / "data" / "out" / "projectfacts.xlsx",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()

    raise FileNotFoundError(
        "Could not find projectfacts.xlsx. Place it in repository root or data/, "
        "or pass --excel-path."
    )


def prepare_records(df: pd.DataFrame) -> tuple[list[dict[str, Any]], int]:
    columns = resolve_columns(df)
    records: list[dict[str, Any]] = []
    skipped = 0
    pf_key_occurrences: dict[str, int] = {}

    for _, row in df.iterrows():
        get = lambda key: row[columns[key]] if key in columns else None
        name = as_text(get("name"))
        ort = as_text(get("ort"))
        street = as_text(get("street"))
        plz = as_text(get("plz"))
        city = as_text(get("city"))
        state = as_text(get("state"))
        country = as_text(get("country"))
        segment_country = as_text(get("segment_country"))
        industries = as_text(get("industries"))
        size = as_text(get("size"))
        company_address = as_text(get("company_address"))
        raw_addresses = as_text(get("raw_addresses"))
        last_changed_at = to_iso_timestamptz(get("last_changed"))
        last_activity_at = to_iso_timestamptz(get("last_activity"))

        name_norm = normalize_text(name)
        city_norm = normalize_text(city)
        address_norm = build_address_norm(street or "", plz or "", city or "", country or "")
        if not name_norm and not address_norm:
            skipped += 1
            continue

        company_address_norm = normalize_text(company_address)
        raw_addresses_norm = normalize_text(raw_addresses)
        pf_key_base = hashlib.sha1(
            f"{name_norm}|{address_norm}|{company_address_norm}|{raw_addresses_norm}".encode("utf-8")
        ).hexdigest()
        occurrence = pf_key_occurrences.get(pf_key_base, 0) + 1
        pf_key_occurrences[pf_key_base] = occurrence
        pf_key = uniquify_pf_key(pf_key_base, occurrence)
        search_text = normalize_text(
            " ".join(
                value
                for value in [
                    name or "",
                    street or "",
                    plz or "",
                    city or "",
                    country or "",
                    industries or "",
                    size or "",
                    company_address or "",
                    raw_addresses or "",
                    ort or "",
                    state or "",
                ]
                if value
            )
        )

        raw_row = {
            str(col): (None if pd.isna(row[col]) else str(row[col]).replace("_x000D_", "\n"))
            for col in df.columns
        }

        records.append(
            {
                "pf_key": pf_key,
                "name": name,
                "ort": ort,
                "name_norm": name_norm,
                "street": street,
                "plz": plz,
                "city": city,
                "city_norm": city_norm,
                "state": state,
                "country": country,
                "segment_country": segment_country,
                "industries": industries,
                "size": size,
                "last_changed_at": last_changed_at,
                "last_activity_at": last_activity_at,
                "company_address": company_address,
                "raw_addresses": raw_addresses,
                "address_norm": address_norm,
                "search_text": search_text,
                "raw_row": raw_row,
            }
        )

    return records, skipped


def batch_upsert(client: Client, records: list[dict[str, Any]], batch_size: int) -> int:
    total = 0
    for idx in range(0, len(records), batch_size):
        batch = records[idx : idx + batch_size]
        client.table("projectfacts").upsert(batch, on_conflict="pf_key").execute()
        total += len(batch)
        print(f"Upserted batch {idx // batch_size + 1}: {len(batch)} rows")
    return total


def replace_all_rows(client: Client) -> int:
    count_resp = client.table("projectfacts").select("id", count="exact").limit(1).execute()
    existing_count = count_resp.count or 0
    if existing_count == 0:
        return 0
    # Delete all rows safely without SQL.
    client.table("projectfacts").delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
    return existing_count


def create_client_from_env() -> Client:
    env_path = find_dotenv(usecwd=True)
    load_dotenv(env_path if env_path else None, override=False)
    url = os.getenv("SUPABASE_URL")
    service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not service_role_key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env")
    return create_client(url, service_role_key)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import projectfacts.xlsx into Supabase")
    parser.add_argument("--excel-path", type=str, default=None, help="Path to projectfacts.xlsx")
    parser.add_argument("--batch-size", type=int, default=500, help="Rows per upsert batch")
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append/upsert without deleting existing projectfacts rows first",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    excel_path = locate_excel(args.excel_path)
    print(f"Using Excel file: {excel_path}")

    client = create_client_from_env()
    ensure_projectfacts_table_ready(client)
    print("Schema preflight passed: projectfacts table is reachable.")

    df = pd.read_excel(excel_path, engine="openpyxl")
    df = df.rename(columns=lambda c: str(c).strip())
    resolve_columns(df)
    print(f"Rows loaded from Excel: {len(df)}")

    records, skipped = prepare_records(df)
    print(f"Rows prepared for upsert: {len(records)}")
    if skipped:
        print(f"Rows skipped (empty name and address): {skipped}")

    if not args.append:
        deleted = replace_all_rows(client)
        print(f"Existing rows deleted for full refresh: {deleted}")

    if records:
        upserted = batch_upsert(client, records, max(1, args.batch_size))
    else:
        upserted = 0

    print(f"Rows upserted: {upserted}")
    print("Import finished successfully.")


if __name__ == "__main__":
    main()
