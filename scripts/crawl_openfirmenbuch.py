#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import random
import re
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from dotenv import find_dotenv, load_dotenv
from supabase import Client, create_client

SCHEMA_SQL_PATH = Path(__file__).resolve().parent / "setup_openfirmenbuch.sql"
OPENFIRMENBUCH_BASE_URL = "https://api.openfirmenbuch.at"
OPENFIRMENBUCH_TIMEOUT_SECONDS = int(os.getenv("OPENFIRMENBUCH_TIMEOUT_SECONDS", "30"))


def now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def today_iso() -> str:
    return dt.date.today().isoformat()


def as_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def clean_name(value: Any) -> str:
    text = as_text(value) or ""
    text = text.lower()
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_fnr(value: Any) -> str:
    text = as_text(value) or ""
    text = text.replace(" ", "")
    return text.lower()


def to_date_iso(value: Any) -> Optional[str]:
    txt = as_text(value)
    if not txt:
        return None
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", txt):
        return txt
    if re.fullmatch(r"\d{8}", txt):
        return f"{txt[:4]}-{txt[4:6]}-{txt[6:8]}"
    try:
        parsed = dt.datetime.fromisoformat(txt.replace("Z", "+00:00"))
        return parsed.date().isoformat()
    except ValueError:
        return None


def to_timestamptz_iso(value: Any) -> Optional[str]:
    txt = as_text(value)
    if not txt:
        return None
    try:
        parsed = dt.datetime.fromisoformat(txt.replace("Z", "+00:00"))
        return parsed.astimezone(dt.timezone.utc).isoformat()
    except ValueError:
        return None


def split_sql_statements(sql_text: str) -> List[str]:
    statements: List[str] = []
    for part in sql_text.split(";"):
        stmt = part.strip()
        if not stmt or stmt.startswith("--"):
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


def ensure_openfirmenbuch_tables_ready(client: Client) -> None:
    required_checks = [
        ("ofb_crawl_queue", "id,source_system,source_key,search_name,status,next_run_at"),
        ("ofb_companies", "id,firmennummer,court_code,final_status,last_seen_at"),
        ("ofb_auszug_snapshots", "id,firmennummer,stichtag,umfang,pruefsumme"),
        ("ofb_financial_years", "id,firmennummer,gj_beginn,gj_ende"),
    ]
    try:
        for table, columns in required_checks:
            client.table(table).select(columns, count="exact").limit(1).execute()
    except Exception as exc:
        msg = str(exc)
        if "PGRST205" in msg or "Could not find the table 'public.ofb_crawl_queue'" in msg:
            print("OpenFirmenbuch tables missing, attempting automatic schema bootstrap...")
            if try_bootstrap_schema_via_rpc(client):
                for table, _ in required_checks:
                    client.table(table).select("id", count="exact").limit(1).execute()
                return
        raise RuntimeError(
            "OpenFirmenbuch tables are missing or malformed. "
            "Run scripts/setup_openfirmenbuch.sql in Supabase SQL Editor, then rerun."
        ) from exc


def create_client_from_env() -> Client:
    env_path = find_dotenv(usecwd=True)
    load_dotenv(env_path if env_path else None, override=False)
    url = os.getenv("SUPABASE_URL")
    service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not service_role_key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env")
    return create_client(url, service_role_key)


def ofb_post_json(path: str, payload: Dict[str, Any]) -> Any:
    url = f"{OPENFIRMENBUCH_BASE_URL}{path}"
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url=url,
        data=body,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=OPENFIRMENBUCH_TIMEOUT_SECONDS) as response:
        raw = response.read().decode("utf-8")
    return json.loads(raw)


def fetch_rows_paginated(client: Client, table: str, columns: str, limit: int, max_rows: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    offset = 0
    while len(out) < max_rows:
        end = offset + limit - 1
        resp = client.table(table).select(columns).range(offset, end).execute()
        rows = getattr(resp, "data", None) or []
        if not rows:
            break
        out.extend(rows)
        if len(rows) < limit:
            break
        offset += limit
    return out[:max_rows]


def queue_seed_rows(client: Client, rows: Iterable[Dict[str, Any]]) -> int:
    payload = [row for row in rows if as_text(row.get("source_key")) and as_text(row.get("search_name"))]
    if not payload:
        return 0
    client.table("ofb_crawl_queue").upsert(payload, on_conflict="source_system,source_key").execute()
    return len(payload)


def seed_queue_from_sources(client: Client, max_rows_per_source: int = 5000) -> Dict[str, int]:
    result = {"wko": 0, "evi": 0, "projectfacts": 0}

    wko_rows = fetch_rows_paginated(
        client,
        "wko_companies",
        "wko_key,name",
        limit=1000,
        max_rows=max_rows_per_source,
    )
    wko_payload = [
        {
            "source_system": "wko",
            "source_key": as_text(row.get("wko_key")) or hashlib.sha1(clean_name(row.get("name")).encode("utf-8")).hexdigest(),
            "source_name": as_text(row.get("name")),
            "search_name": as_text(row.get("name")),
            "search_name_norm": clean_name(row.get("name")),
            "priority": 200,
            "status": "pending",
        }
        for row in wko_rows
        if as_text(row.get("name"))
    ]
    result["wko"] = queue_seed_rows(client, wko_payload)

    evi_rows = fetch_rows_paginated(
        client,
        "evi_bilanz_publications",
        "evi_key,company_name,firmenbuchnummer",
        limit=1000,
        max_rows=max_rows_per_source,
    )
    evi_payload = []
    for row in evi_rows:
        source_key = as_text(row.get("evi_key"))
        source_name = as_text(row.get("company_name"))
        fnr = normalize_fnr(row.get("firmenbuchnummer"))
        if not source_name and not fnr:
            continue
        if not source_key:
            source_key = hashlib.sha1(f"{source_name or ''}|{fnr}".encode("utf-8")).hexdigest()
        evi_payload.append(
            {
                "source_system": "evi",
                "source_key": source_key,
                "source_name": source_name,
                "search_name": source_name or fnr,
                "search_name_norm": clean_name(source_name or fnr),
                "firmennummer": fnr or None,
                "priority": 100,
                "status": "pending",
            }
        )
    result["evi"] = queue_seed_rows(client, evi_payload)

    pf_rows = fetch_rows_paginated(
        client,
        "projectfacts",
        "pf_key,name",
        limit=1000,
        max_rows=max_rows_per_source,
    )
    pf_payload = [
        {
            "source_system": "projectfacts",
            "source_key": as_text(row.get("pf_key")) or hashlib.sha1(clean_name(row.get("name")).encode("utf-8")).hexdigest(),
            "source_name": as_text(row.get("name")),
            "search_name": as_text(row.get("name")),
            "search_name_norm": clean_name(row.get("name")),
            "priority": 150,
            "status": "pending",
        }
        for row in pf_rows
        if as_text(row.get("name"))
    ]
    result["projectfacts"] = queue_seed_rows(client, pf_payload)
    return result


def pick_best_search_result(results: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    def score(row: Dict[str, Any]) -> Tuple[int, int]:
        status = as_text(row.get("finalStatus")) or ""
        active_score = 2
        if "gel" in status.lower():
            active_score = 0
        elif "histor" in status.lower():
            active_score = 1
        has_seat = 1 if as_text(row.get("finalSeat")) else 0
        return (active_score, has_seat)

    ranked = sorted(results, key=score, reverse=True)
    return ranked[0] if ranked else None


def insert_search_log(client: Client, request_payload: Dict[str, Any], response_payload: Dict[str, Any]) -> Tuple[str, List[Dict[str, Any]]]:
    run_payload = {
        "request_firmenwortlaut": request_payload.get("FIRMENWORTLAUT"),
        "request_exaktesuche": request_payload.get("EXAKTESUCHE", False),
        "request_suchbereich": request_payload.get("SUCHBEREICH"),
        "request_gericht": request_payload.get("GERICHT"),
        "request_rechtsform": request_payload.get("RECHTSFORM"),
        "request_rechtseigenschaft": request_payload.get("RECHTSEIGENSCHAFT"),
        "request_ortnr": request_payload.get("ORTNR"),
        "response_count": len(response_payload.get("ERGEBNIS") or []),
        "raw_response": response_payload,
        "ran_at": now_utc_iso(),
    }
    run_resp = client.table("ofb_search_runs").insert(run_payload).execute()
    run_rows = getattr(run_resp, "data", None) or []
    if not run_rows:
        raise RuntimeError("Failed to create ofb_search_runs row")
    run_id = str(run_rows[0]["id"])

    mapped_results: List[Dict[str, Any]] = []
    for result in response_payload.get("ERGEBNIS") or []:
        if not isinstance(result, dict):
            continue
        fnr = normalize_fnr(result.get("fnr"))
        if not fnr:
            continue
        mapped_results.append(
            {
                "search_run_id": run_id,
                "firmennummer": fnr,
                "court_text": result.get("courtText"),
                "court_code": result.get("courtCode"),
                "final_status": result.get("finalStatus"),
                "final_names": result.get("finalNames"),
                "final_seat": result.get("finalSeat"),
                "final_legal_form_text": result.get("finalLegalFormText"),
                "final_legal_form_code": result.get("finalLegalFormCode"),
                "final_right_property": result.get("finalRightProperty"),
                "raw_result": result,
            }
        )
    if mapped_results:
        res_resp = client.table("ofb_search_results").upsert(
            mapped_results,
            on_conflict="search_run_id,firmennummer",
        ).execute()
        inserted = getattr(res_resp, "data", None) or []
        for row in inserted:
            raw = row.get("raw_result") if isinstance(row.get("raw_result"), dict) else {}
            changes = raw.get("changes") if isinstance(raw, dict) else []
            result_id = row.get("id")
            if not result_id or not isinstance(changes, list):
                continue
            change_payload = []
            for idx, ch in enumerate(changes):
                if not isinstance(ch, dict):
                    continue
                change_payload.append(
                    {
                        "search_result_id": result_id,
                        "seq_no": idx,
                        "names": ch.get("names"),
                        "legal_form_text": ch.get("legalFormText"),
                        "legal_form_code": ch.get("legalFormCode"),
                        "seat": ch.get("seat"),
                        "raw_change": ch,
                    }
                )
            if change_payload:
                client.table("ofb_search_result_changes").upsert(
                    change_payload,
                    on_conflict="search_result_id,seq_no",
                ).execute()
    return run_id, mapped_results


def upsert_company_from_search(client: Client, result: Dict[str, Any], euid: Optional[str] = None) -> None:
    fnr = normalize_fnr(result.get("firmennummer"))
    if not fnr:
        return
    payload = {
        "firmennummer": fnr,
        "court_code": result.get("court_code"),
        "court_text": result.get("court_text"),
        "final_status": result.get("final_status"),
        "final_names": result.get("final_names"),
        "final_seat": result.get("final_seat"),
        "final_legal_form_text": result.get("final_legal_form_text"),
        "final_legal_form_code": result.get("final_legal_form_code"),
        "final_right_property": result.get("final_right_property"),
        "euid": euid,
        "last_seen_at": now_utc_iso(),
        "updated_at": now_utc_iso(),
    }
    client.table("ofb_companies").upsert(payload, on_conflict="firmennummer").execute()


def get_or_create_snapshot(
    client: Client,
    firmennummer: str,
    stichtag: str,
    umfang: str,
    response: Dict[str, Any],
) -> str:
    existing_resp = (
        client.table("ofb_auszug_snapshots")
        .select("id")
        .eq("firmennummer", firmennummer)
        .eq("stichtag", stichtag)
        .eq("umfang", umfang)
        .limit(1)
        .execute()
    )
    existing = getattr(existing_resp, "data", None) or []
    payload = {
        "firmennummer": firmennummer,
        "stichtag": stichtag,
        "umfang": umfang,
        "pruefsumme": response.get("PRUEFSUMME"),
        "abfragezeitpunkt": to_timestamptz_iso(response.get("ABFRAGEZEITPUNKT")),
        "metadaten": response.get("METADATEN"),
        "kur": response.get("KUR"),
        "ident": response.get("IDENT"),
        "zwl": response.get("ZWL"),
        "raw_response": response,
    }
    if existing:
        snapshot_id = existing[0]["id"]
        client.table("ofb_auszug_snapshots").update(payload).eq("id", snapshot_id).execute()
        return str(snapshot_id)
    inserted = client.table("ofb_auszug_snapshots").insert(payload).execute()
    rows = getattr(inserted, "data", None) or []
    if not rows:
        raise RuntimeError("Failed to insert ofb_auszug_snapshots")
    return str(rows[0]["id"])


def replace_snapshot_children(client: Client, snapshot_id: str, response: Dict[str, Any]) -> Optional[str]:
    # Rebuild children on each refresh to keep schema mapping simple and deterministic.
    child_tables = [
        "ofb_auszug_vollz",
        "ofb_auszug_euid",
        "ofb_auszug_fun",
        "ofb_auszug_per",
        "ofb_auszug_firma_dkz02",
        "ofb_auszug_firma_dkz03",
        "ofb_auszug_firma_dkz06",
        "ofb_auszug_firma_dkz07",
    ]
    for table in child_tables:
        client.table(table).delete().eq("snapshot_id", snapshot_id).execute()

    euid_value: Optional[str] = None
    euid_rows = []
    for row in response.get("EUID") or []:
        if not isinstance(row, dict):
            continue
        euid_rows.append(
            {
                "snapshot_id": snapshot_id,
                "znr": row.get("ZNR"),
                "euid": row.get("EUID"),
                "raw_row": row,
            }
        )
        if not euid_value and as_text(row.get("EUID")):
            euid_value = as_text(row.get("EUID"))
    if euid_rows:
        client.table("ofb_auszug_euid").insert(euid_rows).execute()

    vollz_rows = []
    for row in response.get("VOLLZ") or []:
        if not isinstance(row, dict):
            continue
        hg = row.get("HG") if isinstance(row.get("HG"), dict) else {}
        vollz_rows.append(
            {
                "snapshot_id": snapshot_id,
                "vnr": as_text(row.get("VNR")),
                "antragstext": row.get("ANTRAGSTEXT"),
                "vollzugsdatum": to_date_iso(row.get("VOLLZUGSDATUM")),
                "hg_code": hg.get("CODE"),
                "hg_text": hg.get("TEXT"),
                "eingelangt_am": to_date_iso(row.get("EINGELANGTAM")),
                "az": row.get("AZ"),
                "raw_row": row,
            }
        )
    if vollz_rows:
        client.table("ofb_auszug_vollz").insert(vollz_rows).execute()

    per_id_by_pnr: Dict[str, str] = {}
    per_rows = []
    for person in response.get("PER") or []:
        if not isinstance(person, dict):
            continue
        pnr = as_text(person.get("PNR"))
        if not pnr:
            continue
        per_rows.append(
            {
                "snapshot_id": snapshot_id,
                "pnr": pnr,
                "pe_dkz03": person.get("PE_DKZ03"),
                "pe_dkz06": person.get("PE_DKZ06"),
                "pe_dkz09": person.get("PE_DKZ09"),
                "pe_staat": person.get("PE_STAAT"),
                "rechtstatsache": person.get("RECHTSTATSACHE"),
                "raw_row": person,
            }
        )
    inserted_per: List[Dict[str, Any]] = []
    if per_rows:
        per_resp = client.table("ofb_auszug_per").insert(per_rows).execute()
        inserted_per = getattr(per_resp, "data", None) or []
    for row in inserted_per:
        per_id_by_pnr[str(row.get("pnr"))] = str(row.get("id"))

    per_dkz02_rows = []
    for person in response.get("PER") or []:
        if not isinstance(person, dict):
            continue
        pnr = as_text(person.get("PNR"))
        per_id = per_id_by_pnr.get(pnr or "")
        if not per_id:
            continue
        for idx, pe_dkz02 in enumerate(person.get("PE_DKZ02") or []):
            if not isinstance(pe_dkz02, dict):
                continue
            per_dkz02_rows.append(
                {
                    "per_id": per_id,
                    "seq_no": idx,
                    "name_formatiert": pe_dkz02.get("NAME_FORMATIERT"),
                    "aufrecht": pe_dkz02.get("AUFRECHT"),
                    "titelvor": pe_dkz02.get("TITELVOR"),
                    "vorname": pe_dkz02.get("VORNAME"),
                    "nachname": pe_dkz02.get("NACHNAME"),
                    "titelnach": pe_dkz02.get("TITELNACH"),
                    "mit_firma_geloescht_durch_vnr": pe_dkz02.get("MIT_FIRMA_GELOESCHT_DURCH_VNR"),
                    "mit_zwl_geloescht_durch_vnr": pe_dkz02.get("MIT_ZWL_GELOESCHT_DURCH_VNR"),
                    "bezeichnung": pe_dkz02.get("BEZEICHNUNG"),
                    "geburtsdatum": pe_dkz02.get("GEBURTSDATUM"),
                    "vnr": pe_dkz02.get("VNR"),
                    "raw_row": pe_dkz02,
                }
            )
    if per_dkz02_rows:
        client.table("ofb_auszug_per_dkz02").upsert(per_dkz02_rows, on_conflict="per_id,seq_no").execute()

    fun_rows = []
    for fun in response.get("FUN") or []:
        if not isinstance(fun, dict):
            continue
        fun_rows.append(
            {
                "snapshot_id": snapshot_id,
                "pnr": fun.get("PNR"),
                "fken": fun.get("FKEN"),
                "fkentext": fun.get("FKENTEXT"),
                "rechtstatsache": fun.get("RECHTSTATSACHE"),
                "fu_dkz11": fun.get("FU_DKZ11"),
                "fu_dkz12": fun.get("FU_DKZ12"),
                "raw_row": fun,
            }
        )
    inserted_fun: List[Dict[str, Any]] = []
    if fun_rows:
        fun_resp = client.table("ofb_auszug_fun").insert(fun_rows).execute()
        inserted_fun = getattr(fun_resp, "data", None) or []

    fun_dkz10_rows = []
    for idx_fun, fun in enumerate(response.get("FUN") or []):
        if not isinstance(fun, dict):
            continue
        if idx_fun >= len(inserted_fun):
            continue
        fun_id = inserted_fun[idx_fun].get("id")
        if not fun_id:
            continue
        for seq_no, dkz10 in enumerate(fun.get("FU_DKZ10") or []):
            if not isinstance(dkz10, dict):
                continue
            vart = dkz10.get("VART") if isinstance(dkz10.get("VART"), dict) else {}
            fun_dkz10_rows.append(
                {
                    "fun_id": fun_id,
                    "seq_no": seq_no,
                    "vertretungsbefugtnurfuer": dkz10.get("VERTRETUNGSBEFUGTNURFUER"),
                    "txtvertr": dkz10.get("TXTVERTR"),
                    "aufrecht": dkz10.get("AUFRECHT"),
                    "datvon": dkz10.get("DATVON"),
                    "datbis": dkz10.get("DATBIS"),
                    "vart_code": vart.get("CODE"),
                    "vart_text": vart.get("TEXT"),
                    "vsbeide": dkz10.get("VSBEIDE"),
                    "whr": dkz10.get("WHR"),
                    "kapital": dkz10.get("KAPITAL"),
                    "mit_firma_geloescht_durch_vnr": dkz10.get("MIT_FIRMA_GELOESCHT_DURCH_VNR"),
                    "mit_zwl_geloescht_durch_vnr": dkz10.get("MIT_ZWL_GELOESCHT_DURCH_VNR"),
                    "bezugsperson": dkz10.get("BEZUGSPERSON"),
                    "text_lines": dkz10.get("TEXT"),
                    "vnr": dkz10.get("VNR"),
                    "raw_row": dkz10,
                }
            )
    if fun_dkz10_rows:
        client.table("ofb_auszug_fun_dkz10").upsert(fun_dkz10_rows, on_conflict="fun_id,seq_no").execute()

    firma = response.get("FIRMA") if isinstance(response.get("FIRMA"), dict) else {}

    for seq_no, row in enumerate(firma.get("FI_DKZ02") or []):
        if not isinstance(row, dict):
            continue
        client.table("ofb_auszug_firma_dkz02").insert(
            {
                "snapshot_id": snapshot_id,
                "seq_no": seq_no,
                "aufrecht": row.get("AUFRECHT"),
                "ausland": row.get("AUSLAND"),
                "mit_firma_geloescht_durch_vnr": row.get("MIT_FIRMA_GELOESCHT_DURCH_VNR"),
                "mit_zwl_geloescht_durch_vnr": row.get("MIT_ZWL_GELOESCHT_DURCH_VNR"),
                "bezeichnung": row.get("BEZEICHNUNG"),
                "vnr": row.get("VNR"),
                "raw_row": row,
            }
        ).execute()

    for seq_no, row in enumerate(firma.get("FI_DKZ03") or []):
        if not isinstance(row, dict):
            continue
        client.table("ofb_auszug_firma_dkz03").insert(
            {
                "snapshot_id": snapshot_id,
                "seq_no": seq_no,
                "zustellbar": row.get("ZUSTELLBAR"),
                "aufrecht": row.get("AUFRECHT"),
                "stelle": row.get("STELLE"),
                "ort": row.get("ORT"),
                "staat": row.get("STAAT"),
                "plz": row.get("PLZ"),
                "strasse": row.get("STRASSE"),
                "stiege": row.get("STIEGE"),
                "mit_firma_geloescht_durch_vnr": row.get("MIT_FIRMA_GELOESCHT_DURCH_VNR"),
                "mit_zwl_geloescht_durch_vnr": row.get("MIT_ZWL_GELOESCHT_DURCH_VNR"),
                "zustellanweisung": row.get("ZUSTELLANWEISUNG"),
                "hausnummer": row.get("HAUSNUMMER"),
                "tuernummer": row.get("TUERNUMMER"),
                "vnr": row.get("VNR"),
                "raw_row": row,
            }
        ).execute()

    for seq_no, row in enumerate(firma.get("FI_DKZ06") or []):
        if not isinstance(row, dict):
            continue
        ortnr = row.get("ORTNR") if isinstance(row.get("ORTNR"), dict) else {}
        client.table("ofb_auszug_firma_dkz06").insert(
            {
                "snapshot_id": snapshot_id,
                "seq_no": seq_no,
                "aufrecht": row.get("AUFRECHT"),
                "mit_firma_geloescht_durch_vnr": row.get("MIT_FIRMA_GELOESCHT_DURCH_VNR"),
                "mit_zwl_geloescht_durch_vnr": row.get("MIT_ZWL_GELOESCHT_DURCH_VNR"),
                "ortnr_code": ortnr.get("CODE"),
                "ortnr_text": ortnr.get("TEXT"),
                "sitz": row.get("SITZ"),
                "vnr": row.get("VNR"),
                "raw_row": row,
            }
        ).execute()

    for seq_no, row in enumerate(firma.get("FI_DKZ07") or []):
        if not isinstance(row, dict):
            continue
        rechtsform = row.get("RECHTSFORM") if isinstance(row.get("RECHTSFORM"), dict) else {}
        client.table("ofb_auszug_firma_dkz07").insert(
            {
                "snapshot_id": snapshot_id,
                "seq_no": seq_no,
                "aufrecht": row.get("AUFRECHT"),
                "mit_firma_geloescht_durch_vnr": row.get("MIT_FIRMA_GELOESCHT_DURCH_VNR"),
                "mit_zwl_geloescht_durch_vnr": row.get("MIT_ZWL_GELOESCHT_DURCH_VNR"),
                "vnr": row.get("VNR"),
                "rechtsform_code": rechtsform.get("CODE"),
                "rechtsform_text": rechtsform.get("TEXT"),
                "raw_row": row,
            }
        ).execute()

    return euid_value


def upsert_financial_blocks(client: Client, firmennummer: str, rows: List[Dict[str, Any]]) -> int:
    total = 0
    for row in rows:
        gj_beginn = to_timestamptz_iso(row.get("gjBeginn"))
        gj_ende = to_timestamptz_iso(row.get("gjEnde"))
        if not gj_beginn or not gj_ende:
            continue
        year_resp = client.table("ofb_financial_years").upsert(
            {
                "firmennummer": firmennummer,
                "gj_beginn": gj_beginn,
                "gj_ende": gj_ende,
                "raw_row": row,
            },
            on_conflict="firmennummer,gj_beginn,gj_ende",
        ).execute()
        year_rows = getattr(year_resp, "data", None) or []
        if not year_rows:
            continue
        financial_year_id = year_rows[0]["id"]

        bilanz = row.get("bilanzDaten") if isinstance(row.get("bilanzDaten"), dict) else {}
        guv = row.get("guvDaten") if isinstance(row.get("guvDaten"), dict) else {}
        kennzahlen = row.get("kennzahlen") if isinstance(row.get("kennzahlen"), dict) else {}
        bilanz_k = kennzahlen.get("bilanzKennzahlen") if isinstance(kennzahlen.get("bilanzKennzahlen"), dict) else {}
        guv_k = kennzahlen.get("guvKennzahlen") if isinstance(kennzahlen.get("guvKennzahlen"), dict) else {}

        client.table("ofb_financial_bilanz").upsert(
            {
                "financial_year_id": financial_year_id,
                "bilanz_summe": bilanz.get("bilanzSumme"),
                "bilanz_summe_vj": bilanz.get("bilanzSummeVJ"),
                "anlage_vermoegen": bilanz.get("anlageVermoegen"),
                "anlage_vermoegen_vj": bilanz.get("anlageVermoegenVJ"),
                "immaterielle_vermoegensgegenstaende": bilanz.get("immaterielleVermoegensgegenstaende"),
                "aktivierte_eigenleistungen": bilanz.get("aktivierteEigenleistungen"),
                "sachanlagen": bilanz.get("sachanlagen"),
                "finanzanlagen": bilanz.get("finanzanlagen"),
                "umlaufvermoegen": bilanz.get("umlaufvermoegen"),
                "vorraete": bilanz.get("vorraete"),
                "vorraete_vj": bilanz.get("vorraeteVJ"),
                "forderungen": bilanz.get("forderungen"),
                "forderungen_vj": bilanz.get("forderungenVJ"),
                "forderungen_lieferungen": bilanz.get("forderungenLieferungen"),
                "wertpapiere": bilanz.get("wertpapiere"),
                "liquides_vermoegen": bilanz.get("liquidesVermoegen"),
                "liquides_vermoegen_vj": bilanz.get("liquidesVermoegenVJ"),
                "rechnungsabgrenzungen": bilanz.get("rechnungsabgrenzungen"),
                "eigenkapital": bilanz.get("eigenkapital"),
                "eigenkapital_vj": bilanz.get("eigenkapitalVJ"),
                "eingefordertes_stammkapital": bilanz.get("eingefordertesStammkapital"),
                "kapitalruecklagen": bilanz.get("kapitalruecklagen"),
                "gewinnruecklagen": bilanz.get("gewinnruecklagen"),
                "gewinnruecklagen_vj": bilanz.get("gewinnruecklagenVJ"),
                "bilanzgewinn": bilanz.get("bilanzgewinn"),
                "vortrag": bilanz.get("vortrag"),
                "vortrag_vj": bilanz.get("vortragVJ"),
                "rueckstellungen": bilanz.get("rueckstellungen"),
                "rueckstellungen_vj": bilanz.get("rueckstellungenVJ"),
                "verbindlichkeiten": bilanz.get("verbindlichkeiten"),
                "verbindlichkeiten_vj": bilanz.get("verbindlichkeitenVJ"),
                "langfristige_verbindlichkeiten": bilanz.get("langfristigeVerbindlichkeiten"),
                "kurzfristige_verbindlichkeiten": bilanz.get("kurzfristigeVerbindlichkeiten"),
                "verbindlichkeiten_lieferungen": bilanz.get("verbindlichkeitenLieferungen"),
                "langfristige_forderungen": bilanz.get("langfristigeForderungen"),
                "kurzfristige_forderungen": bilanz.get("kurzfristigeForderungen"),
                "passive_rechnungsabgrenzungen": bilanz.get("passiveRechnungsabgrenzungen"),
                "raw_row": bilanz,
            },
            on_conflict="financial_year_id",
        ).execute()

        client.table("ofb_financial_guv").upsert(
            {
                "financial_year_id": financial_year_id,
                "betriebs_erfolg": guv.get("betriebsErfolg"),
                "betriebs_erfolg_vj": guv.get("betriebsErfolgVJ"),
                "umsatzerloese": guv.get("umsatzerloese"),
                "umsatzerloese_vj": guv.get("umsatzerloeseVJ"),
                "waren_und_materialeinkauf": guv.get("warenUndMaterialeinkauf"),
                "waren_und_materialeinkauf_vj": guv.get("warenUndMaterialeinkaufVJ"),
                "jahresueberschuss": guv.get("jahresueberschuss"),
                "jahresueberschuss_vj": guv.get("jahresueberschussVJ"),
                "bestandsveraenderung": guv.get("bestandsveraenderung"),
                "bestandsveraenderung_vj": guv.get("bestandsveraenderungVJ"),
                "personalaufwand": guv.get("personalaufwand"),
                "personalaufwand_vj": guv.get("personalaufwandVJ"),
                "steueraufwand": guv.get("steueraufwand"),
                "ergebnis_vor_steuern": guv.get("ergebnisVorSteuern"),
                "zinsen_und_aehnliche_aufwendungen": guv.get("zinsenUndAehnlicheAufwendungen"),
                "abschreibungen": guv.get("abschreibungen"),
                "sonstige_betriebliche_ertraege": guv.get("sonstigeBetrieblicheErtraege"),
                "soziale_aufwendungen": guv.get("sozialeAufwendungen"),
                "sonstige_betriebliche_aufwendungen": guv.get("sonstigeBetrieblicheAufwendungen"),
                "ertraege_aus_beteiligungen": guv.get("ertraegeAusBeteiligungen"),
                "ertraege_aus_wertpapieren": guv.get("ertraegeAusWertpapieren"),
                "sonstige_zinsen_und_aehnliche_ertraege": guv.get("sonstigeZinsenUndAehnlicheErtraege"),
                "aufwendungen_aus_finanzanlagen": guv.get("aufwendungenAusFinanzanlagen"),
                "finanzerfolg": guv.get("finanzerfolg"),
                "aufloesung_gewinnruecklagen": guv.get("aufloesungGewinnruecklagen"),
                "raw_row": guv,
            },
            on_conflict="financial_year_id",
        ).execute()

        client.table("ofb_financial_kennzahlen_bilanz").upsert(
            {
                "financial_year_id": financial_year_id,
                "eigenkapitalquote": bilanz_k.get("eigenkapitalquote"),
                "fremdkapitalquote": bilanz_k.get("fremdkapitalquote"),
                "anlagendeckungsgrad": bilanz_k.get("anlagendeckungsgrad"),
                "anlagendeckungsgrad2": bilanz_k.get("anlagendeckungsgrad2"),
                "liquiditaet_grad1": bilanz_k.get("liquiditaetGrad1"),
                "liquiditaet_grad2": bilanz_k.get("liquiditaetGrad2"),
                "liquiditaet_grad3": bilanz_k.get("liquiditaetGrad3"),
                "working_capital": bilanz_k.get("workingCapital"),
                "anlagenintensitaet": bilanz_k.get("anlagenintensitaet"),
                "umlaufintensitaet": bilanz_k.get("umlaufintensitaet"),
                "verschuldungsgrad": bilanz_k.get("verschuldungsgrad"),
                "investiertes_kapital": bilanz_k.get("investiertesKapital"),
                "veraenderung_liquider_mittel": bilanz_k.get("veraenderungLiquiderMittel"),
                "return_on_equity": bilanz_k.get("returnOnEquity"),
                "return_on_equity_simplified": bilanz_k.get("returnOnEquitySimplified"),
                "return_on_assets": bilanz_k.get("returnOnAssets"),
                "return_on_assets_simplified": bilanz_k.get("returnOnAssetsSimplified"),
                "raw_row": bilanz_k,
            },
            on_conflict="financial_year_id",
        ).execute()

        client.table("ofb_financial_kennzahlen_guv").upsert(
            {
                "financial_year_id": financial_year_id,
                "ebit_marge": guv_k.get("ebitMarge"),
                "nettomarge": guv_k.get("nettomarge"),
                "materialquote": guv_k.get("materialquote"),
                "personalquote": guv_k.get("personalquote"),
                "umsatzwachstum_kurz": guv_k.get("umsatzwachstumKurz"),
                "effektiver_steuersatz": guv_k.get("effektiverSteuersatz"),
                "investitionsquote": guv_k.get("investitionsquote"),
                "debitoren_umschlagshaeufigkeit": guv_k.get("debitorenUmschlagshaeufigkeit"),
                "kreditoren_umschlagshaeufigkeit": guv_k.get("kreditorenUmschlagshaeufigkeit"),
                "bruttomarge": guv_k.get("bruttomarge"),
                "ausschuettungen": guv_k.get("ausschuettungen"),
                "return_on_invested_capital": guv_k.get("returnOnInvestedCapital"),
                "operativer_cashflow": guv_k.get("operativerCashflow"),
                "cashflow_quote": guv_k.get("cashflowQuote"),
                "lagerumschlagsdauer": guv_k.get("lagerumschlagsdauer"),
                "forderungsumschlagsdauer": guv_k.get("forderungsumschlagsdauer"),
                "verbindlichkeitsdauer": guv_k.get("verbindlichkeitsdauer"),
                "cash_conversion_cycle": guv_k.get("cashConversionCycle"),
                "fcf": guv_k.get("FCF"),
                "capex": guv_k.get("CAPEX"),
                "raw_row": guv_k,
            },
            on_conflict="financial_year_id",
        ).execute()
        total += 1
    return total


def resolve_firmennummer_via_search(client: Client, search_name: str) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    request_payload = {
        "FIRMENWORTLAUT": search_name,
        "EXAKTESUCHE": False,
        "SUCHBEREICH": 3,
        "GERICHT": "",
        "RECHTSFORM": "",
        "RECHTSEIGENSCHAFT": "",
        "ORTNR": "",
    }
    response_payload = ofb_post_json("/firmenbuch/suche/firma/compressed", request_payload)
    if not isinstance(response_payload, dict):
        return None, None
    _, mapped_results = insert_search_log(client, request_payload, response_payload)
    best = pick_best_search_result(mapped_results)
    if not best:
        return None, None
    fnr = normalize_fnr(best.get("firmennummer"))
    return (fnr or None), best


def crawl_one_queue_item(client: Client, item: Dict[str, Any], stichtag: str, umfang: str) -> Dict[str, Any]:
    queue_id = item["id"]
    source_system = as_text(item.get("source_system")) or "unknown"
    source_key = as_text(item.get("source_key")) or ""
    search_name = as_text(item.get("search_name"))
    firmennummer = normalize_fnr(item.get("firmennummer"))
    best_search_row: Optional[Dict[str, Any]] = None

    if not firmennummer:
        if not search_name:
            raise RuntimeError("Queue item has neither firmennummer nor search_name")
        firmennummer, best_search_row = resolve_firmennummer_via_search(client, search_name)
        if not firmennummer:
            # No match found: postpone but keep as pending.
            client.table("ofb_crawl_queue").update(
                {
                    "status": "pending",
                    "attempts": int(item.get("attempts") or 0) + 1,
                    "next_run_at": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=12)).isoformat(),
                    "updated_at": now_utc_iso(),
                }
            ).eq("id", queue_id).execute()
            return {"ok": False, "reason": "no_match"}

    cleaned_fnr = normalize_fnr(firmennummer)
    if not cleaned_fnr:
        raise RuntimeError("Could not resolve firmennummer")

    extract_payload = {"FNR": cleaned_fnr, "STICHTAG": stichtag, "UMFANG": umfang}
    extract_response = ofb_post_json("/firmenbuch/auszug", extract_payload)
    if not isinstance(extract_response, dict):
        raise RuntimeError("Unexpected /auszug response format")

    snapshot_id = get_or_create_snapshot(client, cleaned_fnr, stichtag, umfang, extract_response)
    euid = replace_snapshot_children(client, snapshot_id, extract_response)

    financial_response = ofb_post_json("/firmenbuch/urkunde/daten/multiple", {"FNR": cleaned_fnr})
    financial_rows = financial_response if isinstance(financial_response, list) else []
    years_upserted = upsert_financial_blocks(client, cleaned_fnr, financial_rows)

    if best_search_row:
        upsert_company_from_search(client, best_search_row, euid=euid)
    else:
        client.table("ofb_companies").upsert(
            {"firmennummer": cleaned_fnr, "euid": euid, "last_seen_at": now_utc_iso(), "updated_at": now_utc_iso()},
            on_conflict="firmennummer",
        ).execute()

    client.table("ofb_company_source_links").upsert(
        {
            "firmennummer": cleaned_fnr,
            "source_system": source_system,
            "source_key": source_key,
            "source_name": item.get("source_name"),
            "confidence": 1.0 if best_search_row else 0.8,
            "matched_at": now_utc_iso(),
        },
        on_conflict="source_system,source_key",
    ).execute()

    client.table("ofb_crawl_queue").update(
        {
            "firmennummer": cleaned_fnr,
            "status": "done",
            "attempts": int(item.get("attempts") or 0) + 1,
            "last_run_at": now_utc_iso(),
            "next_run_at": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=30)).isoformat(),
            "last_error": None,
            "updated_at": now_utc_iso(),
        }
    ).eq("id", queue_id).execute()

    return {"ok": True, "firmennummer": cleaned_fnr, "snapshot_id": snapshot_id, "years_upserted": years_upserted}


def claim_queue_batch(client: Client, batch_size: int) -> List[Dict[str, Any]]:
    now = now_utc_iso()
    resp = (
        client.table("ofb_crawl_queue")
        .select("*")
        .in_("status", ["pending", "failed"])
        .lte("next_run_at", now)
        .order("priority", desc=False)
        .order("next_run_at", desc=False)
        .limit(batch_size)
        .execute()
    )
    rows = getattr(resp, "data", None) or []
    claimed: List[Dict[str, Any]] = []
    for row in rows:
        queue_id = row.get("id")
        if not queue_id:
            continue
        update_resp = (
            client.table("ofb_crawl_queue")
            .update({"status": "running", "last_run_at": now, "updated_at": now})
            .eq("id", queue_id)
            .eq("status", row.get("status"))
            .execute()
        )
        updated_rows = getattr(update_resp, "data", None) or []
        if updated_rows:
            claimed.append(updated_rows[0])
    return claimed


def mark_failed(client: Client, queue_id: str, previous_attempts: int, error_text: str) -> None:
    attempts = int(previous_attempts or 0) + 1
    wait_minutes = min(24 * 60, 10 * (2 ** min(6, attempts)))
    next_run = dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=wait_minutes)
    client.table("ofb_crawl_queue").update(
        {
            "status": "failed",
            "attempts": attempts,
            "last_error": error_text[:900],
            "next_run_at": next_run.isoformat(),
            "updated_at": now_utc_iso(),
        }
    ).eq("id", queue_id).execute()


def run_once(
    client: Client,
    seed_max_rows_per_source: int,
    batch_size: int,
    stichtag: str,
    umfang: str,
) -> Dict[str, Any]:
    seeded = seed_queue_from_sources(client, max_rows_per_source=seed_max_rows_per_source)
    claimed = claim_queue_batch(client, batch_size=batch_size)
    stats = {
        "seeded": seeded,
        "claimed": len(claimed),
        "ok": 0,
        "failed": 0,
        "no_match": 0,
    }
    for item in claimed:
        queue_id = str(item["id"])
        try:
            result = crawl_one_queue_item(client, item=item, stichtag=stichtag, umfang=umfang)
            if result.get("ok"):
                stats["ok"] += 1
            elif result.get("reason") == "no_match":
                stats["no_match"] += 1
            else:
                stats["failed"] += 1
        except urllib.error.HTTPError as exc:
            mark_failed(client, queue_id=queue_id, previous_attempts=int(item.get("attempts") or 0), error_text=f"HTTP {exc.code}: {exc.reason}")
            stats["failed"] += 1
        except urllib.error.URLError as exc:
            mark_failed(client, queue_id=queue_id, previous_attempts=int(item.get("attempts") or 0), error_text=f"Network error: {exc.reason}")
            stats["failed"] += 1
        except Exception as exc:
            mark_failed(client, queue_id=queue_id, previous_attempts=int(item.get("attempts") or 0), error_text=str(exc))
            stats["failed"] += 1
    return stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Continuous OpenFirmenbuch API crawler")
    parser.add_argument("--batch-size", type=int, default=20, help="Queue items processed per cycle")
    parser.add_argument("--seed-max-rows-per-source", type=int, default=3000, help="Max rows fetched per source table each cycle")
    parser.add_argument("--stichtag", type=str, default=today_iso(), help="As-of date for /firmenbuch/auszug (YYYY-MM-DD)")
    parser.add_argument("--umfang", type=str, default="Kurzinformation", help="UMFANG parameter for /firmenbuch/auszug")
    parser.add_argument("--cycles", type=int, default=0, help="Number of cycles to run (default: endless; set >0 for finite)")
    parser.add_argument("--sleep-seconds", type=float, default=5.0, help="Sleep between cycles")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    client = create_client_from_env()
    ensure_openfirmenbuch_tables_ready(client)
    print("Schema preflight passed: OpenFirmenbuch tables are reachable.")

    cycles = int(args.cycles)
    cycle_no = 0
    while True:
        cycle_no += 1
        started = time.time()
        stats = run_once(
            client=client,
            seed_max_rows_per_source=max(1, int(args.seed_max_rows_per_source)),
            batch_size=max(1, int(args.batch_size)),
            stichtag=args.stichtag,
            umfang=args.umfang,
        )
        elapsed = time.time() - started
        print(
            f"[cycle={cycle_no}] seeded={stats['seeded']} claimed={stats['claimed']} "
            f"ok={stats['ok']} no_match={stats['no_match']} failed={stats['failed']} "
            f"elapsed={elapsed:.1f}s"
        )

        if cycles > 0 and cycle_no >= cycles:
            break
        time.sleep(max(0.0, float(args.sleep_seconds)) + random.uniform(0.0, 0.5))


if __name__ == "__main__":
    main()
