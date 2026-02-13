import datetime as dt
import json
import os
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional

OPENFIRMENBUCH_BASE_URL = "https://api.openfirmenbuch.at"
OPENFIRMENBUCH_TIMEOUT_SECONDS = int(os.getenv("OPENFIRMENBUCH_TIMEOUT_SECONDS", "30"))


def _ofb_post_json(path: str, payload: Dict[str, Any]) -> Any:
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


def _as_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _pick_active(records: Any) -> Optional[Dict[str, Any]]:
    for item in _as_list(records):
        if isinstance(item, dict) and item.get("AUFRECHT") is True:
            return item
    for item in _as_list(records):
        if isinstance(item, dict):
            return item
    return None


def ofb_search_company_compressed(
    firmenwortlaut: str,
    exaktesuche: bool = False,
    suchbereich: int = 3,
    gericht: str = "",
    rechtsform: str = "",
    rechtseigenschaft: str = "",
    ortnr: str = "",
    limit: int = 10,
) -> Dict[str, Any]:
    """
    Search companies via `/firmenbuch/suche/firma/compressed` and return a compact result set for quick matching.
    """
    query = (firmenwortlaut or "").strip()
    if not query:
        return {"ok": False, "error": "Missing firmenwortlaut"}
    safe_limit = max(1, min(int(limit), 50))
    payload = {
        "FIRMENWORTLAUT": query,
        "EXAKTESUCHE": bool(exaktesuche),
        "SUCHBEREICH": int(suchbereich),
        "GERICHT": (gericht or "").strip(),
        "RECHTSFORM": (rechtsform or "").strip(),
        "RECHTSEIGENSCHAFT": (rechtseigenschaft or "").strip(),
        "ORTNR": (ortnr or "").strip(),
    }
    try:
        response = _ofb_post_json("/firmenbuch/suche/firma/compressed", payload)
        results = _as_list(response.get("ERGEBNIS") if isinstance(response, dict) else [])
        compact: List[Dict[str, Any]] = []
        for row in results[:safe_limit]:
            if not isinstance(row, dict):
                continue
            compact.append(
                {
                    "fnr": row.get("fnr"),
                    "courtText": row.get("courtText"),
                    "courtCode": row.get("courtCode"),
                    "finalStatus": row.get("finalStatus"),
                    "finalNames": row.get("finalNames"),
                    "finalSeat": row.get("finalSeat"),
                    "finalLegalFormText": row.get("finalLegalFormText"),
                    "finalLegalFormCode": row.get("finalLegalFormCode"),
                    "changes": row.get("changes", []),
                }
            )
        return {
            "ok": True,
            "count": len(compact),
            "query": payload,
            "results": compact,
        }
    except urllib.error.HTTPError as exc:
        return {"ok": False, "error": f"HTTP {exc.code}: {exc.reason}"}
    except urllib.error.URLError as exc:
        return {"ok": False, "error": f"Network error: {exc.reason}"}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def ofb_get_register_extract(
    fnr: str,
    stichtag: str = "",
    umfang: str = "Kurzinformation",
    include_raw: bool = False,
) -> Dict[str, Any]:
    """
    Fetch a structured Firmenbuch register extract via `/firmenbuch/auszug` for a given company and date.
    """
    cleaned_fnr = (fnr or "").strip()
    if not cleaned_fnr:
        return {"ok": False, "error": "Missing fnr"}
    chosen_stichtag = (stichtag or dt.date.today().isoformat()).strip()
    payload = {"FNR": cleaned_fnr, "STICHTAG": chosen_stichtag, "UMFANG": (umfang or "Kurzinformation").strip()}
    try:
        response = _ofb_post_json("/firmenbuch/auszug", payload)
        if not isinstance(response, dict):
            return {"ok": False, "error": "Unexpected API response format"}
        if include_raw:
            return {"ok": True, "request": payload, "data": response}
        return {
            "ok": True,
            "request": payload,
            "summary": {
                "fnr": response.get("FNR"),
                "stichtag": response.get("STICHTAG"),
                "umfang": response.get("UMFANG"),
                "abfragezeitpunkt": response.get("ABFRAGEZEITPUNKT"),
                "pruefsumme": response.get("PRUEFSUMME"),
                "vollz_count": len(_as_list(response.get("VOLLZ"))),
                "person_count": len(_as_list(response.get("PER"))),
                "function_count": len(_as_list(response.get("FUN"))),
                "euid_count": len(_as_list(response.get("EUID"))),
            },
        }
    except urllib.error.HTTPError as exc:
        return {"ok": False, "error": f"HTTP {exc.code}: {exc.reason}"}
    except urllib.error.URLError as exc:
        return {"ok": False, "error": f"Network error: {exc.reason}"}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def ofb_get_financials_multiple(
    fnr: str,
    include_raw: bool = False,
    limit_years: int = 5,
) -> Dict[str, Any]:
    """
    Fetch multi-year financial statement data via `/firmenbuch/urkunde/daten/multiple`.
    """
    cleaned_fnr = (fnr or "").strip()
    if not cleaned_fnr:
        return {"ok": False, "error": "Missing fnr"}
    safe_limit = max(1, min(int(limit_years), 20))
    payload = {"FNR": cleaned_fnr}
    try:
        response = _ofb_post_json("/firmenbuch/urkunde/daten/multiple", payload)
        rows = _as_list(response)
        if include_raw:
            return {"ok": True, "request": payload, "rows": rows[:safe_limit]}
        compact_rows: List[Dict[str, Any]] = []
        for row in rows[:safe_limit]:
            if not isinstance(row, dict):
                continue
            bilanz = row.get("bilanzDaten") if isinstance(row.get("bilanzDaten"), dict) else {}
            guv = row.get("guvDaten") if isinstance(row.get("guvDaten"), dict) else {}
            kennzahlen = row.get("kennzahlen") if isinstance(row.get("kennzahlen"), dict) else {}
            bilanz_kennzahlen = (
                kennzahlen.get("bilanzKennzahlen") if isinstance(kennzahlen.get("bilanzKennzahlen"), dict) else {}
            )
            guv_kennzahlen = (
                kennzahlen.get("guvKennzahlen") if isinstance(kennzahlen.get("guvKennzahlen"), dict) else {}
            )
            compact_rows.append(
                {
                    "gjBeginn": row.get("gjBeginn"),
                    "gjEnde": row.get("gjEnde"),
                    "bilanzSumme": bilanz.get("bilanzSumme"),
                    "bilanzSummeVJ": bilanz.get("bilanzSummeVJ"),
                    "eigenkapital": bilanz.get("eigenkapital"),
                    "verbindlichkeiten": bilanz.get("verbindlichkeiten"),
                    "umsatzerloese": guv.get("umsatzerloese"),
                    "jahresueberschuss": guv.get("jahresueberschuss"),
                    "eigenkapitalquote": bilanz_kennzahlen.get("eigenkapitalquote"),
                    "ebitMarge": guv_kennzahlen.get("ebitMarge"),
                    "nettomarge": guv_kennzahlen.get("nettomarge"),
                }
            )
        return {"ok": True, "request": payload, "count": len(compact_rows), "rows": compact_rows}
    except urllib.error.HTTPError as exc:
        return {"ok": False, "error": f"HTTP {exc.code}: {exc.reason}"}
    except urllib.error.URLError as exc:
        return {"ok": False, "error": f"Network error: {exc.reason}"}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def ofb_get_company_profile(
    fnr: str,
    stichtag: str = "",
    umfang: str = "Kurzinformation",
) -> Dict[str, Any]:
    """
    Return a concise company profile distilled from `/firmenbuch/auszug` (name, seat, address, legal form).
    """
    extract_result = ofb_get_register_extract(fnr=fnr, stichtag=stichtag, umfang=umfang, include_raw=True)
    if not extract_result.get("ok"):
        return extract_result
    data = extract_result.get("data")
    if not isinstance(data, dict):
        return {"ok": False, "error": "Unexpected register extract format"}

    firma = data.get("FIRMA") if isinstance(data.get("FIRMA"), dict) else {}
    name_block = _pick_active(firma.get("FI_DKZ02"))
    address_block = _pick_active(firma.get("FI_DKZ03"))
    seat_block = _pick_active(firma.get("FI_DKZ06"))
    legal_block = _pick_active(firma.get("FI_DKZ07"))

    return {
        "ok": True,
        "fnr": data.get("FNR"),
        "stichtag": data.get("STICHTAG"),
        "name_lines": (name_block or {}).get("BEZEICHNUNG", []),
        "name": " ".join((name_block or {}).get("BEZEICHNUNG", [])).strip(),
        "seat": (seat_block or {}).get("SITZ"),
        "ortnr": ((seat_block or {}).get("ORTNR") or {}).get("CODE"),
        "address": {
            "strasse": (address_block or {}).get("STRASSE"),
            "hausnummer": (address_block or {}).get("HAUSNUMMER"),
            "plz": (address_block or {}).get("PLZ"),
            "ort": (address_block or {}).get("ORT"),
            "staat": (address_block or {}).get("STAAT"),
        },
        "legal_form": ((legal_block or {}).get("RECHTSFORM") or {}),
        "abfragezeitpunkt": data.get("ABFRAGEZEITPUNKT"),
        "pruefsumme": data.get("PRUEFSUMME"),
    }


def ofb_get_management_roles(
    fnr: str,
    stichtag: str = "",
    umfang: str = "Kurzinformation",
) -> Dict[str, Any]:
    """
    Map management/function entries (FUN) to person records (PER) from `/firmenbuch/auszug`.
    """
    extract_result = ofb_get_register_extract(fnr=fnr, stichtag=stichtag, umfang=umfang, include_raw=True)
    if not extract_result.get("ok"):
        return extract_result
    data = extract_result.get("data")
    if not isinstance(data, dict):
        return {"ok": False, "error": "Unexpected register extract format"}

    per_entries = _as_list(data.get("PER"))
    person_by_pnr: Dict[str, Dict[str, Any]] = {}
    for person in per_entries:
        if not isinstance(person, dict):
            continue
        pnr = str(person.get("PNR") or "").strip()
        if not pnr:
            continue
        person_identity = _pick_active(person.get("PE_DKZ02")) or {}
        person_by_pnr[pnr] = {
            "pnr": pnr,
            "name_formatiert": person_identity.get("NAME_FORMATIERT", []),
            "vorname": person_identity.get("VORNAME"),
            "nachname": person_identity.get("NACHNAME"),
            "geburtsdatum": person_identity.get("GEBURTSDATUM"),
        }

    roles: List[Dict[str, Any]] = []
    for fun in _as_list(data.get("FUN")):
        if not isinstance(fun, dict):
            continue
        pnr = str(fun.get("PNR") or "").strip()
        authority_block = _pick_active(fun.get("FU_DKZ10")) or {}
        roles.append(
            {
                "pnr": pnr or None,
                "role_code": fun.get("FKEN"),
                "role_text": fun.get("FKENTEXT"),
                "person": person_by_pnr.get(pnr),
                "representation_type": (authority_block.get("VART") or {}),
                "representation_text": authority_block.get("TXTVERTR", []),
                "effective_from": authority_block.get("DATVON"),
                "effective_to": authority_block.get("DATBIS"),
                "active": authority_block.get("AUFRECHT"),
                "vnr": authority_block.get("VNR"),
            }
        )

    return {
        "ok": True,
        "fnr": data.get("FNR"),
        "stichtag": data.get("STICHTAG"),
        "count": len(roles),
        "roles": roles,
    }


def ofb_get_company_report(
    fnr: str,
    stichtag: str = "",
    umfang: str = "Kurzinformation",
    include_financials: bool = True,
) -> Dict[str, Any]:
    """
    Convenience report tool: combine company profile, management roles, and optional financials for one FNR.
    """
    profile = ofb_get_company_profile(fnr=fnr, stichtag=stichtag, umfang=umfang)
    if not profile.get("ok"):
        return profile
    management = ofb_get_management_roles(fnr=fnr, stichtag=stichtag, umfang=umfang)
    if not management.get("ok"):
        return management

    report: Dict[str, Any] = {
        "ok": True,
        "fnr": profile.get("fnr"),
        "stichtag": profile.get("stichtag"),
        "company_profile": profile,
        "management_roles": management.get("roles", []),
    }
    if include_financials:
        report["financials"] = ofb_get_financials_multiple(fnr=fnr, include_raw=False, limit_years=5)
    return report
