import re
from typing import Any, Dict, List, Optional, Set

from mas.db import _request_user_context

OFB_TABLES: Dict[str, str] = {
    "ofb_crawl_queue": "Continuous crawl queue with source, status, and scheduling state.",
    "ofb_companies": "Canonical company rows keyed by firmennummer with normalized identity fields.",
    "ofb_company_source_links": "Mappings from upstream source datasets to firmennummer.",
    "ofb_search_runs": "Stored API search requests and response metadata.",
    "ofb_search_results": "Per-run search matches returned by the API.",
    "ofb_search_result_changes": "Historical change snippets attached to search results.",
    "ofb_auszug_snapshots": "Register extract snapshots by stichtag and scope.",
    "ofb_auszug_vollz": "Register event history at VNR/event level.",
    "ofb_auszug_euid": "EUID identifiers and mappings.",
    "ofb_auszug_fun": "Role/function entries linked to people records.",
    "ofb_auszug_fun_dkz10": "Representation authority details for function entries.",
    "ofb_auszug_per": "Person entities keyed by PNR.",
    "ofb_auszug_per_dkz02": "Person identity and name detail blocks.",
    "ofb_auszug_firma_dkz02": "Company name blocks from extract records.",
    "ofb_auszug_firma_dkz03": "Company address blocks from extract records.",
    "ofb_auszug_firma_dkz06": "Company seat/location blocks from extract records.",
    "ofb_auszug_firma_dkz07": "Company legal-form blocks from extract records.",
    "ofb_financial_years": "Fiscal-year wrapper rows per firmennummer.",
    "ofb_financial_bilanz": "Balance-sheet values by financial year.",
    "ofb_financial_guv": "Profit-and-loss values, including revenue fields.",
    "ofb_financial_kennzahlen_bilanz": "Balance-sheet KPI metrics by financial year.",
    "ofb_financial_kennzahlen_guv": "Profit-and-loss KPI metrics by financial year.",
}


def _get_supabase_client() -> Any:
    ctx = _request_user_context.get() or {}
    supabase_client = ctx.get("supabase_client")
    if supabase_client is None:
        raise RuntimeError("Supabase client not available in request context")
    return supabase_client


def _safe_limit(limit: int, default: int = 20, min_value: int = 1, max_value: int = 200) -> int:
    try:
        value = int(limit)
    except Exception:
        value = default
    return max(min_value, min(value, max_value))


def _normalize_firmennummer(value: Any) -> str:
    txt = str(value or "").strip().replace(" ", "")
    return txt.lower()


def _safe_firmennummer(value: str) -> str:
    normalized = _normalize_firmennummer(value)
    if not normalized:
        raise ValueError("Missing firmennummer")
    if not re.fullmatch(r"[a-z0-9/.\-]+", normalized):
        raise ValueError("Invalid firmennummer")
    return normalized


def _year_from_iso(ts: Any) -> Optional[int]:
    txt = str(ts or "")
    if len(txt) < 4 or not txt[:4].isdigit():
        return None
    return int(txt[:4])


def ofb_list_tables() -> Dict[str, Any]:
    """List OpenFirmenbuch-specific tables with concise descriptions of what each table stores."""
    tables = [{"table": name, "description": desc} for name, desc in sorted(OFB_TABLES.items())]
    return {"ok": True, "count": len(tables), "tables": tables}


def ofb_source_overview() -> Dict[str, Any]:
    """Summarize crawl and source coverage metrics across queue and source-link tables."""
    try:
        client = _get_supabase_client()
        queue_resp = client.table("ofb_crawl_queue").select("status,source_system").limit(2000).execute()
        queue_rows = getattr(queue_resp, "data", None) or []
        link_resp = client.table("ofb_company_source_links").select("source_system,firmennummer").limit(4000).execute()
        link_rows = getattr(link_resp, "data", None) or []

        queue_by_status: Dict[str, int] = {}
        queue_by_source: Dict[str, int] = {}
        for row in queue_rows:
            status = str(row.get("status") or "unknown")
            source = str(row.get("source_system") or "unknown")
            queue_by_status[status] = queue_by_status.get(status, 0) + 1
            queue_by_source[source] = queue_by_source.get(source, 0) + 1

        links_by_source: Dict[str, int] = {}
        unique_companies: Set[str] = set()
        for row in link_rows:
            source = str(row.get("source_system") or "unknown")
            links_by_source[source] = links_by_source.get(source, 0) + 1
            fnr = _normalize_firmennummer(row.get("firmennummer"))
            if fnr:
                unique_companies.add(fnr)

        return {
            "ok": True,
            "queue_rows_scanned": len(queue_rows),
            "link_rows_scanned": len(link_rows),
            "queue_by_status": queue_by_status,
            "queue_by_source": queue_by_source,
            "links_by_source": links_by_source,
            "resolved_unique_companies": len(unique_companies),
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def ofb_joined_company_screen(
    name_query: str = "",
    min_revenue: Optional[float] = None,
    max_revenue: Optional[float] = None,
    min_equity_ratio: Optional[float] = None,
    status: str = "",
    legal_form_code: str = "",
    source_system: str = "",
    year: int = 0,
    limit: int = 20,
) -> Dict[str, Any]:
    """
    Return a joined company screening view across canonical company rows, source links, and latest financials.
    Supports filters for name, revenue (umsatzerloese), equity ratio, status, legal form, source system, and year.
    """
    try:
        client = _get_supabase_client()
        safe_limit = _safe_limit(limit, default=20, max_value=150)
        candidate_limit = max(200, safe_limit * 20)

        candidate_fnr: Set[str] = set()
        query = (name_query or "").strip()
        if query:
            pattern = f"%{query}%"
            source_rows = (
                client.table("ofb_company_source_links")
                .select("firmennummer,source_name,source_system")
                .ilike("source_name", pattern)
                .limit(candidate_limit)
                .execute()
            )
            for row in getattr(source_rows, "data", None) or []:
                fnr = _normalize_firmennummer(row.get("firmennummer"))
                if fnr:
                    if source_system and str(row.get("source_system") or "").lower() != source_system.lower():
                        continue
                    candidate_fnr.add(fnr)

            queue_rows = (
                client.table("ofb_crawl_queue")
                .select("firmennummer,search_name,source_system")
                .ilike("search_name", pattern)
                .limit(candidate_limit)
                .execute()
            )
            for row in getattr(queue_rows, "data", None) or []:
                fnr = _normalize_firmennummer(row.get("firmennummer"))
                if fnr:
                    if source_system and str(row.get("source_system") or "").lower() != source_system.lower():
                        continue
                    candidate_fnr.add(fnr)

        companies_query = client.table("ofb_companies").select("*")
        if status:
            companies_query = companies_query.ilike("final_status", status)
        if legal_form_code:
            companies_query = companies_query.ilike("final_legal_form_code", legal_form_code)
        companies_resp = companies_query.limit(candidate_limit).execute()
        companies = getattr(companies_resp, "data", None) or []

        if candidate_fnr:
            companies = [row for row in companies if _normalize_firmennummer(row.get("firmennummer")) in candidate_fnr]
        else:
            if source_system:
                links_resp = (
                    client.table("ofb_company_source_links")
                    .select("firmennummer,source_system")
                    .ilike("source_system", source_system)
                    .limit(candidate_limit)
                    .execute()
                )
                fnr_by_source = {
                    _normalize_firmennummer(row.get("firmennummer"))
                    for row in (getattr(links_resp, "data", None) or [])
                    if _normalize_firmennummer(row.get("firmennummer"))
                }
                companies = [row for row in companies if _normalize_firmennummer(row.get("firmennummer")) in fnr_by_source]

        firmennummer_list = [
            _normalize_firmennummer(row.get("firmennummer"))
            for row in companies
            if _normalize_firmennummer(row.get("firmennummer"))
        ]
        if not firmennummer_list:
            return {"ok": True, "count": 0, "rows": []}

        years_resp = (
            client.table("ofb_financial_years")
            .select("id,firmennummer,gj_beginn,gj_ende")
            .in_("firmennummer", firmennummer_list[:1000])
            .order("gj_ende", desc=True)
            .limit(5000)
            .execute()
        )
        year_rows = getattr(years_resp, "data", None) or []
        latest_year_by_fnr: Dict[str, Dict[str, Any]] = {}
        for row in year_rows:
            fnr = _normalize_firmennummer(row.get("firmennummer"))
            if not fnr:
                continue
            if year:
                row_year = _year_from_iso(row.get("gj_ende"))
                if row_year != int(year):
                    continue
            if fnr not in latest_year_by_fnr:
                latest_year_by_fnr[fnr] = row

        financial_year_ids = [row["id"] for row in latest_year_by_fnr.values() if row.get("id")]
        guv_by_year: Dict[str, Dict[str, Any]] = {}
        b_kpi_by_year: Dict[str, Dict[str, Any]] = {}
        if financial_year_ids:
            guv_resp = (
                client.table("ofb_financial_guv")
                .select("financial_year_id,umsatzerloese,jahresueberschuss,betriebs_erfolg")
                .in_("financial_year_id", financial_year_ids)
                .limit(5000)
                .execute()
            )
            for row in getattr(guv_resp, "data", None) or []:
                key = str(row.get("financial_year_id") or "")
                if key:
                    guv_by_year[key] = row

            b_kpi_resp = (
                client.table("ofb_financial_kennzahlen_bilanz")
                .select("financial_year_id,eigenkapitalquote,verschuldungsgrad")
                .in_("financial_year_id", financial_year_ids)
                .limit(5000)
                .execute()
            )
            for row in getattr(b_kpi_resp, "data", None) or []:
                key = str(row.get("financial_year_id") or "")
                if key:
                    b_kpi_by_year[key] = row

        source_link_rows = (
            client.table("ofb_company_source_links")
            .select("firmennummer,source_system,source_name")
            .in_("firmennummer", firmennummer_list[:1000])
            .limit(5000)
            .execute()
        )
        links_by_fnr: Dict[str, List[Dict[str, Any]]] = {}
        for row in getattr(source_link_rows, "data", None) or []:
            fnr = _normalize_firmennummer(row.get("firmennummer"))
            if not fnr:
                continue
            links_by_fnr.setdefault(fnr, []).append(
                {
                    "source_system": row.get("source_system"),
                    "source_name": row.get("source_name"),
                }
            )

        joined_rows: List[Dict[str, Any]] = []
        for company in companies:
            fnr = _normalize_firmennummer(company.get("firmennummer"))
            if not fnr:
                continue
            year_row = latest_year_by_fnr.get(fnr) or {}
            year_id = str(year_row.get("id") or "")
            guv = guv_by_year.get(year_id, {})
            b_kpi = b_kpi_by_year.get(year_id, {})
            revenue = guv.get("umsatzerloese")
            equity_ratio = b_kpi.get("eigenkapitalquote")

            if min_revenue is not None and (revenue is None or float(revenue) < float(min_revenue)):
                continue
            if max_revenue is not None and (revenue is None or float(revenue) > float(max_revenue)):
                continue
            if min_equity_ratio is not None and (equity_ratio is None or float(equity_ratio) < float(min_equity_ratio)):
                continue

            joined_rows.append(
                {
                    "firmennummer": fnr,
                    "final_names": company.get("final_names"),
                    "final_seat": company.get("final_seat"),
                    "final_status": company.get("final_status"),
                    "legal_form_code": company.get("final_legal_form_code"),
                    "legal_form_text": company.get("final_legal_form_text"),
                    "court_code": company.get("court_code"),
                    "euid": company.get("euid"),
                    "gj_ende": year_row.get("gj_ende"),
                    "umsatzerloese": revenue,
                    "jahresueberschuss": guv.get("jahresueberschuss"),
                    "betriebs_erfolg": guv.get("betriebs_erfolg"),
                    "eigenkapitalquote": equity_ratio,
                    "verschuldungsgrad": b_kpi.get("verschuldungsgrad"),
                    "source_links": links_by_fnr.get(fnr, []),
                }
            )

        joined_rows.sort(key=lambda x: (x.get("umsatzerloese") is None, -(x.get("umsatzerloese") or 0)))
        return {"ok": True, "count": len(joined_rows[:safe_limit]), "rows": joined_rows[:safe_limit]}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def ofb_company_full_view(
    firmennummer: str,
    financial_years_limit: int = 5,
    include_history: bool = True,
) -> Dict[str, Any]:
    """
    Build a comprehensive single-company view with canonical data, source links, latest snapshot details,
    people and roles, recent financial years, and optional register history.
    """
    try:
        client = _get_supabase_client()
        fnr = _safe_firmennummer(firmennummer)
        safe_year_limit = _safe_limit(financial_years_limit, default=5, max_value=20)

        company_resp = client.table("ofb_companies").select("*").eq("firmennummer", fnr).limit(1).execute()
        company_rows = getattr(company_resp, "data", None) or []
        company = company_rows[0] if company_rows else None

        links_resp = client.table("ofb_company_source_links").select("*").eq("firmennummer", fnr).limit(100).execute()
        source_links = getattr(links_resp, "data", None) or []

        snapshot_resp = (
            client.table("ofb_auszug_snapshots")
            .select("id,stichtag,umfang,pruefsumme,abfragezeitpunkt")
            .eq("firmennummer", fnr)
            .order("stichtag", desc=True)
            .limit(1)
            .execute()
        )
        snapshot_rows = getattr(snapshot_resp, "data", None) or []
        latest_snapshot = snapshot_rows[0] if snapshot_rows else None

        per_rows: List[Dict[str, Any]] = []
        role_rows: List[Dict[str, Any]] = []
        firm_name_rows: List[Dict[str, Any]] = []
        firm_address_rows: List[Dict[str, Any]] = []
        if latest_snapshot:
            snapshot_id = latest_snapshot["id"]
            per_rows = (
                client.table("ofb_auszug_per")
                .select("id,pnr")
                .eq("snapshot_id", snapshot_id)
                .limit(500)
                .execute()
            ).data or []

            pnr_to_person: Dict[str, Dict[str, Any]] = {}
            for person in per_rows:
                pnr_to_person[str(person.get("pnr"))] = {"pnr": person.get("pnr")}
            person_ids = [row.get("id") for row in per_rows if row.get("id")]
            if person_ids:
                person_detail_rows = (
                    client.table("ofb_auszug_per_dkz02")
                    .select("per_id,name_formatiert,vorname,nachname,geburtsdatum,aufrecht,vnr")
                    .in_("per_id", person_ids)
                    .limit(2000)
                    .execute()
                ).data or []
                for row in person_detail_rows:
                    per_id = row.get("per_id")
                    pnr = next((p.get("pnr") for p in per_rows if p.get("id") == per_id), None)
                    if pnr is not None:
                        pnr_to_person[str(pnr)] = {
                            "pnr": pnr,
                            "name_formatiert": row.get("name_formatiert"),
                            "vorname": row.get("vorname"),
                            "nachname": row.get("nachname"),
                            "geburtsdatum": row.get("geburtsdatum"),
                            "aufrecht": row.get("aufrecht"),
                            "vnr": row.get("vnr"),
                        }

            roles = (
                client.table("ofb_auszug_fun")
                .select("id,pnr,fken,fkentext")
                .eq("snapshot_id", snapshot_id)
                .limit(1000)
                .execute()
            ).data or []
            fun_ids = [row.get("id") for row in roles if row.get("id")]
            authority_by_fun: Dict[str, List[Dict[str, Any]]] = {}
            if fun_ids:
                authority_rows = (
                    client.table("ofb_auszug_fun_dkz10")
                    .select("fun_id,seq_no,vart_code,vart_text,txtvertr,datvon,datbis,aufrecht,vnr")
                    .in_("fun_id", fun_ids)
                    .limit(2000)
                    .execute()
                ).data or []
                for row in authority_rows:
                    fun_id = str(row.get("fun_id"))
                    authority_by_fun.setdefault(fun_id, []).append(row)

            for role in roles:
                pnr = str(role.get("pnr") or "")
                role_rows.append(
                    {
                        "pnr": pnr or None,
                        "role_code": role.get("fken"),
                        "role_text": role.get("fkentext"),
                        "person": pnr_to_person.get(pnr),
                        "authorities": authority_by_fun.get(str(role.get("id")), []),
                    }
                )

            firm_name_rows = (
                client.table("ofb_auszug_firma_dkz02")
                .select("bezeichnung,aufrecht,vnr")
                .eq("snapshot_id", snapshot_id)
                .limit(100)
                .execute()
            ).data or []
            firm_address_rows = (
                client.table("ofb_auszug_firma_dkz03")
                .select("strasse,hausnummer,plz,ort,staat,aufrecht,vnr")
                .eq("snapshot_id", snapshot_id)
                .limit(100)
                .execute()
            ).data or []

        fy_rows = (
            client.table("ofb_financial_years")
            .select("id,gj_beginn,gj_ende")
            .eq("firmennummer", fnr)
            .order("gj_ende", desc=True)
            .limit(safe_year_limit)
            .execute()
        ).data or []
        fy_ids = [row["id"] for row in fy_rows if row.get("id")]
        guv_rows: List[Dict[str, Any]] = []
        bilanz_rows: List[Dict[str, Any]] = []
        if fy_ids:
            guv_rows = (
                client.table("ofb_financial_guv")
                .select("financial_year_id,umsatzerloese,jahresueberschuss,betriebs_erfolg")
                .in_("financial_year_id", fy_ids)
                .limit(200)
                .execute()
            ).data or []
            bilanz_rows = (
                client.table("ofb_financial_bilanz")
                .select("financial_year_id,bilanz_summe,eigenkapital,verbindlichkeiten")
                .in_("financial_year_id", fy_ids)
                .limit(200)
                .execute()
            ).data or []
        guv_by_id = {str(row.get("financial_year_id")): row for row in guv_rows}
        bilanz_by_id = {str(row.get("financial_year_id")): row for row in bilanz_rows}
        financials = []
        for fy in fy_rows:
            fy_id = str(fy.get("id"))
            financials.append(
                {
                    "gj_beginn": fy.get("gj_beginn"),
                    "gj_ende": fy.get("gj_ende"),
                    "guv": guv_by_id.get(fy_id, {}),
                    "bilanz": bilanz_by_id.get(fy_id, {}),
                }
            )

        history = []
        if include_history:
            history = (
                client.table("ofb_auszug_vollz")
                .select("vnr,vollzugsdatum,eingelangt_am,az,antragstext,hg_code,hg_text")
                .eq("snapshot_id", latest_snapshot["id"] if latest_snapshot else "")
                .order("vollzugsdatum", desc=True)
                .limit(100)
                .execute()
            ).data or []

        return {
            "ok": True,
            "firmennummer": fnr,
            "company": company,
            "source_links": source_links,
            "latest_snapshot": latest_snapshot,
            "company_name_blocks": firm_name_rows,
            "company_address_blocks": firm_address_rows,
            "roles": role_rows,
            "financials": financials,
            "history": history,
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def ofb_find_companies_missing_financials(
    min_age_days: int = 7,
    limit: int = 50,
) -> Dict[str, Any]:
    """
    Find crawled companies that already have register snapshots but still lack financial-year records,
    useful for retry and backfill candidate lists.
    """
    try:
        client = _get_supabase_client()
        safe_limit = _safe_limit(limit, default=50, max_value=200)
        snapshots = (
            client.table("ofb_auszug_snapshots")
            .select("firmennummer,stichtag,created_at")
            .order("created_at", desc=False)
            .limit(5000)
            .execute()
        ).data or []
        if not snapshots:
            return {"ok": True, "count": 0, "rows": []}

        fnr_set = {_normalize_firmennummer(row.get("firmennummer")) for row in snapshots}
        fnr_set = {x for x in fnr_set if x}
        fy_rows = (
            client.table("ofb_financial_years")
            .select("firmennummer")
            .in_("firmennummer", list(fnr_set)[:1000])
            .limit(5000)
            .execute()
        ).data or []
        fnr_with_financials = {_normalize_firmennummer(row.get("firmennummer")) for row in fy_rows}

        missing = []
        for row in snapshots:
            fnr = _normalize_firmennummer(row.get("firmennummer"))
            if not fnr or fnr in fnr_with_financials:
                continue
            created_at = str(row.get("created_at") or "")
            too_new = False
            if len(created_at) >= 10:
                try:
                    created_date = int(created_at[:10].replace("-", ""))
                    today = int(__import__("datetime").date.today().strftime("%Y%m%d"))
                    too_new = (today - created_date) < int(min_age_days)
                except Exception:
                    too_new = False
            if too_new:
                continue
            missing.append(
                {
                    "firmennummer": fnr,
                    "latest_snapshot_stichtag": row.get("stichtag"),
                    "snapshot_created_at": row.get("created_at"),
                }
            )

        deduped: Dict[str, Dict[str, Any]] = {}
        for row in missing:
            deduped[row["firmennummer"]] = row
        rows = list(deduped.values())[:safe_limit]
        return {"ok": True, "count": len(rows), "rows": rows}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}
