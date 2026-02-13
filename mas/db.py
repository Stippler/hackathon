import contextvars
import json
import re
from typing import Any, Dict, List, Optional

_request_user_context: contextvars.ContextVar[Optional[Dict[str, Any]]] = contextvars.ContextVar(
    "request_user_context",
    default=None,
)

KNOWN_TABLES: Dict[str, Dict[str, Any]] = {
    "wko_branches": {
        "description": "WKO branch taxonomy catalog used for category and crawl coverage context.",
        "key_columns": ["branche", "branch_url", "letter", "source", "discovered_at"],
    },
    "wko_companies": {
        "description": "WKO company directory including contact data and crawl metadata.",
        "key_columns": ["branche", "name", "email", "phone", "address", "wko_detail_url", "crawled_at"],
    },
    "projectfacts": {
        "description": "Structured company profile table with segmentation and activity attributes.",
        "key_columns": ["name", "city", "country", "industries", "size", "last_activity_at"],
    },
    "evi_bilanz_publications": {
        "description": "EVI publication events with company identifiers and publication metadata.",
        "key_columns": ["publication_date", "publication_type", "company_name", "firmenbuchnummer", "detail_url"],
    },
}


def set_request_user_context(user_context: Optional[Dict[str, Any]]) -> contextvars.Token:
    return _request_user_context.set(user_context or {})


def reset_request_user_context(token: contextvars.Token) -> None:
    try:
        _request_user_context.reset(token)
    except ValueError:
        # Can happen when async generator shutdown crosses task contexts.
        _request_user_context.set(None)


def current_user_profile() -> Dict[str, Any]:
    """Return the authenticated user profile available in the current request context."""
    ctx = _request_user_context.get() or {}
    return {
        "authenticated": bool(ctx),
        "id": ctx.get("id"),
        "email": ctx.get("email"),
        "raw_user": ctx.get("raw_user", {}),
    }


def list_known_tables() -> Dict[str, Any]:
    """List the known data catalog tables with short purpose descriptions."""
    tables = [
        {"table": name, "description": meta["description"]}
        for name, meta in sorted(KNOWN_TABLES.items())
    ]
    return {"ok": True, "count": len(tables), "tables": tables}


def describe_table(table: str) -> Dict[str, Any]:
    """Describe one known table, including its intent and key columns."""
    table_name = (table or "").strip()
    meta = KNOWN_TABLES.get(table_name)
    if not meta:
        return {"ok": False, "error": f"Unknown table '{table_name}'", "known_tables": sorted(KNOWN_TABLES.keys())}
    return {"ok": True, "table": table_name, **meta}


def list_accessible_tables() -> Dict[str, Any]:
    """Check which known tables are currently queryable for this user context."""
    ctx = _request_user_context.get() or {}
    supabase_client = ctx.get("supabase_client")
    if supabase_client is None:
        return {"ok": False, "error": "Supabase client not available in request context"}

    accessible: List[str] = []
    blocked: List[Dict[str, str]] = []
    for table_name in sorted(KNOWN_TABLES.keys()):
        try:
            supabase_client.table(table_name).select("*").limit(1).execute()
            accessible.append(table_name)
        except Exception as exc:
            blocked.append({"table": table_name, "error": str(exc)})
    return {"ok": True, "accessible": accessible, "blocked": blocked}


def _apply_filter(query: Any, column: str, operator: str, value: Any) -> Any:
    op = operator.lower()
    if op == "eq":
        return query.eq(column, value)
    if op == "neq":
        return query.neq(column, value)
    if op == "gt":
        return query.gt(column, value)
    if op == "gte":
        return query.gte(column, value)
    if op == "lt":
        return query.lt(column, value)
    if op == "lte":
        return query.lte(column, value)
    if op == "like":
        return query.like(column, value)
    if op == "ilike":
        return query.ilike(column, value)
    if op == "in":
        if not isinstance(value, list):
            raise ValueError("'in' filter requires a list value")
        return query.in_(column, value)
    if op == "is":
        return query.is_(column, value)
    raise ValueError(f"Unsupported operator '{operator}'")


def supabase_query(
    table: str,
    columns: str = "*",
    filters_json: str = "[]",
    order_by: str = "",
    ascending: bool = True,
    limit: int = 20,
) -> Dict[str, Any]:
    """
    Run a guarded read query on a Supabase table with optional filters, sorting, and row limits.
    filters_json format: [{"column":"name","op":"ilike","value":"%fraunhofer%"}]
    """
    ctx = _request_user_context.get() or {}
    supabase_client = ctx.get("supabase_client")
    if supabase_client is None:
        return {"ok": False, "error": "Supabase client not available in request context"}

    table_name = (table or "").strip()
    selected_columns = (columns or "*").strip()
    if not re.fullmatch(r"[A-Za-z0-9_]+", table_name):
        return {"ok": False, "error": "Invalid table name"}
    if selected_columns != "*" and not re.fullmatch(r"[A-Za-z0-9_,\s]+", selected_columns):
        return {"ok": False, "error": "Invalid columns expression"}

    safe_limit = max(1, min(int(limit), 100))
    try:
        filters = json.loads(filters_json or "[]")
        if not isinstance(filters, list):
            return {"ok": False, "error": "filters_json must decode to a list"}

        query = supabase_client.table(table_name).select(selected_columns)
        for item in filters:
            if not isinstance(item, dict):
                return {"ok": False, "error": "Each filter must be an object"}
            column = str(item.get("column", "")).strip()
            operator = str(item.get("op", "")).strip()
            value = item.get("value")
            if not column or not re.fullmatch(r"[A-Za-z0-9_]+", column):
                return {"ok": False, "error": "Invalid filter column"}
            query = _apply_filter(query, column, operator, value)

        if order_by:
            order_col = order_by.strip()
            if not re.fullmatch(r"[A-Za-z0-9_]+", order_col):
                return {"ok": False, "error": "Invalid order_by column"}
            query = query.order(order_col, desc=not ascending)

        response = query.limit(safe_limit).execute()
        data = getattr(response, "data", None)
        return {
            "ok": True,
            "table": table_name,
            "columns": selected_columns,
            "filters": filters,
            "order_by": order_by or None,
            "ascending": bool(ascending),
            "limit": safe_limit,
            "rows": data or [],
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def _safe_like(value: str) -> str:
    txt = (value or "").strip()
    return f"%{txt}%"


def search_projectfacts(
    name_query: str = "",
    city_query: str = "",
    industry_query: str = "",
    country: str = "",
    segment_country: str = "",
    size: str = "",
    last_activity_after: str = "",
    limit: int = 20,
) -> Dict[str, Any]:
    """
    Explicit search tool for the projectfacts table.
    Supports fuzzy text filters and optional exact filters for segmentation fields.
    """
    ctx = _request_user_context.get() or {}
    supabase_client = ctx.get("supabase_client")
    if supabase_client is None:
        return {"ok": False, "error": "Supabase client not available in request context"}

    safe_limit = max(1, min(int(limit), 100))
    try:
        query = (
            supabase_client.table("projectfacts")
            .select(
                "id,name,city,country,segment_country,industries,size,last_activity_at,company_address,search_text"
            )
        )

        if (name_query or "").strip():
            query = query.or_(
                f"name.ilike.{_safe_like(name_query)},name_norm.ilike.{_safe_like(name_query)},search_text.ilike.{_safe_like(name_query)}"
            )
        if (city_query or "").strip():
            query = query.or_(
                f"city.ilike.{_safe_like(city_query)},city_norm.ilike.{_safe_like(city_query)},search_text.ilike.{_safe_like(city_query)}"
            )
        if (industry_query or "").strip():
            query = query.or_(f"industries.ilike.{_safe_like(industry_query)},search_text.ilike.{_safe_like(industry_query)}")
        if (country or "").strip():
            query = query.eq("country", country.strip())
        if (segment_country or "").strip():
            query = query.eq("segment_country", segment_country.strip())
        if (size or "").strip():
            query = query.ilike("size", size.strip())
        if (last_activity_after or "").strip():
            query = query.gte("last_activity_at", last_activity_after.strip())

        response = query.order("last_activity_at", desc=True).limit(safe_limit).execute()
        rows = getattr(response, "data", None) or []
        return {
            "ok": True,
            "table": "projectfacts",
            "filters": {
                "name_query": name_query,
                "city_query": city_query,
                "industry_query": industry_query,
                "country": country,
                "segment_country": segment_country,
                "size": size,
                "last_activity_after": last_activity_after,
            },
            "limit": safe_limit,
            "rows": rows,
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def search_wko_companies(
    name_query: str = "",
    branche_query: str = "",
    address_query: str = "",
    has_email: Optional[bool] = None,
    has_website: Optional[bool] = None,
    crawled_after: str = "",
    limit: int = 20,
) -> Dict[str, Any]:
    """
    Explicit search tool for the wko_companies table.
    Supports company, branch, and address search plus simple contact-presence filters.
    """
    ctx = _request_user_context.get() or {}
    supabase_client = ctx.get("supabase_client")
    if supabase_client is None:
        return {"ok": False, "error": "Supabase client not available in request context"}

    safe_limit = max(1, min(int(limit), 100))
    try:
        query = supabase_client.table("wko_companies").select(
            "id,branche,name,email,phone,company_website,address,wko_detail_url,crawled_at,search_text"
        )

        if (name_query or "").strip():
            query = query.or_(f"name.ilike.{_safe_like(name_query)},search_text.ilike.{_safe_like(name_query)}")
        if (branche_query or "").strip():
            query = query.ilike("branche", _safe_like(branche_query))
        if (address_query or "").strip():
            query = query.or_(f"address.ilike.{_safe_like(address_query)},search_text.ilike.{_safe_like(address_query)}")
        if has_email is True:
            query = query.not_.is_("email", "null")
        elif has_email is False:
            query = query.is_("email", "null")
        if has_website is True:
            query = query.not_.is_("company_website", "null")
        elif has_website is False:
            query = query.is_("company_website", "null")
        if (crawled_after or "").strip():
            query = query.gte("crawled_at", crawled_after.strip())

        response = query.order("crawled_at", desc=True).limit(safe_limit).execute()
        rows = getattr(response, "data", None) or []
        return {
            "ok": True,
            "table": "wko_companies",
            "filters": {
                "name_query": name_query,
                "branche_query": branche_query,
                "address_query": address_query,
                "has_email": has_email,
                "has_website": has_website,
                "crawled_after": crawled_after,
            },
            "limit": safe_limit,
            "rows": rows,
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def search_wko_branches(
    branche_query: str = "",
    letter: str = "",
    source_query: str = "",
    discovered_after: str = "",
    limit: int = 20,
) -> Dict[str, Any]:
    """
    Explicit search tool for the wko_branches (branchen) table.
    Supports branch-name lookup and optional letter/source/date filters.
    """
    ctx = _request_user_context.get() or {}
    supabase_client = ctx.get("supabase_client")
    if supabase_client is None:
        return {"ok": False, "error": "Supabase client not available in request context"}

    safe_limit = max(1, min(int(limit), 100))
    try:
        query = supabase_client.table("wko_branches").select(
            "id,branche,branch_url,letter,source,discovered_at"
        )
        if (branche_query or "").strip():
            query = query.ilike("branche", _safe_like(branche_query))
        if (letter or "").strip():
            query = query.eq("letter", letter.strip().upper())
        if (source_query or "").strip():
            query = query.ilike("source", _safe_like(source_query))
        if (discovered_after or "").strip():
            query = query.gte("discovered_at", discovered_after.strip())

        response = query.order("discovered_at", desc=True).limit(safe_limit).execute()
        rows = getattr(response, "data", None) or []
        return {
            "ok": True,
            "table": "wko_branches",
            "filters": {
                "branche_query": branche_query,
                "letter": letter,
                "source_query": source_query,
                "discovered_after": discovered_after,
            },
            "limit": safe_limit,
            "rows": rows,
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}
