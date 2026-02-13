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
