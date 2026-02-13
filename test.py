from mas.cli import main


if __name__ == "__main__":
    main()
import argparse
import datetime as dt
import json
import os
import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

import dspy
from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError, model_validator
from supabase import create_client


def _clean(text: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def _ilike_pattern(text: str) -> str:
    cleaned = _clean(text)
    return "%" if not cleaned else f"%{cleaned}%"


def _safe_dump(obj: Any, max_len: int = 900) -> str:
    try:
        text = json.dumps(obj, ensure_ascii=False, default=str, indent=2)
    except Exception:
        text = str(obj)
    if len(text) <= max_len:
        return text
    return text[:max_len] + "\n... [truncated]"


def _extract_links_from_obj(obj: Any) -> List[str]:
    links: List[str] = []
    pattern = re.compile(r"https?://[^\s\"'>]+")

    def walk(value: Any) -> None:
        if value is None:
            return
        if isinstance(value, str):
            links.extend(pattern.findall(value))
            return
        if isinstance(value, dict):
            for v in value.values():
                walk(v)
            return
        if isinstance(value, list):
            for v in value:
                walk(v)
            return

    walk(obj)
    # dedupe preserve order
    out: List[str] = []
    seen = set()
    for link in links:
        if link in seen:
            continue
        seen.add(link)
        out.append(link.rstrip(".,);"))
    return out


def _norm_name(name: Optional[str]) -> str:
    text = _clean(name).lower()
    text = text.replace("&", " und ")
    text = re.sub(r"\b(gmbh|ag|kg|og|mbh|ges\.?m\.?b\.?h\.?)\b", " ", text)
    text = re.sub(r"[^a-z0-9äöüß\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _name_similarity(a: Optional[str], b: Optional[str]) -> float:
    na = _norm_name(a)
    nb = _norm_name(b)
    if not na or not nb:
        return 0.0
    return SequenceMatcher(None, na, nb).ratio()


def _country_is_dach(country: Optional[str]) -> bool:
    c = _clean(country).lower()
    if not c:
        return False
    dach_tokens = {
        "austria",
        "österreich",
        "germany",
        "deutschland",
        "switzerland",
        "schweiz",
    }
    return any(tok in c for tok in dach_tokens)


FRAUNHOFER_LSCM_PROFILE = {
    "focus_areas": [
        {
            "name": "Strategische Supply-Chain- und Netzwerkplanung",
            "keywords": ["netzwerkplanung", "standort", "reshoring", "nearshoring", "milp", "resilienz"],
        },
        {
            "name": "Datengetriebene Planung und Bestandsoptimierung",
            "keywords": ["bestandsoptimierung", "bedarfsprognose", "disposition", "simulation", "forecasting"],
        },
        {
            "name": "Lagerplanung, Automatisierung und Intralogistik",
            "keywords": ["lagerplanung", "intralogistik", "materialfluss", "layout", "automatisierung"],
        },
        {
            "name": "Mobile Robotik und FTS",
            "keywords": ["fts", "agv", "mobile robotik", "cobot", "automatisierte verladung"],
        },
        {
            "name": "Ersatzteil- und Instandhaltungsmanagement",
            "keywords": ["ersatzteil", "predictive maintenance", "ausfallprognose", "instandhaltung"],
        },
        {
            "name": "Stammdaten-Optimierung und KI-Enablement",
            "keywords": ["stammdaten", "datenqualität", "ki-agenten", "web scraping", "kpi"],
        },
    ],
    "target_industries": [
        "Industrie und Maschinenbau",
        "Handel und Großhandel",
        "Logistikdienstleister",
        "Energie und Infrastruktur",
        "Baustoffindustrie",
        "Produktion",
    ],
    "acquisition_intent": (
        "Suche nach Unternehmen, die Fraunhofer-LSCM-nahe Kompetenzen ergänzen: "
        "SCM-Analytics, Optimierung, Intralogistik, Automatisierung, Predictive Maintenance, "
        "Daten-/KI-Enablement."
    ),
}


def fraunhofer_lscm_focus() -> Dict[str, Any]:
    """
    Return Fraunhofer LSCM focus areas and keywords derived from leistungsangebot.md.
    Use this tool first to anchor acquisition reasoning.
    """
    return FRAUNHOFER_LSCM_PROFILE


def _keyword_variants(text: str) -> List[str]:
    """
    Very small bilingual normalization for common acquisition-search terms.
    Helps when user asks in English but WKO branch labels are German.
    """
    base = _clean(text).lower()
    if not base:
        return [""]

    variants = [base]
    substitutions = {
        "waste": ["abfall", "entsorgung"],
        "recycling": ["recycling", "verwertung"],
        "environmental services": ["umwelt", "entsorgung", "abfall"],
        "machinery": ["maschinenbau"],
    }
    for src, targets in substitutions.items():
        if src in base:
            for tgt in targets:
                variants.append(base.replace(src, tgt))
                variants.append(tgt)

    # preserve order, remove duplicates
    seen = set()
    out: List[str] = []
    for v in variants:
        if v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


@dataclass
class GrablinDB:
    """Minimal Supabase query layer for acquisition scouting."""

    url: str
    key: str

    def __post_init__(self) -> None:
        self.sb = create_client(self.url, self.key)

    # --- WKO branches ---
    def wko_list_branches(
        self,
        limit: int = 80,
        letter: Optional[str] = None,
        query: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Return a small branch subset only. For discovery, prefer query/letter filtering.
        """
        args = BranchQueryArgs(limit=limit, letter=letter, query=query)
        q = (
            self.sb.table("wko_branches")
            .select("branche,branch_url,letter,discovered_at")
            .order("branche", desc=False)
            .limit(args.limit)
        )
        if args.letter:
            q = q.eq("letter", _clean(args.letter)[:1].upper())
        if args.query:
            q = q.ilike("branche", _ilike_pattern(args.query))
        res = q.execute()
        rows = res.data or []
        return {
            "rows": rows,
            "count": len(rows),
            "filters": {"letter": args.letter, "query": args.query},
        }

    def wko_match_branch(self, query: str, limit: int = 15) -> Dict[str, Any]:
        q = _clean(query)
        if not q:
            return {"query": q, "candidates": []}
        all_candidates: List[Dict[str, Any]] = []
        seen = set()

        for variant in _keyword_variants(q):
            res = (
                self.sb.table("wko_branches")
                .select("branche,branch_url,letter")
                .ilike("branche", _ilike_pattern(variant))
                .limit(limit)
                .execute()
            )
            for row in res.data or []:
                key = (row.get("branche"), row.get("branch_url"))
                if key in seen:
                    continue
                seen.add(key)
                all_candidates.append(row)
            if len(all_candidates) >= limit:
                break
        return {"query": q, "candidates": all_candidates[:limit]}

    # --- WKO companies ---
    def wko_search_companies(
        self,
        text: str,
        branch: Optional[str] = None,
        limit: int = 30,
        offset: int = 0,
        only_with_website: bool = False,
        only_with_email: bool = False,
    ) -> Dict[str, Any]:
        q = _clean(text)
        rows: List[Dict[str, Any]] = []
        seen = set()

        for variant in _keyword_variants(q) if q else [""]:
            query = (
                self.sb.table("wko_companies")
                .select(
                    "name,branche,address,street,zip_city,email,phone,company_website,"
                    "wko_detail_url,crawled_at"
                )
                .order("crawled_at", desc=True)
            )
            if variant:
                query = query.ilike("search_text", _ilike_pattern(variant))
            if branch:
                query = query.eq("branche", branch)
            if only_with_website:
                query = query.not_.is_("company_website", "null")
            if only_with_email:
                query = query.not_.is_("email", "null")
            query = query.range(max(0, offset), max(0, offset) + min(50, limit) - 1)
            res = query.execute()
            for row in res.data or []:
                key = (row.get("name"), row.get("wko_detail_url"))
                if key in seen:
                    continue
                seen.add(key)
                rows.append(row)
            if len(rows) >= min(50, limit):
                break

        return {
            "query": q,
            "branch": branch,
            "rows": rows[: min(50, limit)],
            "count": len(rows[: min(50, limit)]),
            "filters": {
                "only_with_website": only_with_website,
                "only_with_email": only_with_email,
            },
        }

    def wko_unique_values(
        self,
        column: str = "branche",
        limit: int = 30,
        min_count: int = 1,
        query: Optional[str] = None,
        scan_limit: int = 10000,
    ) -> Dict[str, Any]:
        """
        Get frequent unique values from wko_companies for a chosen column.
        Useful for exploratory profiling without using the branch table.
        """
        allowed = {"branche", "zip_city", "street", "company_website", "email"}
        col = _clean(column)
        if col not in allowed:
            raise ValueError(f"Unsupported column '{column}'. Allowed: {sorted(allowed)}")

        rows = (
            self.sb.table("wko_companies")
            .select(col)
            .limit(min(20000, max(100, scan_limit)))
            .execute()
            .data
            or []
        )

        counts: Dict[str, int] = {}
        q = _clean(query).lower()
        for row in rows:
            val = _clean(row.get(col))
            if not val:
                continue
            if q and q not in val.lower():
                continue
            counts[val] = counts.get(val, 0) + 1

        items = [{"value": v, "count": c} for v, c in counts.items() if c >= max(1, min_count)]
        items.sort(key=lambda x: x["count"], reverse=True)
        return {
            "column": col,
            "rows": items[: min(200, max(1, limit))],
            "count": len(items[: min(200, max(1, limit))]),
            "scanned": len(rows),
            "filters": {"min_count": min_count, "query": query},
        }

    def wko_count_by_branch(
        self,
        limit: int = 50,
        min_count: int = 1,
        query: Optional[str] = None,
        scan_limit: int = 10000,
    ) -> Dict[str, Any]:
        """
        Count companies per branch directly from wko_companies.
        """
        rows = (
            self.sb.table("wko_companies")
            .select("branche")
            .limit(min(20000, max(100, scan_limit)))
            .execute()
            .data
            or []
        )

        q = _clean(query).lower()
        counts: Dict[str, int] = {}
        for row in rows:
            branche = _clean(row.get("branche"))
            if not branche:
                continue
            if q and q not in branche.lower():
                continue
            counts[branche] = counts.get(branche, 0) + 1

        items = [{"branche": b, "count": c} for b, c in counts.items() if c >= max(1, min_count)]
        items.sort(key=lambda x: x["count"], reverse=True)
        return {
            "rows": items[: min(200, max(1, limit))],
            "count": len(items[: min(200, max(1, limit))]),
            "scanned": len(rows),
            "filters": {"min_count": min_count, "query": query},
        }

    # --- Projectfacts ---
    def pf_search(
        self,
        text: str,
        limit: int = 20,
        offset: int = 0,
        city: Optional[str] = None,
        industry: Optional[str] = None,
        size: Optional[str] = None,
    ) -> Dict[str, Any]:
        q = _clean(text)
        query = self.sb.table("projectfacts").select(
            "name,company_address,city,state,country,industries,size,last_activity_at,last_changed_at"
        )
        if q:
            query = query.ilike("search_text", _ilike_pattern(q))
        if city:
            query = query.ilike("city", _ilike_pattern(city))
        if industry:
            query = query.ilike("industries", _ilike_pattern(industry))
        if size:
            query = query.ilike("size", _ilike_pattern(size))
        query = query.order("last_activity_at", desc=True).range(
            max(0, offset), max(0, offset) + min(50, limit) - 1
        )
        res = query.execute()
        rows = res.data or []
        return {
            "query": q,
            "rows": rows,
            "count": len(rows),
            "filters": {"city": city, "industry": industry, "size": size},
        }

    # --- EVI ---
    def evi_search_publications(
        self,
        text: str,
        limit: int = 20,
        offset: int = 0,
        company_name: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        firmenbuchnummer: Optional[str] = None,
    ) -> Dict[str, Any]:
        args = EviSearchArgs(
            text=text,
            limit=limit,
            offset=offset,
            company_name=company_name,
            date_from=date_from,
            date_to=date_to,
            firmenbuchnummer=firmenbuchnummer,
        )
        q = args.text
        query = self.sb.table("evi_bilanz_publications").select(
            "publication_date,publication_type,company_name,firmenbuchnummer,"
            "detail_url,crawled_at"
        )
        if q:
            query = query.ilike("search_text", _ilike_pattern(q))
        if args.company_name:
            query = query.ilike("company_name", _ilike_pattern(args.company_name))
        if args.firmenbuchnummer:
            query = query.eq("firmenbuchnummer", _clean(args.firmenbuchnummer))
        if args.date_from:
            query = query.gte("publication_date", args.date_from)
        if args.date_to:
            query = query.lte("publication_date", args.date_to)
        query = query.order("publication_date", desc=True).range(
            max(0, args.offset), max(0, args.offset) + min(50, args.limit) - 1
        )
        res = query.execute()
        rows = res.data or []
        return {
            "query": args.text,
            "rows": rows,
            "count": len(rows),
            "filters": {
                "company_name": args.company_name,
                "date_from": args.date_from,
                "date_to": args.date_to,
                "firmenbuchnummer": args.firmenbuchnummer,
            },
        }

    def evi_candidate_companies(
        self,
        text: str = "",
        min_records: int = 2,
        limit: int = 15,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        EVI-first candidate discovery.
        Aggregates EVI publication rows to company-level candidates using count + recency.
        """
        args = EviCandidatesArgs(
            text=text,
            min_records=min_records,
            limit=limit,
            date_from=date_from,
            date_to=date_to,
        )

        # Pull a moderate window and aggregate client-side.
        fetch_limit = min(400, max(120, args.limit * 20))
        raw_rows = self.evi_search_publications(
            text=args.text,
            limit=fetch_limit,
            offset=0,
            date_from=args.date_from,
            date_to=args.date_to,
        )["rows"]

        agg: Dict[str, Dict[str, Any]] = {}
        for row in raw_rows:
            name = _clean(row.get("company_name"))
            if not name:
                continue

            cur = agg.get(name)
            pub_date = row.get("publication_date")
            detail_url = row.get("detail_url")
            fb = row.get("firmenbuchnummer")
            if cur is None:
                agg[name] = {
                    "company_name": name,
                    "publication_count": 1,
                    "latest_publication_date": pub_date,
                    "firmenbuchnummers": [fb] if fb else [],
                    "evi_links": [detail_url] if detail_url else [],
                }
                continue

            cur["publication_count"] += 1
            if pub_date and (
                not cur.get("latest_publication_date")
                or str(pub_date) > str(cur.get("latest_publication_date"))
            ):
                cur["latest_publication_date"] = pub_date
            if fb and fb not in cur["firmenbuchnummers"]:
                cur["firmenbuchnummers"].append(fb)
            if detail_url and detail_url not in cur["evi_links"]:
                cur["evi_links"].append(detail_url)

        candidates = [
            c for c in agg.values() if c.get("publication_count", 0) >= args.min_records
        ]
        candidates.sort(
            key=lambda x: (x.get("publication_count", 0), str(x.get("latest_publication_date") or "")),
            reverse=True,
        )

        trimmed: List[Dict[str, Any]] = []
        for c in candidates[: args.limit]:
            entry = dict(c)
            entry["evi_links"] = entry.get("evi_links", [])[:5]
            entry["firmenbuchnummers"] = entry.get("firmenbuchnummers", [])[:5]
            trimmed.append(entry)

        return {
            "query": args.text,
            "rows": trimmed,
            "count": len(trimmed),
            "min_records": args.min_records,
            "scanned_rows": len(raw_rows),
            "filters": {"date_from": args.date_from, "date_to": args.date_to},
        }

    # --- Cross-source helper ---
    def company_snapshot(
        self,
        company_query: str,
        limit_per_source: int = 6,
        include_wko: bool = False,
    ) -> Dict[str, Any]:
        """
        Compact cross-table lookup for one company phrase.
        Default is EVI+Projectfacts (WKO optional).
        """
        q = _clean(company_query)
        pf = self.pf_search(text=q, limit=limit_per_source)
        evi = self.evi_search_publications(text=q, limit=limit_per_source)
        evi_links = [r.get("detail_url") for r in evi["rows"] if r.get("detail_url")]
        wko = {"rows": [], "count": 0}
        wko_links: List[str] = []
        if include_wko:
            wko = self.wko_search_companies(text=q, limit=limit_per_source)
            wko_links = [r.get("wko_detail_url") for r in wko["rows"] if r.get("wko_detail_url")]

        return {
            "query": q,
            "wko_count": wko["count"],
            "projectfacts_count": pf["count"],
            "evi_count": evi["count"],
            "evi_links": evi_links[:limit_per_source],
            "wko_links": wko_links[:limit_per_source],
            "wko": wko["rows"],
            "projectfacts": pf["rows"],
            "evi": evi["rows"],
        }

    def evi_projectfacts_candidates(
        self,
        text: str = "",
        min_records: int = 2,
        limit: int = 12,
        missing_only: bool = False,
        similarity_threshold: float = 0.78,
        prioritize_dach: bool = True,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Primary practical tool:
        1) Discover candidates from EVI (count + recency),
        2) Check if they already exist in projectfacts,
        3) Return actionable list with links.
        """
        evi_candidates = self.evi_candidate_companies(
            text=text,
            min_records=min_records,
            limit=limit * 2,
            date_from=date_from,
            date_to=date_to,
        )["rows"]

        rows: List[Dict[str, Any]] = []
        for cand in evi_candidates:
            cname = cand.get("company_name", "")
            pf = self.pf_search(text=cname, limit=5)
            pf_rows = pf.get("rows", [])

            strong_matches = []
            for pr in pf_rows:
                pname = pr.get("name", "")
                if not pname:
                    continue
                sim = _name_similarity(cname, pname)
                if sim >= similarity_threshold:
                    m = dict(pr)
                    m["similarity"] = round(sim, 4)
                    strong_matches.append(m)
                    continue
                # Substring fallback for legal suffix noise.
                norm_c = _norm_name(cname)
                norm_p = _norm_name(pname)
                if norm_c and (norm_c in norm_p or norm_p in norm_c):
                    strong_matches.append(pr)

            exists = len(strong_matches) > 0
            if missing_only and exists:
                continue

            # DACH evidence heuristics
            dach_evidence: List[str] = []
            dach_projectfacts = any(_country_is_dach(m.get("country")) for m in strong_matches)
            if dach_projectfacts:
                dach_evidence.append("projectfacts_country_dach")

            # If WKO has a strong fuzzy match, treat as likely Austria (thus DACH)
            wko_hits = self.wko_search_companies(text=cname, limit=5).get("rows", [])
            best_wko_score = 0.0
            best_wko = None
            for wh in wko_hits:
                score = _name_similarity(cname, wh.get("name"))
                if score > best_wko_score:
                    best_wko_score = score
                    best_wko = wh
            dach_wko = best_wko is not None and best_wko_score >= similarity_threshold
            if dach_wko:
                dach_evidence.append("wko_match_likely_at")

            dach_priority = 1 if (dach_projectfacts or dach_wko) else 0

            rows.append(
                {
                    "company_name": cname,
                    "publication_count": cand.get("publication_count", 0),
                    "latest_publication_date": cand.get("latest_publication_date"),
                    "evi_links": cand.get("evi_links", []),
                    "firmenbuchnummers": cand.get("firmenbuchnummers", []),
                    "exists_in_projectfacts": exists,
                    "projectfacts_match_count": len(strong_matches),
                    "dach_priority": dach_priority,
                    "dach_evidence": dach_evidence,
                    "wko_match_count": 1 if dach_wko else 0,
                    "wko_best_match_score": round(best_wko_score, 4),
                    "wko_best_match_name": best_wko.get("name") if best_wko else None,
                    "projectfacts_matches": [
                        {
                            "name": m.get("name"),
                            "city": m.get("city"),
                            "country": m.get("country"),
                            "industries": m.get("industries"),
                            "size": m.get("size"),
                            "last_activity_at": m.get("last_activity_at"),
                        }
                        for m in strong_matches[:3]
                    ],
                }
            )

            if len(rows) >= limit:
                break

        if prioritize_dach:
            rows.sort(
                key=lambda r: (
                    r.get("dach_priority", 0),
                    r.get("publication_count", 0),
                    str(r.get("latest_publication_date") or ""),
                ),
                reverse=True,
            )

        return {
            "query": text,
            "count": len(rows),
            "rows": rows[:limit],
            "filters": {
                "min_records": min_records,
                "missing_only": missing_only,
                "similarity_threshold": similarity_threshold,
                "prioritize_dach": prioritize_dach,
                "date_from": date_from,
                "date_to": date_to,
            },
        }

    def fuzzy_join_evi_presence(
        self,
        text: str = "",
        min_records: int = 2,
        limit: int = 12,
        similarity_threshold: float = 0.76,
        include_wko: bool = True,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Fuzzy join EVI candidates against projectfacts and optionally WKO.
        Returns best matches + scores for quick coverage checks.
        """
        evi_candidates = self.evi_candidate_companies(
            text=text,
            min_records=min_records,
            limit=limit,
            date_from=date_from,
            date_to=date_to,
        ).get("rows", [])

        joined_rows: List[Dict[str, Any]] = []
        for cand in evi_candidates:
            cname = cand.get("company_name")
            if not cname:
                continue

            # Projectfacts best match
            pf_hits = self.pf_search(text=cname, limit=8).get("rows", [])
            best_pf = None
            best_pf_score = 0.0
            for hit in pf_hits:
                score = _name_similarity(cname, hit.get("name"))
                if score > best_pf_score:
                    best_pf_score = score
                    best_pf = hit

            pf_matched = best_pf is not None and best_pf_score >= similarity_threshold

            # WKO best match (optional)
            best_wko = None
            best_wko_score = 0.0
            wko_matched = False
            if include_wko:
                wko_hits = self.wko_search_companies(text=cname, limit=8).get("rows", [])
                for hit in wko_hits:
                    score = _name_similarity(cname, hit.get("name"))
                    if score > best_wko_score:
                        best_wko_score = score
                        best_wko = hit
                wko_matched = best_wko is not None and best_wko_score >= similarity_threshold

            joined_rows.append(
                {
                    "company_name": cname,
                    "publication_count": cand.get("publication_count", 0),
                    "latest_publication_date": cand.get("latest_publication_date"),
                    "evi_links": cand.get("evi_links", []),
                    "firmenbuchnummers": cand.get("firmenbuchnummers", []),
                    "projectfacts_match": {
                        "matched": pf_matched,
                        "score": round(best_pf_score, 4),
                        "name": best_pf.get("name") if best_pf else None,
                        "city": best_pf.get("city") if best_pf else None,
                        "last_activity_at": best_pf.get("last_activity_at") if best_pf else None,
                    },
                    "wko_match": {
                        "matched": wko_matched,
                        "score": round(best_wko_score, 4),
                        "name": best_wko.get("name") if best_wko else None,
                        "branche": best_wko.get("branche") if best_wko else None,
                        "wko_detail_url": best_wko.get("wko_detail_url") if best_wko else None,
                    },
                }
            )

        return {
            "query": text,
            "count": len(joined_rows),
            "rows": joined_rows,
            "filters": {
                "min_records": min_records,
                "similarity_threshold": similarity_threshold,
                "include_wko": include_wko,
                "date_from": date_from,
                "date_to": date_to,
            },
        }


def build_db_from_env() -> GrablinDB:
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")
    if not url or not key:
        raise RuntimeError(
            "Missing SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_ANON_KEY) in .env"
        )
    return GrablinDB(url=url, key=key)


class BranchQueryArgs(BaseModel):
    limit: int = Field(default=80, ge=1, le=200)
    letter: Optional[str] = None
    query: Optional[str] = None

    @model_validator(mode="after")
    def check_discovery_scope(self) -> "BranchQueryArgs":
        # Discourage huge unfiltered branch dumps; force narrow intent.
        if not self.query and not self.letter and self.limit > 80:
            raise ValueError("Unfiltered branch listing is capped; provide query or letter for bigger lists.")
        return self


class EviSearchArgs(BaseModel):
    text: str = ""
    limit: int = Field(default=20, ge=1, le=50)
    offset: int = Field(default=0, ge=0)
    company_name: Optional[str] = None
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    firmenbuchnummer: Optional[str] = None

    @model_validator(mode="after")
    def validate_dates(self) -> "EviSearchArgs":
        def parse_iso(d: Optional[str]) -> Optional[dt.date]:
            if not d:
                return None
            return dt.date.fromisoformat(d)

        frm = parse_iso(self.date_from)
        to = parse_iso(self.date_to)
        if frm and to and frm > to:
            raise ValueError("date_from must be <= date_to")
        return self


class EviCandidatesArgs(BaseModel):
    text: str = ""
    min_records: int = Field(default=2, ge=1, le=50)
    limit: int = Field(default=15, ge=1, le=50)
    date_from: Optional[str] = None
    date_to: Optional[str] = None

    @model_validator(mode="after")
    def validate_dates(self) -> "EviCandidatesArgs":
        def parse_iso(d: Optional[str]) -> Optional[dt.date]:
            if not d:
                return None
            return dt.date.fromisoformat(d)

        frm = parse_iso(self.date_from)
        to = parse_iso(self.date_to)
        if frm and to and frm > to:
            raise ValueError("date_from must be <= date_to")
        return self


class ScoutStatusProvider(dspy.streaming.StatusMessageProvider):
    def module_start_status_message(self, instance, inputs):
        name = instance.__class__.__name__
        return f"[module:start] {name}"

    def module_end_status_message(self, outputs):
        return "[module:end]"

    def tool_start_status_message(self, instance, inputs):
        tool_name = getattr(instance, "name", getattr(instance, "__name__", "tool"))
        return f"[tool:start] {tool_name} args={_safe_dump(inputs, max_len=220)}"

    def tool_end_status_message(self, outputs):
        return f"[tool:end] result={_safe_dump(outputs, max_len=260)}"


def build_agent(db: GrablinDB) -> Tuple[dspy.ReAct, Any]:
    tools = {
        "fraunhofer_lscm_focus": fraunhofer_lscm_focus,
        "wko_unique_values": db.wko_unique_values,
        "wko_count_by_branch": db.wko_count_by_branch,
        "pf_search": db.pf_search,
        "evi_search_publications": db.evi_search_publications,
        "evi_candidate_companies": db.evi_candidate_companies,
        "evi_projectfacts_candidates": db.evi_projectfacts_candidates,
        "fuzzy_join_evi_presence": db.fuzzy_join_evi_presence,
        "company_snapshot": db.company_snapshot,
    }

    class AcquisitionScout(dspy.Signature):
        """
        You are an acquisition scouting assistant grounded in internal tables.

        Available sources:
        - fraunhofer_lscm_focus: Fraunhofer LSCM strategic capability profile (acts as domain context).
        - evi_bilanz_publications: publication activity by company (name, fb number, links).
        - projectfacts: profile data incl. industries/size and last_activity_at.
        - wko_companies: optional enrichment + profiling via unique/count tools.

        Rules:
        - Use tools for all company claims; do not invent entities.
        - Start with fraunhofer_lscm_focus to anchor strategy and keyword themes.
        - Use evi_projectfacts_candidates as the default first tool.
        - Prioritize DACH-region companies first (DE/AT/CH signals), then others.
        - For each shortlisted company, explicitly state: DACH signal, existence in projectfacts, and evidence links.
        - Primary output should be: which EVI companies already exist in projectfacts and which are missing.
        - Prefer EVI-backed candidates and note publication recency when available.
        - If evidence is sparse, provide directional/general statements and explicitly call out data gaps.
        - For fuzzy matching questions, use fuzzy_join_evi_presence and report similarity scores.
        - If user asks for branch distribution or unique values, use wko_count_by_branch / wko_unique_values.
        - Ignore wko_branches table; derive branch insights from wko_companies only.
        - Use company_snapshot only for deeper drill-down on 1-2 candidates.
        - Keep final output practical and concise: 3-8 rows.
        - Include evidence links in every final answer (EVI first; WKO only if explicitly used).
        """

        user_request: str = dspy.InputField()
        history: dspy.History = dspy.InputField()
        process_result: str = dspy.OutputField(
            desc="Grounded summary with concrete candidates, EVI-prioritized evidence, links, and next actions."
        )

    # LM setup
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("Missing OPENAI_API_KEY in .env")
    dspy.configure(lm=dspy.LM("openai/gpt-5.2"))

    react_agent = dspy.ReAct(
        AcquisitionScout,
        tools=[
            tools["fraunhofer_lscm_focus"],
            tools["wko_unique_values"],
            tools["wko_count_by_branch"],
            tools["pf_search"],
            tools["evi_search_publications"],
            tools["evi_candidate_companies"],
            tools["evi_projectfacts_candidates"],
            tools["fuzzy_join_evi_presence"],
            tools["company_snapshot"],
        ],
        max_iters=12,
    )
    stream_listeners = [
        # ReAct's iterative internal field; allow reuse to stream each iteration.
        dspy.streaming.StreamListener(signature_field_name="next_thought", allow_reuse=True),
        # Final answer field from our output signature.
        dspy.streaming.StreamListener(signature_field_name="process_result"),
    ]
    stream_agent = dspy.streamify(
        react_agent,
        status_message_provider=ScoutStatusProvider(),
        stream_listeners=stream_listeners,
        async_streaming=False,
    )
    return react_agent, stream_agent


def print_trace(pred: Any) -> None:
    print("\n" + "=" * 90)
    print("RESULT")
    print("=" * 90)
    print(pred.process_result)

    if hasattr(pred, "reasoning") and pred.reasoning:
        print("\n" + "-" * 90)
        print("REASONING")
        print("-" * 90)
        print(pred.reasoning)

    traj = getattr(pred, "trajectory", None)
    if not traj:
        return

    print("\n" + "-" * 90)
    print("TOOL TRACE")
    print("-" * 90)

    i = 0
    while True:
        thought_key = f"thought_{i}"
        tool_key = f"tool_name_{i}"
        args_key = f"tool_args_{i}"
        obs_key = f"observation_{i}"

        if thought_key not in traj and tool_key not in traj:
            break

        print(f"\nStep {i + 1}")
        if thought_key in traj:
            print(f"Thought: {traj.get(thought_key)}")
        if tool_key in traj:
            print(f"Tool: {traj.get(tool_key)}")
        if args_key in traj:
            print(f"Args:\n{_safe_dump(traj.get(args_key), max_len=500)}")
        if obs_key in traj:
            print(f"Observation:\n{_safe_dump(traj.get(obs_key), max_len=700)}")

        i += 1


def _run_with_stream(agent: dspy.ReAct, stream_agent: Any, user_request: str, history: dspy.History):
    stream = stream_agent(user_request=user_request, history=history)
    final_pred = None

    in_thought_line = False
    in_result_line = False
    for chunk in stream:
        if isinstance(chunk, dspy.streaming.StatusMessage):
            if in_thought_line or in_result_line:
                print()
                in_thought_line = False
                in_result_line = False
            print(chunk.message)
            continue

        if isinstance(chunk, dspy.streaming.StreamResponse):
            field = chunk.signature_field_name
            if field == "next_thought":
                if not in_thought_line:
                    print("[thought] ", end="", flush=True)
                    in_thought_line = True
                    in_result_line = False
                print(chunk.chunk, end="", flush=True)
            elif field == "process_result":
                if not in_result_line:
                    if in_thought_line:
                        print()
                    print("[draft] ", end="", flush=True)
                    in_result_line = True
                    in_thought_line = False
                print(chunk.chunk, end="", flush=True)
            continue

        if isinstance(chunk, dspy.Prediction):
            final_pred = chunk

    if in_thought_line or in_result_line:
        print()

    # Safety fallback if stream didn't emit final prediction for some reason.
    if final_pred is None:
        final_pred = agent(user_request=user_request, history=history)
    final_pred = _enrich_final_result_with_links(final_pred)
    return final_pred


def _enrich_final_result_with_links(pred: Any) -> Any:
    """
    Guarantee final answer includes evidence links.
    Prefer EVI links first, then WKO links.
    """
    traj = getattr(pred, "trajectory", {}) or {}
    all_links: List[str] = []
    evi_links: List[str] = []
    wko_links: List[str] = []

    for key, value in traj.items():
        if not key.startswith("observation_"):
            continue
        links = _extract_links_from_obj(value)
        for link in links:
            all_links.append(link)
            lower = link.lower()
            if "evi.gv.at" in lower:
                evi_links.append(link)
            elif "firmen.wko.at" in lower:
                wko_links.append(link)

    # dedupe while preserving order
    def dedupe(vals: List[str]) -> List[str]:
        out: List[str] = []
        seen = set()
        for x in vals:
            if x in seen:
                continue
            seen.add(x)
            out.append(x)
        return out

    evi_links = dedupe(evi_links)
    wko_links = dedupe(wko_links)
    all_links = dedupe(all_links)

    if "http://" in pred.process_result or "https://" in pred.process_result:
        return pred

    selected = (evi_links[:5] + wko_links[:5])[:8]
    if not selected and all_links:
        selected = all_links[:8]
    if not selected:
        return pred

    link_lines = ["", "Evidence links:"]
    for idx, link in enumerate(selected, start=1):
        prefix = "EVI" if link in evi_links else ("WKO" if link in wko_links else "SRC")
        link_lines.append(f"{idx}. [{prefix}] {link}")
    pred.process_result = pred.process_result.rstrip() + "\n" + "\n".join(link_lines)
    return pred


def run_demo_queries(agent: dspy.ReAct, stream_agent: Any, history: dspy.History) -> None:
    test_queries = [
        (
            "Using the Fraunhofer LSCM service profile, give a general acquisition landscape statement: "
            "which capability areas look most relevant right now, and how current EVI evidence maps to companies "
            "already present vs missing in projectfacts. Prioritize DACH candidates first."
        ),
        (
            "Create a generic strategic watchlist for Fraunhofer LSCM across 3 areas "
            "(e.g., network planning, intralogistics/automation, data & AI enablement), "
            "with DACH companies prioritized. Use EVI plus fuzzy matching to summarize medium-confidence evidence."
        ),
        (
            "Write an executive quarterly scouting summary for Fraunhofer LSCM: "
            "typical acquisition themes, DACH-first company signals from EVI, projectfacts coverage gaps, "
            "and concrete next validation steps. Include evidence links."
        ),
    ]

    for idx, query in enumerate(test_queries, start=1):
        print("\n" + "#" * 90)
        print(f"TEST QUERY {idx}")
        print("#" * 90)
        print(query)

        pred = _run_with_stream(agent=agent, stream_agent=stream_agent, user_request=query, history=history)
        print_trace(pred)

        history.messages.append({"user_request": query, "process_result": pred.process_result})


def run_interactive(agent: dspy.ReAct, stream_agent: Any, history: dspy.History) -> None:
    print("\nInteractive mode. Type 'exit' to quit.\n")
    while True:
        user = input("You: ").strip()
        if user.lower() in {"exit", "quit"}:
            break
        if not user:
            continue
        pred = _run_with_stream(agent=agent, stream_agent=stream_agent, user_request=user, history=history)
        print_trace(pred)
        history.messages.append({"user_request": user, "process_result": pred.process_result})


def main() -> None:
    parser = argparse.ArgumentParser(description="DSPy acquisition scouting test harness")
    parser.add_argument(
        "--mode",
        choices=["demo", "interactive"],
        default="demo",
        help="Run predefined tests or chat interactively.",
    )
    args = parser.parse_args()

    db = build_db_from_env()
    # Validate once early so any schema-style issues fail fast at startup.
    _ = BranchQueryArgs()
    _ = EviSearchArgs()

    agent, stream_agent = build_agent(db)
    history = dspy.History(messages=[])

    if args.mode == "interactive":
        run_interactive(agent, stream_agent, history)
    else:
        run_demo_queries(agent, stream_agent, history)


if __name__ == "__main__":
    main()

