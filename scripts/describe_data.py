#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = ROOT / "scripts"
DATA_DIR = ROOT / "data"


TABLE_RE = re.compile(
    r"create\s+table\s+if\s+not\s+exists\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\((.*?)\);",
    flags=re.IGNORECASE | re.DOTALL,
)
INDEX_RE = re.compile(
    r"create\s+(unique\s+)?index\s+if\s+not\s+exists\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+on\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+(.*?);",
    flags=re.IGNORECASE | re.DOTALL,
)


@dataclass
class Column:
    name: str
    definition: str


@dataclass
class Table:
    name: str
    source_file: Path
    columns: list[Column] = field(default_factory=list)
    constraints: list[str] = field(default_factory=list)
    indexes: list[str] = field(default_factory=list)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a detailed markdown data catalog from schemas, data files, and pipeline code."
    )
    parser.add_argument(
        "--output",
        type=str,
        default="DATA_CATALOG.md",
        help="Output markdown path relative to repo root.",
    )
    parser.add_argument(
        "--sample-rows",
        type=int,
        default=1000,
        help="Max valid rows to inspect for JSONL field-level statistics.",
    )
    parser.add_argument(
        "--include-local-files",
        action="store_true",
        help="Include local file profiling section (disabled by default).",
    )
    return parser.parse_args()


def split_sql_items(block: str) -> list[str]:
    items: list[str] = []
    current: list[str] = []
    depth = 0
    for ch in block:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1

        if ch == "," and depth == 0:
            item = "".join(current).strip()
            if item:
                items.append(item)
            current = []
            continue
        current.append(ch)
    tail = "".join(current).strip()
    if tail:
        items.append(tail)
    return items


def parse_schema_file(path: Path) -> dict[str, Table]:
    sql_text = path.read_text(encoding="utf-8")
    tables: dict[str, Table] = {}

    for match in TABLE_RE.finditer(sql_text):
        table_name = match.group(1)
        body = match.group(2)
        table = Table(name=table_name, source_file=path)

        for item in split_sql_items(body):
            clean = " ".join(item.strip().split())
            lowered = clean.lower()
            if not clean:
                continue
            if lowered.startswith(("constraint ", "primary key", "unique ", "foreign key", "check ")):
                table.constraints.append(clean)
                continue

            parts = clean.split(maxsplit=1)
            col_name = parts[0]
            col_def = parts[1] if len(parts) > 1 else ""
            table.columns.append(Column(name=col_name, definition=col_def))

        tables[table_name] = table

    for idx_match in INDEX_RE.finditer(sql_text):
        unique_kw = "UNIQUE " if idx_match.group(1) else ""
        idx_name = idx_match.group(2)
        table_name = idx_match.group(3)
        tail = " ".join(idx_match.group(4).split())
        line = f"{unique_kw}INDEX {idx_name} ON {table_name} {tail}"
        if table_name in tables:
            tables[table_name].indexes.append(line)

    return tables


def parse_csv_file(path: Path) -> dict[str, Any]:
    data_rows = 0
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader, [])
        for row in reader:
            if row and any(cell.strip() for cell in row):
                data_rows += 1
    return {
        "kind": "csv",
        "columns": header,
        "rows": data_rows,
    }


def parse_json_file(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"kind": "json", "error": str(exc)}

    if isinstance(payload, dict):
        out: dict[str, Any] = {"kind": "json", "top_level_keys": list(payload.keys())}
        for key, value in payload.items():
            if isinstance(value, list) and value and isinstance(value[0], dict):
                out[f"{key}_item_keys"] = sorted({k for row in value[:10] for k in row.keys()})
                out[f"{key}_count"] = len(value)
            elif isinstance(value, list):
                out[f"{key}_count"] = len(value)
        return out

    if isinstance(payload, list):
        keys: set[str] = set()
        for item in payload[:100]:
            if isinstance(item, dict):
                keys.update(item.keys())
        return {"kind": "json", "list_item_keys": sorted(keys), "list_size": len(payload)}

    return {"kind": "json", "value_type": type(payload).__name__}


def parse_datetime_value(value: Any) -> datetime | None:
    if value is None:
        return None
    txt = str(value).strip()
    if not txt:
        return None

    date_formats = (
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%d.%m.%Y",
    )
    if txt.endswith("Z"):
        txt = txt[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(txt)
    except ValueError:
        pass
    for fmt in date_formats:
        try:
            return datetime.strptime(txt, fmt)
        except ValueError:
            continue
    return None


def parse_jsonl_file(path: Path, sample_rows: int) -> dict[str, Any]:
    keys: set[str] = set()
    key_counts: dict[str, int] = {}
    sampled = 0
    total_nonempty_lines = 0
    total_valid_rows = 0
    parse_errors = 0
    date_min: dict[str, datetime] = {}
    date_max: dict[str, datetime] = {}

    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            total_nonempty_lines += 1
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                parse_errors += 1
                continue
            total_valid_rows += 1
            if isinstance(row, dict):
                if sampled < sample_rows:
                    for key, value in row.items():
                        keys.add(key)
                        if value is not None and str(value).strip() != "":
                            key_counts[key] = key_counts.get(key, 0) + 1

                        if key.endswith("_at") or "date" in key.lower():
                            dt = parse_datetime_value(value)
                            if dt is None:
                                continue
                            if key not in date_min or dt < date_min[key]:
                                date_min[key] = dt
                            if key not in date_max or dt > date_max[key]:
                                date_max[key] = dt
                    sampled += 1

    key_coverage: dict[str, float] = {}
    if sampled > 0:
        for key in sorted(keys):
            key_coverage[key] = round((key_counts.get(key, 0) / sampled) * 100.0, 2)

    date_ranges: dict[str, str] = {}
    for key in sorted(date_min.keys()):
        date_ranges[key] = f"{date_min[key].isoformat()} -> {date_max[key].isoformat()}"

    return {
        "kind": "jsonl",
        "total_nonempty_lines": total_nonempty_lines,
        "total_valid_rows": total_valid_rows,
        "sampled_rows": sampled,
        "detected_keys": sorted(keys),
        "key_coverage_percent_in_sample": key_coverage,
        "date_ranges_in_sample": date_ranges,
        "json_parse_errors_in_sample": parse_errors,
    }


def collect_data_files() -> list[Path]:
    files: list[Path] = []
    if DATA_DIR.exists():
        files.extend(DATA_DIR.rglob("*.json"))
        files.extend(DATA_DIR.rglob("*.jsonl"))
        files.extend(DATA_DIR.rglob("*.csv"))
        files.extend(DATA_DIR.rglob("*.xlsx"))
    files.extend(ROOT.glob("*.json"))
    files.extend(ROOT.glob("*.jsonl"))
    files.extend(ROOT.glob("*.csv"))
    files.extend(ROOT.glob("*.xlsx"))
    deduped = sorted(set(files))
    return deduped


def file_metadata(path: Path) -> dict[str, Any]:
    st = path.stat()
    return {
        "bytes": st.st_size,
        "modified_at": datetime.fromtimestamp(st.st_mtime).isoformat(),
    }


def infer_data_pipeline() -> list[dict[str, Any]]:
    # Hand-curated flow based on crawler/ and scripts/ implementation.
    return [
        {
            "step": "Discover WKO branches",
            "code": "crawler/branch_catalog.py",
            "inputs": ["https://firmen.wko.at/branchen.aspx"],
            "outputs": ["data/wko_branch_catalog.json"],
            "notes": "Crawls A-Z branch index, deduplicates branch/url pairs, stores crawl metadata.",
        },
        {
            "step": "Rate branch crawl priority",
            "code": "crawler/branch_rating.py",
            "inputs": ["data/wko_branch_catalog.json", "data/crawl_state.json (if exists)"],
            "outputs": ["data/wko_branch_ratings.json"],
            "notes": "Scores branches by keyword relevance, freshness, historical yield, and denied-access penalty.",
        },
        {
            "step": "Continuously crawl WKO companies",
            "code": "crawler/continuous_crawler.py",
            "inputs": ["data/wko_branch_catalog.json", "data/wko_branch_ratings.json", "WKO website pages"],
            "outputs": [
                "data/out/companies_continuous.jsonl",
                "data/crawl_state.json",
                "data/out/companies_dedupe.sqlite",
            ],
            "notes": "Uses adaptive loop + load-more paging, local SQLite dedupe, optional Supabase upsert to wko_companies.",
        },
        {
            "step": "Crawl EVI bilanz publications",
            "code": "crawler/evi_bilanz.py",
            "inputs": ["https://www.evi.gv.at/s?suche=Bilanz"],
            "outputs": ["data/out/evi_bilanz.jsonl"],
            "notes": "Crawls paginated search results, extracts publication metadata and company fields.",
        },
        {
            "step": "Create DB schema for WKO",
            "code": "scripts/setup_wko_schema.sql",
            "inputs": ["Supabase/Postgres"],
            "outputs": ["public.wko_companies", "public.wko_branches"],
            "notes": "Creates tables, unique constraints, and trigram/text indexes.",
        },
        {
            "step": "Import WKO to DB",
            "code": "scripts/import_wko_companies.py",
            "inputs": [
                "data/out/companies_continuous.jsonl (default candidate)",
                "data/wko_branch_catalog.json (optional branch import)",
            ],
            "outputs": ["public.wko_companies", "public.wko_branches"],
            "notes": "Normalizes fields, computes wko_key hash, upserts to Supabase with conflict handling.",
        },
        {
            "step": "Create DB schema for EVI",
            "code": "scripts/setup_evi_schema.sql",
            "inputs": ["Supabase/Postgres"],
            "outputs": ["public.evi_bilanz_publications"],
            "notes": "Creates table plus unique and search indexes.",
        },
        {
            "step": "Import EVI to DB",
            "code": "scripts/import_evi_bilanz.py",
            "inputs": ["data/out/evi_bilanz.jsonl (default candidate)"],
            "outputs": ["public.evi_bilanz_publications"],
            "notes": "Parses publication_date, builds evi_key hash, upserts by evi_key.",
        },
        {
            "step": "Create DB schema for projectfacts",
            "code": "scripts/setup_projectfacts_schema.sql",
            "inputs": ["Supabase/Postgres"],
            "outputs": ["public.projectfacts"],
            "notes": "Creates table with normalized/search columns and trigram indexes.",
        },
        {
            "step": "Import projectfacts to DB",
            "code": "scripts/import_projectfacts.py",
            "inputs": ["data/projectfacts.xlsx or data/out/projectfacts.xlsx"],
            "outputs": ["public.projectfacts"],
            "notes": "Maps German column names, normalizes text, computes pf_key hash, upserts into Supabase.",
        },
    ]


def render_markdown(
    tables: dict[str, Table],
    schema_files: list[Path],
    data_profile: dict[Path, dict[str, Any]],
    sample_rows: int,
    include_local_files: bool,
) -> str:
    lines: list[str] = []
    lines.append("# Data Catalog")
    lines.append("")
    lines.append("Auto-generated by `scripts/describe_data.py`.")
    lines.append("")
    lines.append("## Executive Snapshot")
    lines.append("")
    lines.append(f"- SQL tables discovered: **{len(tables)}**")
    if include_local_files:
        lines.append(f"- Local data files profiled: **{len(data_profile)}**")
        lines.append(f"- JSONL sample size for field stats: **{sample_rows}** rows")
    lines.append("")

    lines.append("## Data Creation Pipeline")
    lines.append("")
    lines.append("Derived from `crawler/` and `scripts/` code paths.")
    lines.append("")
    for item in infer_data_pipeline():
        lines.append(f"### {item['step']}")
        lines.append("")
        lines.append(f"- Code: `{item['code']}`")
        lines.append(f"- Inputs: `{', '.join(item['inputs'])}`")
        lines.append(f"- Outputs: `{', '.join(item['outputs'])}`")
        lines.append(f"- Notes: {item['notes']}")
        lines.append("")

    lines.append("## SQL Schemas")
    lines.append("")
    lines.append("Scanned schema files:")
    for schema_path in schema_files:
        lines.append(f"- `{schema_path.relative_to(ROOT)}`")
    lines.append("")
    lines.append(f"Detected tables: **{len(tables)}**")
    lines.append("")

    for table_name in sorted(tables.keys()):
        table = tables[table_name]
        lines.append(f"### `{table.name}`")
        lines.append("")
        lines.append(f"- Source schema: `{table.source_file.relative_to(ROOT)}`")
        lines.append(f"- Columns: {len(table.columns)}")
        if table.constraints:
            lines.append(f"- Table constraints: {len(table.constraints)}")
        if table.indexes:
            lines.append(f"- Indexes: {len(table.indexes)}")
        lines.append("")
        lines.append("| Column | Definition |")
        lines.append("|---|---|")
        for col in table.columns:
            lines.append(f"| `{col.name}` | `{col.definition}` |")
        lines.append("")
        if table.constraints:
            lines.append("Constraints:")
            for constraint in table.constraints:
                lines.append(f"- `{constraint}`")
            lines.append("")
        if table.indexes:
            lines.append("Indexes:")
            for index in table.indexes:
                lines.append(f"- `{index}`")
            lines.append("")

    if include_local_files:
        lines.append("## Local Data Files")
        lines.append("")
        lines.append(
            "Profiles below are inferred from file structure and filesystem metadata."
        )
        lines.append("")
        lines.append(f"Detected files: **{len(data_profile)}**")
        lines.append("")

        for path in sorted(data_profile.keys()):
            rel = path.relative_to(ROOT)
            info = data_profile[path]
            kind = info.get("kind", "unknown")
            meta = file_metadata(path)
            lines.append(f"### `{rel}`")
            lines.append("")
            lines.append(f"- Type: `{kind}`")
            lines.append(f"- Size bytes: `{meta['bytes']}`")
            lines.append(f"- Modified at: `{meta['modified_at']}`")

            if "error" in info:
                lines.append(f"- Error: `{info['error']}`")
                lines.append("")
                continue

            if kind == "csv":
                columns = info.get("columns", [])
                lines.append(f"- Columns ({len(columns)}): `{', '.join(columns)}`")
                lines.append(f"- Data rows: `{info.get('rows', 0)}`")
            elif kind == "json":
                if "top_level_keys" in info:
                    keys = info["top_level_keys"]
                    lines.append(f"- Top-level keys ({len(keys)}): `{', '.join(keys)}`")
                if "list_item_keys" in info:
                    keys = info["list_item_keys"]
                    lines.append(f"- List item keys ({len(keys)}): `{', '.join(keys)}`")
                for key, value in info.items():
                    if key.endswith("_item_keys") and isinstance(value, list):
                        label = key.replace("_item_keys", "")
                        lines.append(f"- `{label}` item keys ({len(value)}): `{', '.join(value)}`")
                    if key.endswith("_count"):
                        label = key.replace("_count", "")
                        lines.append(f"- `{label}` count: `{value}`")
            elif kind == "jsonl":
                lines.append(f"- Non-empty lines: `{info.get('total_nonempty_lines', 0)}`")
                lines.append(f"- Valid JSON rows: `{info.get('total_valid_rows', 0)}`")
                lines.append(f"- Sampled rows: `{info.get('sampled_rows', 0)}`")
                keys = info.get("detected_keys", [])
                lines.append(f"- Detected keys ({len(keys)}): `{', '.join(keys)}`")
                lines.append(f"- JSON parse errors: `{info.get('json_parse_errors_in_sample', 0)}`")
                coverage = info.get("key_coverage_percent_in_sample", {})
                if coverage:
                    lines.append("- Key non-empty coverage in sample:")
                    for key in sorted(coverage.keys()):
                        lines.append(f"  - `{key}`: `{coverage[key]}%`")
                date_ranges = info.get("date_ranges_in_sample", {})
                if date_ranges:
                    lines.append("- Date ranges in sample:")
                    for key in sorted(date_ranges.keys()):
                        lines.append(f"  - `{key}`: `{date_ranges[key]}`")
            elif kind == "xlsx":
                lines.append("- Binary spreadsheet file; columns are inferred by importer script.")
            lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def main() -> None:
    args = parse_args()
    schema_files = sorted(SCRIPTS_DIR.glob("setup_*_schema.sql"))
    all_tables: dict[str, Table] = {}
    for schema_file in schema_files:
        all_tables.update(parse_schema_file(schema_file))

    data_profile: dict[Path, dict[str, Any]] = {}
    if args.include_local_files:
        for file_path in collect_data_files():
            suffix = file_path.suffix.lower()
            if suffix == ".csv":
                data_profile[file_path] = parse_csv_file(file_path)
            elif suffix == ".json":
                data_profile[file_path] = parse_json_file(file_path)
            elif suffix == ".jsonl":
                data_profile[file_path] = parse_jsonl_file(file_path, sample_rows=max(1, args.sample_rows))
            elif suffix == ".xlsx":
                data_profile[file_path] = {"kind": "xlsx"}

    markdown = render_markdown(
        tables=all_tables,
        schema_files=schema_files,
        data_profile=data_profile,
        sample_rows=max(1, args.sample_rows),
        include_local_files=args.include_local_files,
    )

    output_path = (ROOT / args.output).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(markdown, encoding="utf-8")
    print(f"Wrote data catalog: {output_path}")


if __name__ == "__main__":
    main()
