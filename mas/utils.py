import ast
import datetime as dt
import json
import math
import re
from difflib import SequenceMatcher
from typing import Any, List, Optional


def clean(text: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def ilike_pattern(text: str) -> str:
    cleaned = clean(text)
    return "%" if not cleaned else f"%{cleaned}%"


def safe_dump(obj: Any, max_len: int = 900) -> str:
    try:
        text = json.dumps(obj, ensure_ascii=False, default=str, indent=2)
    except Exception:
        text = str(obj)
    if len(text) <= max_len:
        return text
    return text[:max_len] + "\n... [truncated]"


def extract_links_from_obj(obj: Any) -> List[str]:
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
    out: List[str] = []
    seen = set()
    for link in links:
        if link in seen:
            continue
        seen.add(link)
        out.append(link.rstrip(".,);"))
    return out


def norm_name(name: Optional[str]) -> str:
    text = clean(name).lower()
    text = text.replace("&", " und ")
    text = re.sub(r"\b(gmbh|ag|kg|og|mbh|ges\.?m\.?b\.?h\.?)\b", " ", text)
    text = re.sub(r"[^a-z0-9äöüß\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def name_similarity(a: Optional[str], b: Optional[str]) -> float:
    na = norm_name(a)
    nb = norm_name(b)
    if not na or not nb:
        return 0.0
    return SequenceMatcher(None, na, nb).ratio()


def country_is_dach(country: Optional[str]) -> bool:
    c = clean(country).lower()
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


def keyword_variants(text: str) -> List[str]:
    base = clean(text).lower()
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

    seen = set()
    out: List[str] = []
    for v in variants:
        if v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


def _safe_eval_expr(expression: str) -> float:
    allowed_bin_ops = (ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Pow, ast.Mod)
    allowed_unary_ops = (ast.UAdd, ast.USub)
    allowed_funcs = {
        "sqrt": math.sqrt,
        "sin": math.sin,
        "cos": math.cos,
        "tan": math.tan,
        "log": math.log,
        "exp": math.exp,
        "abs": abs,
        "round": round,
    }
    allowed_names = {"pi": math.pi, "e": math.e}

    def evaluate(node: ast.AST) -> float:
        if isinstance(node, ast.Expression):
            return evaluate(node.body)
        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
            return float(node.value)
        if isinstance(node, ast.BinOp) and isinstance(node.op, allowed_bin_ops):
            left = evaluate(node.left)
            right = evaluate(node.right)
            if isinstance(node.op, ast.Add):
                return left + right
            if isinstance(node.op, ast.Sub):
                return left - right
            if isinstance(node.op, ast.Mult):
                return left * right
            if isinstance(node.op, ast.Div):
                return left / right
            if isinstance(node.op, ast.Pow):
                return left**right
            return left % right
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, allowed_unary_ops):
            value = evaluate(node.operand)
            return +value if isinstance(node.op, ast.UAdd) else -value
        if isinstance(node, ast.Name) and node.id in allowed_names:
            return float(allowed_names[node.id])
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            fn = allowed_funcs.get(node.func.id)
            if fn is None:
                raise ValueError(f"Function '{node.func.id}' is not allowed")
            args = [evaluate(arg) for arg in node.args]
            return float(fn(*args))
        raise ValueError("Unsupported expression")

    parsed = ast.parse(expression, mode="eval")
    return evaluate(parsed)


def calculator(expression: str) -> dict[str, Any]:
    """Safely evaluate arithmetic expressions (for example `2*(5+3)` or `sqrt(81)`)."""
    expr = (expression or "").strip()
    if not expr:
        return {"ok": False, "error": "Missing expression"}
    try:
        value = _safe_eval_expr(expr)
        return {"ok": True, "expression": expr, "value": value}
    except Exception as exc:
        return {"ok": False, "expression": expr, "error": str(exc)}


def current_datetime(timezone: str = "UTC") -> dict[str, str]:
    """Return the current ISO-8601 datetime in UTC (default) or local timezone."""
    tz = (timezone or "UTC").strip().lower()
    if tz == "local":
        now = dt.datetime.now().astimezone()
    else:
        now = dt.datetime.now(tz=dt.timezone.utc)
        tz = "utc"
    return {"timezone": tz.upper(), "iso": now.isoformat()}
