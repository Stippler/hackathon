from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional

import dspy

from .utils import extract_links_from_obj, safe_dump


class RunLogger:
    def __init__(self, log_path: Optional[str] = None) -> None:
        path = Path(log_path) if log_path else Path("logs") / f"mas_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._fh = path.open("a", encoding="utf-8")
        self.line(f"[session:start] {datetime.now().isoformat()}")
        self.line(f"[session:log_file] {self.path}")

    def line(self, text: str = "") -> None:
        self._fh.write(text + "\n")
        self._fh.flush()

    def chunk(self, text: str) -> None:
        self._fh.write(text)
        self._fh.flush()

    def close(self) -> None:
        self.line(f"[session:end] {datetime.now().isoformat()}")
        self._fh.close()


def print_tool_guide(logger: Optional[RunLogger] = None) -> None:
    lines = [
        "",
        "=" * 90,
        "TOOL GUIDE",
        "=" * 90,
        "- fraunhofer_lscm_focus: strategic profile with capabilities, industries, and acquisition relevance.",
        "- select_rows: generic SELECT-style read with filters, sorting, and row limits.",
        "- evi_lookup: focused EVI lookup by company name, Firmenbuch number, or publication date range.",
        "- distinct_counts: quick frequency distribution for one selected column.",
        "- fuzzy_join: approximate join between two tables using company-name similarity scoring.",
    ]
    for line in lines:
        print(line)
        if logger:
            logger.line(line)


def print_trace(pred: Any, logger: Optional[RunLogger] = None) -> None:
    lines = ["", "=" * 90, "RESULT", "=" * 90, pred.process_result]
    for line in lines:
        print(line)
        if logger:
            logger.line(line)

    traj = getattr(pred, "trajectory", None)
    if not traj:
        return

    for line in ["", "-" * 90, "TOOL TRACE", "-" * 90]:
        print(line)
        if logger:
            logger.line(line)
    i = 0
    while True:
        thought_key = f"thought_{i}"
        tool_key = f"tool_name_{i}"
        args_key = f"tool_args_{i}"
        obs_key = f"observation_{i}"
        if thought_key not in traj and tool_key not in traj:
            break
        step_line = f"\nStep {i + 1}"
        print(step_line)
        if logger:
            logger.line(step_line)
        if thought_key in traj:
            text = f"Thought: {traj.get(thought_key)}"
            print(text)
            if logger:
                logger.line(text)
        if tool_key in traj:
            text = f"Tool: {traj.get(tool_key)}"
            print(text)
            if logger:
                logger.line(text)
        if args_key in traj:
            text = f"Args:\n{safe_dump(traj.get(args_key), max_len=500)}"
            print(text)
            if logger:
                logger.line(text)
        if obs_key in traj:
            obs_dump = safe_dump(traj.get(obs_key), max_len=700)
            text = f"Observation:\n{obs_dump}"
            print(text)
            if logger:
                logger.line(text)
                if "Execution error" in obs_dump:
                    logger.line(f"[tool:error] step={i + 1}")
        i += 1


def enrich_final_result_with_links(pred: Any) -> Any:
    traj = getattr(pred, "trajectory", {}) or {}
    all_links: List[str] = []
    evi_links: List[str] = []
    wko_links: List[str] = []
    for key, value in traj.items():
        if not key.startswith("observation_"):
            continue
        for link in extract_links_from_obj(value):
            all_links.append(link)
            lower = link.lower()
            if "evi.gv.at" in lower:
                evi_links.append(link)
            elif "firmen.wko.at" in lower:
                wko_links.append(link)

    def dedupe(vals: List[str]) -> List[str]:
        out: List[str] = []
        seen = set()
        for v in vals:
            if v in seen:
                continue
            seen.add(v)
            out.append(v)
        return out

    evi_links = dedupe(evi_links)
    wko_links = dedupe(wko_links)
    all_links = dedupe(all_links)
    if "http://" in pred.process_result or "https://" in pred.process_result:
        return pred
    selected = (evi_links[:5] + wko_links[:5])[:8] or all_links[:8]
    if not selected:
        return pred
    lines = ["", "Evidence links:"]
    for idx, link in enumerate(selected, start=1):
        prefix = "EVI" if link in evi_links else ("WKO" if link in wko_links else "SRC")
        lines.append(f"{idx}. [{prefix}] {link}")
    pred.process_result = pred.process_result.rstrip() + "\n" + "\n".join(lines)
    return pred


def run_with_stream(
    agent: dspy.ReAct,
    stream_agent: Any,
    user_request: str,
    history: dspy.History,
    logger: Optional[RunLogger] = None,
):
    if logger:
        logger.line("")
        logger.line("[request:start]")
        logger.line(f"[request:text] {user_request}")
    stream = stream_agent(user_request=user_request, history=history)
    final_pred = None
    in_thought_line = False
    in_result_line = False
    try:
        for chunk in stream:
            if isinstance(chunk, dspy.streaming.StatusMessage):
                if in_thought_line or in_result_line:
                    print()
                    if logger:
                        logger.line()
                    in_thought_line = False
                    in_result_line = False
                print(chunk.message)
                if logger:
                    logger.line(chunk.message)
                continue
            if isinstance(chunk, dspy.streaming.StreamResponse):
                field = chunk.signature_field_name
                if field == "next_thought":
                    if not in_thought_line:
                        print("[thought] ", end="", flush=True)
                        if logger:
                            logger.chunk("[thought] ")
                        in_thought_line = True
                        in_result_line = False
                    print(chunk.chunk, end="", flush=True)
                    if logger:
                        logger.chunk(chunk.chunk)
                elif field == "process_result":
                    if not in_result_line:
                        if in_thought_line:
                            print()
                            if logger:
                                logger.line()
                        print("[draft] ", end="", flush=True)
                        if logger:
                            logger.chunk("[draft] ")
                        in_result_line = True
                        in_thought_line = False
                    print(chunk.chunk, end="", flush=True)
                    if logger:
                        logger.chunk(chunk.chunk)
                continue
            if isinstance(chunk, dspy.Prediction):
                final_pred = chunk
    except Exception as exc:
        if logger:
            logger.line(f"[stream:error] {type(exc).__name__}: {exc}")
        raise
    if in_thought_line or in_result_line:
        print()
        if logger:
            logger.line()
    if final_pred is None:
        if logger:
            logger.line("[fallback] stream produced no final prediction, calling agent directly")
        final_pred = agent(user_request=user_request, history=history)
    enriched = enrich_final_result_with_links(final_pred)
    if logger:
        logger.line("[request:end]")
    return enriched


def run_demo_queries(
    agent: dspy.ReAct,
    stream_agent: Any,
    history: dspy.History,
    logger: Optional[RunLogger] = None,
) -> None:
    print_tool_guide(logger=logger)
    test_queries = [
        (
            "Tell me everything about Fraunhofer. Start with the Fraunhofer profile, then use evi_lookup "
            "with company_name and summarize all returned publications and links."
        ),
        (
            "Show a simple filtered SELECT on projectfacts for names matching Fraunhofer, and summarize results."
        ),
        (
            "Run fuzzy_join between evi_bilanz_publications.company_name and projectfacts.name for query "
            "Fraunhofer. Explain match scores and likely matches."
        ),
    ]
    for idx, query in enumerate(test_queries, start=1):
        for line in ["", "#" * 90, f"TEST QUERY {idx}", "#" * 90, query]:
            print(line)
            if logger:
                logger.line(line)
        pred = run_with_stream(agent=agent, stream_agent=stream_agent, user_request=query, history=history, logger=logger)
        print_trace(pred, logger=logger)
        history.messages.append({"user_request": query, "process_result": pred.process_result})


def run_interactive(
    agent: dspy.ReAct,
    stream_agent: Any,
    history: dspy.History,
    logger: Optional[RunLogger] = None,
) -> None:
    print_tool_guide(logger=logger)
    line = "\nInteractive mode. Type 'exit' to quit.\n"
    print(line)
    if logger:
        logger.line(line.rstrip("\n"))
    while True:
        user = input("You: ").strip()
        if logger:
            logger.line(f"[interactive:user] {user}")
        if user.lower() in {"exit", "quit"}:
            if logger:
                logger.line("[interactive:exit]")
            break
        if not user:
            continue
        pred = run_with_stream(agent=agent, stream_agent=stream_agent, user_request=user, history=history, logger=logger)
        print_trace(pred, logger=logger)
        history.messages.append({"user_request": user, "process_result": pred.process_result})
