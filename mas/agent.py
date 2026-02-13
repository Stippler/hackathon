import asyncio
import contextlib
import json
import logging
import os
import re
import threading
from contextlib import contextmanager
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

import dspy
from mas.db import (
    current_user_profile,
    describe_table,
    list_accessible_tables,
    list_known_tables,
    reset_request_user_context,
    set_request_user_context,
    supabase_query,
)
from mas.db_ofb import (
    ofb_company_full_view,
    ofb_find_companies_missing_financials,
    ofb_joined_company_screen,
    ofb_list_tables,
    ofb_source_overview,
)
from mas.fraunhofer import (
    fraunhofer_area_details,
    fraunhofer_industries,
    fraunhofer_methods,
    fraunhofer_overview,
    fraunhofer_project_types,
    fraunhofer_service_areas,
    fraunhofer_value_drivers,
)
from mas.openfirmenbuch import (
    ofb_get_company_profile,
    ofb_get_company_report,
    ofb_get_financials_multiple,
    ofb_get_management_roles,
    ofb_get_register_extract,
    ofb_search_company_compressed,
)
from mas.utils import calculator, current_datetime

DEFAULT_MODEL = "openai/gpt-5.2"
logging.getLogger("dspy.utils.callback").setLevel(logging.ERROR)


@contextmanager
def dspy_lm_context(model_name: str):
    """
    Isolate DSPy LM state per agent run to avoid cross-talk in parallel tasks.
    """
    lm = dspy.LM(model_name, cache=False)

    if hasattr(dspy, "settings") and hasattr(dspy.settings, "context"):
        with dspy.settings.context(lm=lm):
            yield
        return

    global_lock = getattr(dspy_lm_context, "_lock", None)
    if global_lock is None:
        dspy_lm_context._lock = threading.Lock()
        global_lock = dspy_lm_context._lock
    with global_lock:
        dspy.configure(lm=lm)
        yield


def projectfacts_query(
    columns: str = "*",
    filters_json: str = "[]",
    order_by: str = "",
    ascending: bool = True,
    limit: int = 20,
) -> Dict[str, Any]:
    """Query only the projectfacts table."""
    return supabase_query(
        table="projectfacts",
        columns=columns,
        filters_json=filters_json,
        order_by=order_by,
        ascending=ascending,
        limit=limit,
    )


def wko_query(
    columns: str = "*",
    filters_json: str = "[]",
    order_by: str = "",
    ascending: bool = True,
    limit: int = 20,
) -> Dict[str, Any]:
    """Query only the wko_companies table."""
    return supabase_query(
        table="wko_companies",
        columns=columns,
        filters_json=filters_json,
        order_by=order_by,
        ascending=ascending,
        limit=limit,
    )


def evi_query(
    columns: str = "*",
    filters_json: str = "[]",
    order_by: str = "",
    ascending: bool = True,
    limit: int = 20,
) -> Dict[str, Any]:
    """Query only the evi_bilanz_publications table."""
    return supabase_query(
        table="evi_bilanz_publications",
        columns=columns,
        filters_json=filters_json,
        order_by=order_by,
        ascending=ascending,
        limit=limit,
    )


class ManagerQA(dspy.Signature):
    """
    Project manager/orchestrator agent.
    Responsibilities:
    - Decide whether the request is simple enough for a short direct answer.
    - If data work is needed, delegate to specialist agents (projectfacts, wko, evi, ofb).
    - Synthesize specialist outputs into one coherent final answer.
    - Do not perform deep table-level analysis itself when specialists are available.
    """

    user_request: str = dspy.InputField()
    history: dspy.History = dspy.InputField()
    process_result: str = dspy.OutputField()


class ProjectfactsQA(dspy.Signature):
    """
    Specialist agent for the projectfacts dataset only.
    Use only projectfacts-focused tools and produce concise findings.
    Preferred tools:
    - `projectfacts_query` for table-scoped retrieval from `projectfacts`.
    - `ofb_search_company_compressed` only when a registry mapping hint is needed.
    Focus:
    - company profile attributes (city/country/industries/size/activity).
    - concise, evidence-based summaries from retrieved rows.
    """

    user_request: str = dspy.InputField()
    history: dspy.History = dspy.InputField()
    process_result: str = dspy.OutputField()


class WkoQA(dspy.Signature):
    """
    Specialist agent for WKO company directory data only.
    Use only WKO-focused tools and produce concise findings.
    Preferred tools:
    - `wko_query` for table-scoped retrieval from `wko_companies`.
    - `ofb_search_company_compressed` only when mapping names to firmennummer candidates.
    Focus:
    - branches, contact points, addresses, and WKO detail links.
    """

    user_request: str = dspy.InputField()
    history: dspy.History = dspy.InputField()
    process_result: str = dspy.OutputField()


class EviQA(dspy.Signature):
    """
    Specialist agent for EVI publication data only.
    Use only EVI-focused tools and produce concise findings.
    Preferred tools:
    - `evi_query` for table-scoped retrieval from `evi_bilanz_publications`.
    - `ofb_search_company_compressed` only when linking publications to registry entities.
    Focus:
    - publication timeline, types, detail URLs, and firmenbuch hints.
    """

    user_request: str = dspy.InputField()
    history: dspy.History = dspy.InputField()
    process_result: str = dspy.OutputField()


class OfbQA(dspy.Signature):
    """
    Specialist agent for OpenFirmenbuch data and tools.
    Use OFB tools for register extracts, financials, and joined OFB analytics.
    Preferred tools:
    - OFB joined DB tools: `ofb_joined_company_screen`, `ofb_company_full_view`, `ofb_source_overview`, `ofb_list_tables`.
    - OFB endpoint tools: `ofb_search_company_compressed`, `ofb_get_register_extract`, `ofb_get_financials_multiple`,
      `ofb_get_company_profile`, `ofb_get_management_roles`, `ofb_get_company_report`.
    - `supabase_query` as fallback for explicit table checks.
    Focus:
    - firmennummer-centric register + financial analysis and KPI/revenue filtering.
    """

    user_request: str = dspy.InputField()
    history: dspy.History = dspy.InputField()
    process_result: str = dspy.OutputField()



class AgentStatusProvider(dspy.streaming.StatusMessageProvider):
    def module_start_status_message(self, instance: Any, inputs: Any):
        name = instance.__class__.__name__
        return f"[module:start] {name}"

    def module_end_status_message(self, outputs: Any):
        return "[module:end]"

    def lm_start_status_message(self, instance: Any, inputs: Any):
        return "[lm:start]"

    def lm_end_status_message(self, outputs: Any):
        return "[lm:end]"

    def tool_start_status_message(self, instance: Any, inputs: Any):
        tool_name = getattr(instance, "name", getattr(instance, "__name__", "tool"))
        args = inputs if isinstance(inputs, dict) else {"value": str(inputs)}
        return f"[tool:start] {tool_name} args={json.dumps(args, ensure_ascii=False, default=str)}"

    def tool_end_status_message(self, outputs: Any):
        summary: Dict[str, Any] = {"ok": True}
        if isinstance(outputs, dict):
            if "ok" in outputs:
                summary["ok"] = bool(outputs.get("ok"))
            if "error" in outputs and outputs.get("error"):
                summary["error"] = str(outputs.get("error"))
            if "table" in outputs:
                summary["table"] = outputs.get("table")
            if isinstance(outputs.get("rows"), list):
                summary["rows_count"] = len(outputs.get("rows") or [])
            if isinstance(outputs.get("docs"), list):
                summary["docs_count"] = len(outputs.get("docs") or [])
        else:
            summary["preview"] = str(outputs)[:120]
        return f"[tool:end] {json.dumps(summary, ensure_ascii=False, default=str)}"


SPECIALIST_SPAWN_ORDER = ("projectfacts", "wko", "evi", "ofb")
SPECIALIST_PROMPTS: Dict[str, str] = {
    "projectfacts": (
        "You are the PROJECTFACTS specialist.\n"
        "Scope: query and analyze `projectfacts` only.\n"
        "Goal: extract company profile insights (city/country/industries/size/activity) relevant to the manager task.\n"
        "Rules: do not invent data, cite concrete rows/fields, keep output concise.\n\n"
        "Manager reason: {reason}\n"
        "Original user request:\n{request}\n"
    ),
    "wko": (
        "You are the WKO specialist.\n"
        "Scope: query and analyze `wko_companies` only.\n"
        "Goal: extract branch/contact/address/detail-link evidence relevant to the manager task.\n"
        "Rules: do not invent data, keep output concise and evidence-first.\n\n"
        "Manager reason: {reason}\n"
        "Original user request:\n{request}\n"
    ),
    "evi": (
        "You are the EVI specialist.\n"
        "Scope: query and analyze `evi_bilanz_publications` only.\n"
        "Goal: extract publication timeline/type/detail-url evidence relevant to the manager task.\n"
        "Rules: do not invent data, keep output concise and evidence-first.\n\n"
        "Manager reason: {reason}\n"
        "Original user request:\n{request}\n"
    ),
    "ofb": (
        "You are the OPENFIRMENBUCH specialist.\n"
        "Scope: use OFB tables/tools for firmennummer-centric register and financial analysis.\n"
        "Goal: return high-value OFB findings (identity, legal form, management, revenue/KPIs, and matches).\n"
        "Rules: do not invent data, keep output concise and evidence-first.\n\n"
        "Manager reason: {reason}\n"
        "Original user request:\n{request}\n"
    ),
}


def _build_specialist_prompt(agent_id: str, user_request: str, reason: str) -> str:
    template = SPECIALIST_PROMPTS.get(agent_id)
    if not template:
        return user_request
    return template.format(reason=reason, request=user_request)


def _manager_plan(question: str) -> Dict[str, str]:
    text = (question or "").lower()
    if len(text.strip()) < 30 and not any(k in text for k in ("projectfacts", "wko", "evi", "ofb", "firmenbuch")):
        return {}

    plan: Dict[str, str] = {}
    if any(k in text for k in ("projectfacts", "industr", "city", "country", "size", "segment")):
        plan["projectfacts"] = "Need structured company profile fields from projectfacts."
    if any(k in text for k in ("wko", "branche", "phone", "email", "website", "kontakt")):
        plan["wko"] = "Need WKO registry/contact enrichment."
    if any(k in text for k in ("evi", "publication", "veroeff", "detail_url", "bilanz publication")):
        plan["evi"] = "Need publication timeline and evidence links from EVI."
    if any(k in text for k in ("ofb", "firmenbuch", "firmennummer", "fnr", "revenue", "umsatz", "bilanz", "kennzahl", "euid")):
        plan["ofb"] = "Need OpenFirmenbuch register/financial insights."

    if not plan and any(k in text for k in ("company", "companies", "firma", "firmen", "vergleich", "compare", "join")):
        plan = {
            "projectfacts": "General company query benefits from profile enrichment.",
            "wko": "General company query benefits from WKO metadata.",
            "evi": "General company query benefits from publication metadata.",
        }
    return plan


def _tools_for_specialist(agent_id: str) -> List[Any]:
    common_tools: List[Any] = [
        calculator,
        current_datetime,
        list_known_tables,
        describe_table,
        list_accessible_tables,
    ]
    if agent_id == "projectfacts":
        return common_tools + [
            projectfacts_query,
            # Optional helper to map profile names to official registry candidates.
            ofb_search_company_compressed,
        ]
    if agent_id == "wko":
        return common_tools + [
            wko_query,
            # Optional helper for FNR candidate resolution from WKO names.
            ofb_search_company_compressed,
        ]
    if agent_id == "evi":
        return common_tools + [
            evi_query,
            # Optional helper for linking EVI names to registry identities.
            ofb_search_company_compressed,
        ]
    if agent_id == "ofb":
        return common_tools + [
            supabase_query,
            ofb_list_tables,
            ofb_source_overview,
            ofb_joined_company_screen,
            ofb_company_full_view,
            ofb_find_companies_missing_financials,
            ofb_search_company_compressed,
            ofb_get_register_extract,
            ofb_get_financials_multiple,
            ofb_get_company_profile,
            ofb_get_management_roles,
            ofb_get_company_report,
        ]
    return []


def _signature_for_specialist(agent_id: str) -> Any:
    if agent_id == "projectfacts":
        return ProjectfactsQA
    if agent_id == "wko":
        return WkoQA
    if agent_id == "evi":
        return EviQA
    if agent_id == "ofb":
        return OfbQA
    return ManagerQA


async def _run_specialist_streaming_async(
    agent_id: str,
    reason: str,
    user_request: str,
    history: dspy.History,
    user_context: Optional[Dict[str, Any]],
    queue: "asyncio.Queue[Dict[str, Any]]",
    model_name: Optional[str] = None,
) -> Dict[str, Any]:
    chosen_model = model_name or os.getenv("DSPY_MODEL", DEFAULT_MODEL)
    await queue.put({"type": "trace_token", "agent_id": "manager", "data": {"text": f"Starting `{agent_id}`: {reason}\n"}})
    await queue.put(
        {
            "type": "trace_token",
            "agent_id": agent_id,
            "data": {"text": f"[working] {agent_id} agent started.\n"},
        }
    )

    ctx_token = set_request_user_context(user_context)
    output_stream = None
    final_answer = ""
    final_prediction = None
    try:
        with dspy_lm_context(chosen_model):
            _, stream_agent = _create_specialist_stream_agent(agent_id)
            output_stream = stream_agent(user_request=user_request, history=history)

            async for chunk in output_stream:
                if isinstance(chunk, dspy.streaming.StatusMessage):
                    message = chunk.message or ""
                    if message.startswith("[tool:start]"):
                        rest = message.replace("[tool:start]", "", 1).strip()
                        tool_name, _, args_part = rest.partition(" args=")
                        args: Dict[str, Any] = {}
                        if args_part:
                            try:
                                parsed = json.loads(args_part)
                                args = parsed if isinstance(parsed, dict) else {"value": parsed}
                            except Exception:
                                args = {"raw": args_part}
                        await queue.put({"type": "tool_start", "agent_id": agent_id, "data": {"tool": tool_name, "args": args}})
                        continue

                    if message.startswith("[tool:end]"):
                        summary_part = message.replace("[tool:end]", "", 1).strip()
                        summary: Dict[str, Any] = {}
                        if summary_part:
                            try:
                                parsed = json.loads(summary_part)
                                if isinstance(parsed, dict):
                                    summary = parsed
                            except Exception:
                                summary = {"raw": summary_part}
                        await queue.put({"type": "tool_end", "agent_id": agent_id, "data": summary})
                        if "rows_count" in summary or "docs_count" in summary:
                            await queue.put({"type": "retrieve", "agent_id": agent_id, "data": summary})
                        continue

                    await queue.put({"type": "trace_token", "agent_id": agent_id, "data": {"text": f"{message}\n"}})
                    continue

                if isinstance(chunk, dspy.streaming.StreamResponse):
                    if chunk.signature_field_name == "process_result":
                        token = chunk.chunk or ""
                        if token:
                            final_answer += token
                            await queue.put({"type": "answer_token", "agent_id": agent_id, "data": {"text": token}})
                    continue

                if isinstance(chunk, dspy.Prediction):
                    final_prediction = chunk

        if not final_answer and final_prediction is not None:
            final_answer = (final_prediction.process_result or "").strip()
        final_answer = final_answer.strip() or "No findings."
        await queue.put({"type": "final", "agent_id": agent_id, "data": {"answer": final_answer}})
        return {"ok": True, "summary": final_answer}
    except asyncio.CancelledError:
        if output_stream is not None:
            with contextlib.suppress(Exception):
                await output_stream.aclose()
        raise
    except Exception as exc:
        await queue.put({"type": "error", "agent_id": agent_id, "data": {"message": str(exc)}})
        return {"ok": False, "error": str(exc)}
    finally:
        reset_request_user_context(ctx_token)


async def _stream_parallel_specialists_async(
    user_request: str,
    history: dspy.History,
    user_context: Optional[Dict[str, Any]],
    plan: Dict[str, str],
    model_name: Optional[str] = None,
) -> AsyncIterator[Dict[str, Any]]:
    queue: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue()
    planned_items = [(agent_id, reason) for agent_id, reason in plan.items() if agent_id in SPECIALIST_PROMPTS]
    planned_items.sort(key=lambda item: SPECIALIST_SPAWN_ORDER.index(item[0]) if item[0] in SPECIALIST_SPAWN_ORDER else 999)
    tasks = [
        asyncio.create_task(
            _run_specialist_streaming_async(
                agent_id=agent_id,
                reason=reason,
                user_request=_build_specialist_prompt(agent_id=agent_id, user_request=user_request, reason=reason),
                history=history,
                user_context=user_context,
                queue=queue,
                model_name=model_name,
            )
        )
        for agent_id, reason in planned_items
    ]
    try:
        pending = len(tasks)
        while pending > 0 or not queue.empty():
            try:
                event = await asyncio.wait_for(queue.get(), timeout=0.15)
                yield event
            except asyncio.TimeoutError:
                pending = sum(1 for t in tasks if not t.done())

        results = await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()

    specialist_sections: List[str] = []
    for idx, (agent_id, _) in enumerate(planned_items):
        item = results[idx]
        if isinstance(item, Exception):
            specialist_sections.append(f"### {agent_id}\n- Error: {item}")
            continue
        if not isinstance(item, dict) or not item.get("ok"):
            specialist_sections.append(f"### {agent_id}\n- Error: {(item or {}).get('error') if isinstance(item, dict) else 'unknown'}")
            continue
        specialist_sections.append(f"### {agent_id}\n{item.get('summary')}")

    synthesis_prompt = (
        "You are the manager agent. Merge specialist summaries into one coherent final answer.\n\n"
        f"Original user request:\n{user_request}\n\n"
        "Specialist outputs:\n"
        + "\n\n".join(specialist_sections)
    )
    synthesis_history = dspy.History(messages=[])
    ctx_token = set_request_user_context(user_context)
    try:
        async for event in _stream_manager_direct_async(
            user_request=synthesis_prompt,
            dspy_history=synthesis_history,
            model_name=model_name,
        ):
            yield event
    finally:
        reset_request_user_context(ctx_token)


def _to_dspy_history(history: Optional[List[Dict[str, str]]] = None) -> dspy.History:
    messages: List[Dict[str, str]] = []
    for item in history or []:
        question = (item.get("question") or "").strip()
        answer = (item.get("answer") or "").strip()
        if question and answer:
            messages.append({"user_request": question, "process_result": answer})
    return dspy.History(messages=messages)


def _build_user_request_with_history(
    question: str,
    history: Optional[List[Dict[str, str]]] = None,
    max_turns: int = 8,
) -> str:
    turns = [
        {
            "question": (item.get("question") or "").strip(),
            "answer": (item.get("answer") or "").strip(),
        }
        for item in (history or [])
        if (item.get("question") or "").strip() and (item.get("answer") or "").strip()
    ]
    if not turns:
        return question

    recent_turns = turns[-max(1, int(max_turns)) :]
    history_lines: List[str] = []
    for idx, turn in enumerate(recent_turns, start=1):
        history_lines.append(f"Turn {idx} user: {turn['question']}")
        history_lines.append(f"Turn {idx} assistant: {turn['answer']}")

    history_block = "\n".join(history_lines)
    return (
        "Recent conversation context (most recent turns):\n"
        f"{history_block}\n\n"
        f"Current user request:\n{question}"
    )


def _build_manager_agent() -> dspy.ReAct:
    return dspy.ReAct(
        ManagerQA,
        tools=[
            calculator,
            current_datetime,
            current_user_profile,
        ],
        max_iters=10,
    )


def _build_specialist_agent(agent_id: str) -> dspy.ReAct:
    signature = _signature_for_specialist(agent_id)
    tools = _tools_for_specialist(agent_id)
    if not tools:
        raise ValueError(f"Unknown specialist agent '{agent_id}'")
    return dspy.ReAct(signature, tools=tools, max_iters=8)


def _create_specialist_stream_agent(agent_id: str) -> Tuple[dspy.ReAct, Any]:
    react_agent = _build_specialist_agent(agent_id)
    stream_listeners = [
        dspy.streaming.StreamListener(
            signature_field_name="process_result",
            predict=react_agent.extract,
            predict_name="extract",
            allow_reuse=True,
        ),
    ]
    stream_agent = dspy.streamify(
        react_agent,
        status_message_provider=AgentStatusProvider(),
        stream_listeners=stream_listeners,
        async_streaming=True,
        is_async_program=False,
    )
    return react_agent, stream_agent


def _create_manager_stream_agent() -> Tuple[dspy.ReAct, Any]:
    react_agent = _build_manager_agent()
    stream_listeners = [
        dspy.streaming.StreamListener(
            signature_field_name="process_result",
            predict=react_agent.extract,
            predict_name="extract",
            allow_reuse=True,
        ),
    ]
    stream_agent = dspy.streamify(
        react_agent,
        status_message_provider=AgentStatusProvider(),
        stream_listeners=stream_listeners,
        async_streaming=True,
        is_async_program=False,
    )
    return react_agent, stream_agent


def ask_question(
    question: str,
    history: Optional[List[Dict[str, str]]] = None,
    user_context: Optional[Dict[str, Any]] = None,
) -> str:
    chosen_model = os.getenv("DSPY_MODEL", DEFAULT_MODEL)
    agent = _build_manager_agent()
    dspy_history = _to_dspy_history(history)
    user_request = _build_user_request_with_history(question=question, history=history)
    ctx_token = set_request_user_context(user_context)
    try:
        with dspy_lm_context(chosen_model):
            prediction = agent(user_request=user_request, history=dspy_history)
        return (prediction.process_result or "").strip()
    finally:
        reset_request_user_context(ctx_token)


async def _stream_manager_direct_async(
    user_request: str,
    dspy_history: dspy.History,
    model_name: Optional[str] = None,
) -> AsyncIterator[Dict[str, Any]]:
    chosen_model = model_name or os.getenv("DSPY_MODEL", DEFAULT_MODEL)
    final_answer = ""
    output_stream = None
    try:
        with dspy_lm_context(chosen_model):
            _, stream_agent = _create_manager_stream_agent()
            output_stream = stream_agent(user_request=user_request, history=dspy_history)
            final_prediction = None
            async for chunk in output_stream:
                if isinstance(chunk, dspy.streaming.StatusMessage):
                    message = chunk.message or ""
                    if message.startswith("[tool:start]"):
                        rest = message.replace("[tool:start]", "", 1).strip()
                        tool_name, _, args_part = rest.partition(" args=")
                        args: Dict[str, Any] = {}
                        if args_part:
                            try:
                                parsed_args = json.loads(args_part)
                                if isinstance(parsed_args, dict):
                                    args = parsed_args
                                else:
                                    args = {"value": parsed_args}
                            except Exception:
                                args = {"raw": args_part}
                        yield {"type": "tool_start", "agent_id": "manager", "data": {"tool": tool_name, "args": args}}
                        continue
                    if message.startswith("[tool:end]"):
                        summary_part = message.replace("[tool:end]", "", 1).strip()
                        summary: Dict[str, Any] = {}
                        if summary_part:
                            try:
                                parsed_summary = json.loads(summary_part)
                                if isinstance(parsed_summary, dict):
                                    summary = parsed_summary
                            except Exception:
                                summary = {"raw": summary_part}
                        yield {"type": "tool_end", "agent_id": "manager", "data": summary}
                        if "rows_count" in summary or "docs_count" in summary:
                            yield {"type": "retrieve", "agent_id": "manager", "data": summary}
                        continue
                    yield {"type": "trace_token", "agent_id": "manager", "data": {"text": f"{message}\n"}}
                    continue
                if isinstance(chunk, dspy.streaming.StreamResponse):
                    if chunk.signature_field_name == "process_result":
                        token = chunk.chunk or ""
                        if token:
                            final_answer += token
                            yield {"type": "answer_token", "agent_id": "manager", "data": {"text": token}}
                    continue
                if isinstance(chunk, dspy.Prediction):
                    final_prediction = chunk

        if final_prediction is not None and not final_answer:
            final_answer = (final_prediction.process_result or "").strip()
        if not final_answer:
            final_answer = "I could not generate a final answer. Please try again."
        yield {"type": "final", "agent_id": "manager", "data": {"answer": final_answer}}
    except asyncio.CancelledError:
        if output_stream is not None:
            with contextlib.suppress(Exception):
                await output_stream.aclose()
        return


async def stream_question_answer_async(
    question: str,
    history: Optional[List[Dict[str, str]]] = None,
    user_context: Optional[Dict[str, Any]] = None,
) -> AsyncIterator[Dict[str, Any]]:
    dspy_history = _to_dspy_history(history)
    user_request = _build_user_request_with_history(question=question, history=history)
    plan = _manager_plan(question)
    try:
        if plan:
            yield {
                "type": "trace_token",
                "agent_id": "manager",
                "data": {
                    "text": (
                        f"[working] Manager scheduled specialists: {', '.join(plan.keys())}\n"
                        "Agent spawn list: projectfacts, wko, evi, ofb.\n"
                    )
                },
            }
            async for event in _stream_parallel_specialists_async(
                user_request=user_request,
                history=dspy_history,
                user_context=user_context,
                plan=plan,
            ):
                yield event
            return

        async for event in _stream_manager_direct_async(user_request=user_request, dspy_history=dspy_history):
            yield event
    except asyncio.CancelledError:
        return


def build_agent(db: Any = None, model_name: Optional[str] = None):
    del db
    with dspy_lm_context(model_name or os.getenv("DSPY_MODEL", DEFAULT_MODEL)):
        agent, stream_agent = _create_manager_stream_agent()
    return agent, stream_agent