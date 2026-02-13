import asyncio
import contextlib
import json
import logging
import os
import threading
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
_configured_model_name: Optional[str] = None
_configure_lock = threading.Lock()
logging.getLogger("dspy.utils.callback").setLevel(logging.ERROR)


class ToolQA(dspy.Signature):
    """
    Helpful assistant that can think step-by-step and call tools when needed.
    Tool usage rules:
    - For arithmetic or numeric expressions, call `calculator` before answering.
    - For current time/date requests, call `current_datetime` before answering.
    - For account questions (who am I, my email, my user id), call `current_user_profile`.
    - For table discovery and schema context, call `list_known_tables` or `describe_table`.
    - For read-only data access, call `supabase_query`.
    - For Austrian company registry lookups by name, call `ofb_search_company_compressed`.
    - For detailed Firmenbuch extract data, call `ofb_get_register_extract`.
    - For balance sheet/P&L/KPI data, call `ofb_get_financials_multiple`.
    - For a concise company summary from Firmenbuch fields, call `ofb_get_company_profile`.
    - For management/representation mapping, call `ofb_get_management_roles`.
    - Prefer tool outputs over guesses whenever a tool can answer directly.
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


_base_agent: Optional[dspy.ReAct] = None
_base_agent_model: Optional[str] = None
_base_agent_lock = threading.Lock()


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


def _ensure_dspy_configured(model_name: Optional[str] = None) -> str:
    global _configured_model_name
    chosen_model = model_name or os.getenv("DSPY_MODEL", DEFAULT_MODEL)
    if _configured_model_name == chosen_model:
        return chosen_model

    with _configure_lock:
        if _configured_model_name == chosen_model:
            return chosen_model
        dspy.configure(lm=dspy.LM(chosen_model, cache=False))
        _configured_model_name = chosen_model
    return chosen_model


def _get_base_agent(model_name: Optional[str] = None) -> dspy.ReAct:
    global _base_agent, _base_agent_model
    chosen_model = _ensure_dspy_configured(model_name=model_name)
    if _base_agent is not None and _base_agent_model == chosen_model:
        return _base_agent

    with _base_agent_lock:
        if _base_agent is not None and _base_agent_model == chosen_model:
            return _base_agent
        _base_agent = dspy.ReAct(
            ToolQA,
            tools=[
                calculator,
                current_datetime,
                current_user_profile,
                list_known_tables,
                describe_table,
                list_accessible_tables,
                supabase_query,
                ofb_search_company_compressed,
                ofb_get_register_extract,
                ofb_get_financials_multiple,
                ofb_get_company_profile,
                ofb_get_management_roles,
                ofb_get_company_report,
            ],
            max_iters=10,
        )
        _base_agent_model = chosen_model
    return _base_agent


def _create_stream_agent(model_name: Optional[str] = None) -> Tuple[dspy.ReAct, Any]:
    react_agent = _get_base_agent(model_name=model_name)
    stream_listeners = [
        # ReAct emits next_thought multiple times across iterations; keep listener reusable.
        dspy.streaming.StreamListener(signature_field_name="next_thought", allow_reuse=True),
        # ChainOfThought modules often emit "reasoning"; stream it when available.
        dspy.streaming.StreamListener(signature_field_name="reasoning", allow_reuse=True),
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
    agent, _ = _create_stream_agent()
    dspy_history = _to_dspy_history(history)
    user_request = _build_user_request_with_history(question=question, history=history)
    ctx_token = set_request_user_context(user_context)
    try:
        prediction = agent(user_request=user_request, history=dspy_history)
        return (prediction.process_result or "").strip()
    finally:
        reset_request_user_context(ctx_token)


async def stream_question_answer_async(
    question: str,
    history: Optional[List[Dict[str, str]]] = None,
    user_context: Optional[Dict[str, Any]] = None,
) -> AsyncIterator[Dict[str, Any]]:
    _, stream_agent = _create_stream_agent()
    dspy_history = _to_dspy_history(history)
    user_request = _build_user_request_with_history(question=question, history=history)
    ctx_token = set_request_user_context(user_context)

    final_answer = ""
    output_stream = None
    try:
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
                    yield {"type": "tool_start", "agent_id": "rag", "data": {"tool": tool_name, "args": args}}
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
                    yield {"type": "tool_end", "agent_id": "rag", "data": summary}
                    if "rows_count" in summary or "docs_count" in summary:
                        yield {"type": "retrieve", "agent_id": "rag", "data": summary}
                    continue
                yield {"type": "trace_token", "agent_id": "rag", "data": {"text": f"{message}\n"}}
                continue
            if isinstance(chunk, dspy.streaming.StreamResponse):
                field = chunk.signature_field_name
                if field in {"next_thought", "reasoning"}:
                    token = chunk.chunk or ""
                    if token:
                        yield {
                            "type": "trace_token",
                            "agent_id": "rag",
                            "data": {"text": token, "source": field},
                        }
                elif field == "process_result":
                    token = chunk.chunk or ""
                    if token:
                        final_answer += token
                        yield {"type": "answer_token", "agent_id": "rag", "data": {"text": token}}
                continue
            if isinstance(chunk, dspy.Prediction):
                final_prediction = chunk

        if final_prediction is not None and not final_answer:
            final_answer = (final_prediction.process_result or "").strip()
        if not final_answer:
            final_answer = "I could not generate a final answer. Please try again."

        yield {"type": "final", "agent_id": "rag", "data": {"answer": final_answer}}
    except asyncio.CancelledError:
        if output_stream is not None:
            with contextlib.suppress(Exception):
                await output_stream.aclose()
        return
    finally:
        reset_request_user_context(ctx_token)


def build_agent(db: Any = None, model_name: Optional[str] = None):
    del db
    agent, stream_agent = _create_stream_agent(model_name=model_name)
    return agent, stream_agent