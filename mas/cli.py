import argparse

import dspy

from .agent import DEFAULT_MODEL, build_agent
from .db import build_db_from_env
from .runner import RunLogger, run_demo_queries, run_interactive


def main() -> None:
    parser = argparse.ArgumentParser(description="MAS acquisition scouting harness")
    parser.add_argument(
        "--mode",
        choices=["demo", "interactive"],
        default="demo",
        help="Run predefined tests or chat interactively.",
    )
    parser.add_argument(
        "--model",
        default=None,
        help=(
            "DSPy model name (e.g. openai/gpt-5.2). "
            "If omitted, uses MAS_LM env var, else defaults to "
            f"{DEFAULT_MODEL}."
        ),
    )
    parser.add_argument(
        "--log-file",
        default=None,
        help=(
            "Path to run log file. If omitted, writes to logs/mas_run_<timestamp>.log."
        ),
    )
    args = parser.parse_args()

    db = build_db_from_env()
    agent, stream_agent = build_agent(db, model_name=args.model)
    history = dspy.History(messages=[])
    logger = RunLogger(log_path=args.log_file)

    try:
        if args.mode == "interactive":
            run_interactive(agent, stream_agent, history, logger=logger)
        else:
            run_demo_queries(agent, stream_agent, history, logger=logger)
    finally:
        logger.close()
