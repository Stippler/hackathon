import argparse
import json

from mas.agent import DEFAULT_MODEL, build_agent
from mas.db import build_db_from_env
from mas.models import FilterArgs, FuzzyJoinArgs, SelectArgs
from mas.profile import fraunhofer_lscm_focus
from mas.utils import clean, name_similarity, norm_name


def _title(text: str) -> None:
    print("\n" + "=" * 90)
    print(text)
    print("=" * 90)


def _show(label: str, value) -> None:
    print(f"{label}:")
    print(json.dumps(value, ensure_ascii=False, default=str, indent=2))


def run_models() -> None:
    _title("MODELS")
    _show("SelectArgs", SelectArgs(table="projectfacts", limit=5).model_dump())
    _show("FilterArgs", FilterArgs(column="name", op="ilike", value="fraunhofer").model_dump())
    _show(
        "FuzzyJoinArgs",
        FuzzyJoinArgs(
            left_table="evi_bilanz_publications",
            right_table="projectfacts",
            left_key="company_name",
            right_key="name",
        ).model_dump(),
    )


def run_utils() -> None:
    _title("UTILS")
    _show("clean", clean("  Fraunhofer   LSCM "))
    _show("norm_name", norm_name("Fraunhofer GmbH & Co KG"))
    _show("name_similarity", name_similarity("Fraunhofer", "Fraunhofer Institut"))


def run_profile() -> None:
    _title("PROFILE")
    profile = fraunhofer_lscm_focus()
    _show(
        "fraunhofer_lscm_focus summary",
        {
            "focus_areas": len(profile.get("focus_areas", [])),
            "target_industries": len(profile.get("target_industries", [])),
        },
    )


def run_db() -> None:
    _title("DB INTEGRATION")
    db = build_db_from_env()

    evi = db.evi_lookup(company_name="fraunhofer", limit=5)
    _show("evi_lookup(company_name='fraunhofer')", {"count": evi["count"], "first": (evi["rows"] or [None])[0]})

    pf = db.select_rows(
        table="projectfacts",
        columns=["name", "city", "country", "last_changed_at"],
        filters=[{"column": "name", "op": "ilike", "value": "fraunhofer"}],
        limit=5,
        order_by="last_changed_at",
    )
    _show("select_rows(projectfacts, name ilike fraunhofer)", {"count": pf["count"], "rows": pf["rows"]})

    counts = db.distinct_counts(
        table="wko_companies",
        column="branche",
        query_text="transport kurier paket zustell",
        limit=5,
    )
    _show("distinct_counts(wko_companies.branche)", counts)

    join = db.fuzzy_join(
        left_table="evi_bilanz_publications",
        right_table="projectfacts",
        left_key="company_name",
        right_key="name",
        query_text="fraunhofer",
        limit=5,
    )
    _show("fuzzy_join(evi company_name -> projectfacts name)", join)


def run_agent(model_name: str | None) -> None:
    _title("AGENT")
    print(f"default model: {DEFAULT_MODEL}")
    if model_name:
        print(f"override model: {model_name}")
    db = build_db_from_env()
    _, _ = build_agent(db, model_name=model_name)
    print("build_agent: OK")


def main() -> None:
    parser = argparse.ArgumentParser(description="Simple verbose checks for MAS modules.")
    parser.add_argument("--skip-db", action="store_true")
    parser.add_argument("--skip-agent", action="store_true")
    parser.add_argument("--model", default=None)
    args = parser.parse_args()

    run_models()
    run_utils()
    run_profile()
    if not args.skip_db:
        run_db()
    if not args.skip_agent:
        run_agent(args.model)


if __name__ == "__main__":
    main()
