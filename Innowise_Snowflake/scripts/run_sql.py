"""Run .sql files against the Snowflake account configured in `.env`.

The Airflow DAGs are the pipeline's real entry point; this is the hand-driven
one — for deploying DDL before Airflow is up, for the Time Travel scripts, and
for the row-level-security demo, none of which belong on a schedule.

    ../.venv/bin/python scripts/run_sql.py sql/ddl/*.sql
    ../.venv/bin/python scripts/run_sql.py --role DWH_ANALYST_EU sql/analysis/rls_demo.sql
    ../.venv/bin/python scripts/run_sql.py --quiet sql/time_travel/01_ddl_time_travel.sql

Statements are split by the Snowflake connector's own splitter, which
understands `$$ ... $$` blocks — so a stored procedure whose body is full of
semicolons still arrives as one statement.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
# check_connection.py already knows how to turn `.env` into connect kwargs,
# including the four authentication styles. Reusing it keeps the credential
# rules in exactly one place.
sys.path.insert(0, str(PROJECT_ROOT))

from check_connection import build_connect_kwargs, load_env  # noqa: E402

MAX_PRINTED_ROWS = 20
MAX_CELL_WIDTH = 40


def print_result(cursor) -> None:
    """Print whatever the statement returned, truncated to stay readable."""
    if cursor.description is None:
        return
    headers = [col.name for col in cursor.description]
    rows = cursor.fetchmany(MAX_PRINTED_ROWS)
    if not rows:
        print("    (no rows)")
        return

    def cell(value: object) -> str:
        text = "NULL" if value is None else str(value)
        return text if len(text) <= MAX_CELL_WIDTH else text[: MAX_CELL_WIDTH - 1] + "…"

    table = [headers] + [[cell(v) for v in row] for row in rows]
    widths = [max(len(r[i]) for r in table) for i in range(len(headers))]
    for i, row in enumerate(table):
        print("    " + "  ".join(v.ljust(w) for v, w in zip(row, widths)))
        if i == 0:
            print("    " + "  ".join("-" * w for w in widths))
    if len(rows) == MAX_PRINTED_ROWS:
        print(f"    … first {MAX_PRINTED_ROWS} rows only")


def run_file(cursor, path: Path, quiet: bool) -> None:
    from snowflake.connector.util_text import split_statements

    print(f"\n=== {path} " + "=" * max(0, 60 - len(str(path))))
    with path.open(encoding="utf-8") as handle:
        # remove_comments=False keeps the SQL recognisable in Snowflake's query
        # history, which matters when the audit log stores query ids.
        statements = list(split_statements(handle, remove_comments=False))

    for statement, _is_put in statements:
        text = statement.strip()
        if not text or text == ";":
            continue
        # First non-comment line, as a label — enough to follow along without
        # echoing a 60-line CREATE TABLE.
        label = next(
            (
                line.strip()
                for line in text.splitlines()
                if line.strip() and not line.strip().startswith("--")
            ),
            text[:70],
        )
        print(f"  -> {label[:100]}")
        cursor.execute(text)
        if not quiet:
            print_result(cursor)


def main() -> int:
    import snowflake.connector

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="+", type=Path, help=".sql files, run in the order given")
    parser.add_argument("--role", help="USE ROLE before running — for the RLS demo")
    parser.add_argument("--quiet", action="store_true", help="run statements without printing result sets")
    args = parser.parse_args()

    missing = [str(f) for f in args.files if not f.exists()]
    if missing:
        sys.exit("No such file: " + ", ".join(missing))

    kwargs = build_connect_kwargs(load_env())
    kwargs.pop("_auth_label", None)
    kwargs["application"] = "innowise_dwh_runner"
    if args.role:
        kwargs["role"] = args.role

    with snowflake.connector.connect(**kwargs) as conn:
        with conn.cursor() as cur:
            for path in args.files:
                try:
                    run_file(cur, path, args.quiet)
                except snowflake.connector.errors.Error as exc:
                    print(f"\nFAILED in {path}:\n{exc}", file=sys.stderr)
                    return 1
    print("\nAll files completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
