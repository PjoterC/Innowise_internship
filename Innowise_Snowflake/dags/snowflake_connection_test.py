"""Proves Airflow can reach Snowflake through the `snowflake_default` connection.

Run this once after `docker compose up -d`. If both tasks are green, the
credentials in `.env` reached the container intact and any later DAG can just
ask for `snowflake_conn_id="snowflake_default"`.

The connection itself is defined by the AIRFLOW_CONN_SNOWFLAKE_DEFAULT
environment variable in docker-compose.yml — there is nothing to create in the
UI, and nothing lands in the metadata database.
"""

from __future__ import annotations

from pathlib import Path

import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, task

SNOWFLAKE_CONN_ID = "snowflake_default"

# Relative .sql paths are resolved against the DAG folder, but sql/ is mounted
# next to it at /opt/airflow/sql. Deriving the path from __file__ rather than
# hard-coding it keeps this working outside the container too, where the same
# two directories sit side by side in the repo.
SQL_DIR = Path(__file__).resolve().parent.parent / "sql"


@dag(
    dag_id="snowflake_connection_test",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    # Manual only: this is a diagnostic, it should never run on its own.
    schedule=None,
    catchup=False,
    tags=["snowflake", "setup"],
    template_searchpath=[str(SQL_DIR)],
    doc_md=__doc__,
)
def snowflake_connection_test():
    @task
    def report_session_context() -> dict[str, str]:
        """Connect through the hook and log what Snowflake thinks the session is."""
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        # get_first returns one row, so the result is small enough to push to
        # XCom as-is — no need to stream or paginate a seven-column single row.
        row = hook.get_first(
            "SELECT CURRENT_VERSION(), CURRENT_ACCOUNT(), CURRENT_USER(), "
            "CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()"
        )
        labels = ["version", "account", "user", "role", "warehouse", "database", "schema"]
        context = {label: str(value) for label, value in zip(labels, row)}

        for label, value in context.items():
            print(f"{label:<10} {value}")

        if row[4] is None:
            print(
                "WARNING: no active warehouse — the login works, but queries that "
                "actually read data will fail until SNOWFLAKE_WAREHOUSE names one."
            )
        return context

    # The same check through the operator path rather than the hook path: it is
    # the one a real DAG uses, and it exercises the provider's SQL plumbing
    # (templated .sql file, cursor handling) that the hook call above skips.
    run_context_query = SQLExecuteQueryOperator(
        task_id="run_context_query",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="context.sql",
        show_return_value_in_logs=True,
    )

    report_session_context() >> run_context_query


snowflake_connection_test()
