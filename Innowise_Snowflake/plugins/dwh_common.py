"""Snowflake plumbing shared by the DWH DAGs.

Lives in plugins/ because Airflow puts that directory on sys.path and does not
scan it for DAGs — so `from dwh_common import ...` works and this file is not
parsed as a (DAG-less) DAG file.

One consequence worth knowing: Airflow re-reads DAG *files* on every parse, but
a plugin module stays in sys.modules once imported. Editing this file therefore
needs a restart before the DAGs see the change, or they fail to import against
the version already in memory:

    docker compose restart airflow-dag-processor airflow-scheduler
"""

from __future__ import annotations

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import get_current_context

SNOWFLAKE_CONN_ID = "snowflake_default"
DATABASE = "AIRLINE_DWH"


def get_hook() -> SnowflakeHook:
    """
    A hook already pointed at the warehouse database.

    The procedures address their tables as SCHEMA.TABLE, so the session needs a
    current database. Setting it here rather than in .env keeps the connection
    generic and the pipeline explicit about what it touches.
    """
    return SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID, database=DATABASE)


def run_query(sql: str, parameters: tuple = ()) -> list[tuple]:
    """
    Execute one statement on a raw connection and return every row.
    """
    with get_hook().get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, parameters)
        return cur.fetchall()


def call_load_procedure(procedure: str) -> str:
    """
    CALL a load procedure with the current run id and return what it said.

    The run id is the thread tying every audit row of one pipeline execution
    together, so it is read from the Airflow context instead of being passed
    down through each task signature.
    """
    run_id = get_current_context()["dag_run"].run_id
    rows = run_query(f"CALL {procedure}(%s)", (run_id,))
    result = rows[0][0] if rows else "no result"
    print(f"{procedure}: {result}")
    return result
