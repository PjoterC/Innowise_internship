"""The main pipeline: stage 1 -> stage 2 -> stage 3.

    dim_passenger ─┐
    dim_airport   ─┼─> fct_flight_booking ──> agg_flight_status_daily ──> audit
    dim_date      ─┘

Every task is one CALL. All the SQL lives in stored procedures, so this file
holds the ordering and nothing else — which is the only thing Airflow is better
at than Snowflake.

Each procedure reads its own stream. That is what makes the DAG restartable:
the three dimensions can succeed and the fact fail, and a retry re-runs all four
while the three that already committed find their streams empty and do nothing.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import chain, dag, get_current_context, task
from dwh_common import call_load_procedure, run_query


@dag(
    dag_id="dwh_pipeline",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["snowflake", "dwh", "stage-2", "stage-3"],
    doc_md=__doc__,
)
def dwh_pipeline():
    @task
    def load_dim_passenger() -> str:
        return call_load_procedure("CORE.SP_LOAD_DIM_PASSENGER")

    @task
    def load_dim_airport() -> str:
        return call_load_procedure("CORE.SP_LOAD_DIM_AIRPORT")

    @task
    def load_dim_date() -> str:
        return call_load_procedure("CORE.SP_LOAD_DIM_DATE")

    @task
    def load_fct_flight_booking() -> str:
        return call_load_procedure("CORE.SP_LOAD_FCT_FLIGHT_BOOKING")

    @task
    def load_agg_flight_status_daily() -> str:
        return call_load_procedure("MART.SP_LOAD_AGG_FLIGHT_STATUS_DAILY")

    @task
    def report_audit_log() -> list[tuple]:
        """Read back what the procedures logged for this run."""
        run_id = get_current_context()["dag_run"].run_id
        rows = run_query(
            """
            SELECT TARGET_OBJECT, OPERATION, ROWS_INSERTED, ROWS_UPDATED,
                   ROUND(DURATION_SEC, 2), STATUS
            FROM META.ETL_AUDIT_LOG
            WHERE RUN_ID = %s
            ORDER BY AUDIT_ID
            """,
            (run_id,),
        )
        for row in rows:
            print(f"{row[0]:<32} {row[1]:<6} +{row[2]:<8} ~{row[3]:<8} {row[4]:>8}s  {row[5]}")
        return rows

    # The dimensions are independent of each other; the fact needs all three to
    # exist first, and the aggregate needs the fact.
    chain(
        [load_dim_passenger(), load_dim_airport(), load_dim_date()],
        load_fct_flight_booking(),
        load_agg_flight_status_daily(),
        report_audit_log(),
    )


dwh_pipeline()
