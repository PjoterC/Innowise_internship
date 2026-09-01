"""Source file -> internal stage -> RAW.AIRLINE_RAW, 1:1.

Two steps, because they belong on two sides of the wire: PUT is a client-side
command the Snowflake driver executes and so cannot live in a stored procedure,
while the COPY is entirely server-side and does.

Re-running is safe. Snowflake remembers which files it has already COPYed (for
64 days) and skips them, so a second run loads nothing and audits 0 rows. Set
the `force_reload` param to replay a file deliberately.
"""

from __future__ import annotations

import re
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, get_current_context, task
from dwh_common import get_hook, run_query

STAGE = "RAW.STG_AIRLINE_FILES"


@dag(
    dag_id="dwh_ingest_raw",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["snowflake", "dwh", "stage-1"],
    params={
        # data/ is mounted into the container by docker-compose.yml.
        "source_file": Param("/opt/airflow/data/Airline Dataset.csv", type="string"),
        "force_reload": Param(False, type="boolean"),
    },
    doc_md=__doc__,
)
def dwh_ingest_raw():
    @task
    def put_file_to_stage() -> str:
        """Upload the CSV to the internal stage; return a PATTERN matching it."""
        source = Path(get_current_context()["params"]["source_file"])
        if not source.exists():
            raise FileNotFoundError(f"{source} is not visible to the worker")

        with get_hook().get_conn() as conn, conn.cursor() as cur:
            # The URI is quoted because the dataset's filename contains a space.
            cur.execute(
                f"PUT 'file://{source}' @{STAGE} AUTO_COMPRESS = TRUE OVERWRITE = TRUE"
            )
            for row in cur.fetchall():
                print(row)

        # AUTO_COMPRESS appends .gz, so the pattern has to allow for it. Scoping
        # the pattern to this one file keeps the COPY from picking up whatever
        # else happens to be sitting on the stage.
        #
        # The procedure embeds this in a SQL string literal before Snowflake
        # reads it as a regex, so it passes through two parsers. re.escape()
        # would spray backslashes that the literal then eats; replacing every
        # not-plainly-safe character with "." avoids arguing with either parser,
        # and "." matches itself anyway.
        return ".*" + re.sub(r"[^A-Za-z0-9_-]", ".", source.name) + "([.]gz)?"

    @task
    def copy_into_raw(file_pattern: str) -> str:
        """Run the stage-1 load procedure, which also writes the audit row."""
        context = get_current_context()
        rows = run_query(
            "CALL RAW.SP_LOAD_RAW_FROM_STAGE(%s, %s, %s)",
            (
                context["dag_run"].run_id,
                file_pattern,
                bool(context["params"]["force_reload"]),
            ),
        )
        print(rows[0][0])
        return rows[0][0]

    # Landing rows is only half a load — they are not in the model until the
    # main pipeline drains the streams, so this hands straight over.
    run_pipeline = TriggerDagRunOperator(
        task_id="run_main_pipeline",
        trigger_dag_id="dwh_pipeline",
        wait_for_completion=False,
    )

    copy_into_raw(put_file_to_stage()) >> run_pipeline


dwh_ingest_raw()
