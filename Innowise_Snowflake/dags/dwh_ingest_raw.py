"""Source file -> internal stage -> RAW.AIRLINE_RAW, 1:1.

The PUT and the COPY belong on two sides of the wire: PUT is a client-side
command the Snowflake driver executes and so cannot live in a stored procedure,
while the COPY is entirely server-side and does.

The COPY is then done one of two ways, chosen per run by the `load_method`
param:

    procedure  CALL RAW.SP_LOAD_RAW_FROM_STAGE(...) through SnowflakeHook
    operator   SQLExecuteQueryOperator running sql/airflow/load_raw_copy_into.sql

They land identical rows, and both write the same audit row through
META.SP_WRITE_AUDIT — so downstream neither the pipeline nor audit_report.sql
can tell which one ran. What differs is where the SQL lives:

* the procedure is a database object. It validates its own arguments, wraps the
  COPY in an exception handler, and so audits a *failed* load as well as a
  successful one. Anything with USAGE on it can call it — Airflow is just one
  caller. This is the default;
* the .sql file is an Airflow artifact, rendered by Jinja at task run time.
  Less machinery, and the statement is right there in the task log — but a COPY
  that throws takes the whole task down before the audit CALL at the end of the
  file is reached, so a failure exists only in the Airflow log.

They are alternatives rather than a sequence on purpose. Snowflake's load
history is keyed on (table, staged file), so running both over one file would
COPY the same rows twice under two different batch ids.

Re-running either is safe, but not silent. put_file_to_stage PUTs with
OVERWRITE = TRUE, and Snowflake identifies an already-loaded file by name and
ETag rather than by content — so a re-upload reads as a modified file and the
COPY loads it again even with FORCE = FALSE. A second run therefore appends a
second full copy to RAW.AIRLINE_RAW, distinguishable by BATCH_ID.

Nothing downstream is fooled: the MERGEs are keyed on natural keys, so the
duplicate batch drains all four streams as zero inserts and zero updates. Set
the `force_reload` param to replay a file deliberately — it is the honest way to
say so, and the audit log records it as such.
"""

from __future__ import annotations

import re
from pathlib import Path

import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, get_current_context, task
from dwh_common import DATABASE, SNOWFLAKE_CONN_ID, get_hook, run_query

STAGE = "RAW.STG_AIRLINE_FILES"

# sql/ is mounted next to dags/ at /opt/airflow/sql. Deriving the path from
# __file__ rather than hard-coding it keeps this working outside the container
# too, where the same two directories sit side by side in the repo.
SQL_DIR = Path(__file__).resolve().parent.parent / "sql"


@dag(
    dag_id="dwh_ingest_raw",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["snowflake", "dwh", "stage-1"],
    template_searchpath=[str(SQL_DIR)],
    params={
        # data/ is mounted into the container by docker-compose.yml.
        "source_file": Param("/opt/airflow/data/Airline Dataset.csv", type="string"),
        "load_method": Param("procedure", type="string", enum=["procedure", "operator"]),
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
        # Both consumers embed this in a SQL string literal before Snowflake
        # reads it as a regex, so it passes through two parsers. re.escape()
        # would spray backslashes that the literal then eats; replacing every
        # not-plainly-safe character with "." avoids arguing with either parser,
        # and "." matches itself anyway. It also means the result cannot carry a
        # quote, which is what makes it safe to template into the .sql file
        # below — the procedure rejects one outright.
        return ".*" + re.sub(r"[^A-Za-z0-9_-]", ".", source.name) + "([.]gz)?"

    @task.branch
    def choose_load_method() -> str:
        """Pick which of the two COPY paths runs. The other is skipped."""
        method = get_current_context()["params"]["load_method"]
        return "copy_into_raw_via_procedure" if method == "procedure" else "copy_into_raw_via_operator"

    @task(task_id="copy_into_raw_via_procedure")
    def copy_into_raw_via_procedure(file_pattern: str) -> str:
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

    # The same load with the SQL in a file instead of in the database. hook_params
    # pins the database because the operator uses the raw connection, whose
    # database may be blank in .env — the procedures address their tables as
    # SCHEMA.TABLE and need a current database either way.
    copy_into_raw_via_operator = SQLExecuteQueryOperator(
        task_id="copy_into_raw_via_operator",
        conn_id=SNOWFLAKE_CONN_ID,
        hook_params={"database": DATABASE},
        sql="airflow/load_raw_copy_into.sql",
        split_statements=True,
        return_last=True,
        show_return_value_in_logs=True,
    )

    # Landing rows is only half a load — they are not in the model until the
    # main pipeline drains the streams, so this hands straight over. The trigger
    # rule is the branch's doing: exactly one of the two COPY tasks runs and the
    # other is skipped, which the default all_success would refuse to accept.
    run_pipeline = TriggerDagRunOperator(
        task_id="run_main_pipeline",
        trigger_dag_id="dwh_pipeline",
        wait_for_completion=False,
        trigger_rule="none_failed_min_one_success",
    )

    pattern = put_file_to_stage()
    branch = choose_load_method()
    pattern >> branch
    branch >> copy_into_raw_via_procedure(pattern) >> run_pipeline
    branch >> copy_into_raw_via_operator >> run_pipeline


dwh_ingest_raw()
