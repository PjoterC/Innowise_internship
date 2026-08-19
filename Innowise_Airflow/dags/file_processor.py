from datetime import datetime

import pandas as pd
from airflow.sdk import Metadata, PokeReturnValue, dag, task, task_group
from airflow.providers.standard.operators.bash import BashOperator
from pathlib import Path
import re
import string

from assets import CLEANED_REVIEWS_PATH, RAW_DIR, cleaned_reviews


def _write_atomically(df: pd.DataFrame, file_path) -> None:
    """Write a CSV via a temp file and a rename.

    `reviews_to_mongo` reads this file as soon as the asset updates, so a
    plain to_csv would let it observe a half-written frame. Rename is atomic
    within a filesystem: a reader sees either the old file or the new one.
    """
    target = Path(file_path)
    tmp = target.with_name(target.name + ".tmp")
    df.to_csv(tmp, index=False)
    tmp.replace(target)


@dag(
    dag_id="file_processor",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # Every run writes the same derived file, so two overlapping runs would
    # read each other's half-written output. Serialise them.
    max_active_runs=1,
    tags=["pandas", "mongodb"]
)
def file_processor():

    @task.sensor(poke_interval=30, timeout=300, mode="reschedule")
    def wait_for_file() -> PokeReturnValue:
        found = next((p for p in sorted(RAW_DIR.glob("*.csv")) if p.is_file()), None)
        return PokeReturnValue(is_done=found is not None, xcom_value=str(found) if found else None)

    file_path = wait_for_file()

    @task.branch
    def file_emptiness_check(file_path: str) -> str:
        try:
            has_rows = not pd.read_csv(file_path, nrows=1).empty
        except pd.errors.EmptyDataError:
            has_rows = False
        return "data_processing.replace_nulls" if has_rows else "log_empty"

    log_empty = BashOperator(
        task_id="log_empty",
        # The path goes through the environment rather than being interpolated
        # into the command line: input filenames routinely contain spaces, and
        # a bare template would split into several arguments.
        bash_command='bash /opt/airflow/scripts/log_empty.sh "$DATA_FILE"',
        env={"DATA_FILE": "{{ ti.xcom_pull(task_ids='wait_for_file') }}"},
        append_env=True,
    )

    @task_group(group_id="data_processing")
    def process_data(file_path: str):
        @task
        def replace_nulls(file_path: str) -> str:
            df = pd.read_csv(file_path)
            # "-" is a text placeholder, so only text columns get it. Filling it
            # into a numeric column would flip that column to object dtype and
            # land strings in Mongo, breaking the $avg query downstream. Numeric
            # columns keep NaN and are written out as empty cells instead.
            text_cols = df.select_dtypes(include=["object", "string"]).columns
            df[text_cols] = df[text_cols].fillna("-")

            # Create processed file
            CLEANED_REVIEWS_PATH.parent.mkdir(parents=True, exist_ok=True)
            _write_atomically(df, CLEANED_REVIEWS_PATH)
            return str(CLEANED_REVIEWS_PATH)

        @task
        def sort_data(file_path: str) -> str:
            df = pd.read_csv(file_path)
            # `replace_nulls` leaves "-" where a date was missing, which
            # to_datetime cannot parse. Coerce those to NaT rather than failing
            # the task, sort them to the end, and put the placeholder back so
            # the column stays consistent with every other text column.
            parsed = pd.to_datetime(df["at"], errors="coerce")
            df = df.assign(_sort_key=parsed).sort_values("_sort_key", na_position="last")
            df["at"] = df["_sort_key"].dt.strftime("%Y-%m-%d %H:%M:%S").fillna("-")
            df = df.drop(columns="_sort_key")
            _write_atomically(df, file_path)
            return file_path

        
        @task(outlets=[cleaned_reviews])
        def remove_unnecessary_chars(file_path: str):
            df = pd.read_csv(file_path)
            df['content'] = df['content'].replace(
                f"[^\\w\\s{re.escape(string.punctuation)}]", "", regex=True
            )
            _write_atomically(df, file_path)
            yield Metadata(cleaned_reviews, {"path": file_path, "rows": len(df)})

        remove_unnecessary_chars(sort_data(replace_nulls(file_path)))


    branch = file_emptiness_check(file_path)
    branch >> [log_empty, process_data(file_path)]


file_processor()