from datetime import datetime

import pandas as pd
from airflow.sdk import Metadata, PokeReturnValue, dag, task, task_group
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from pathlib import Path
import re
import string

from assets import cleaned_reviews



@dag(
    dag_id="file_processor",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["pandas", "mongodb"]
)
def file_processor():

    @task.sensor(poke_interval=30, timeout=300, mode="reschedule")
    def wait_for_file() -> PokeReturnValue:
        data_dir = Path("/opt/airflow/data")
        # Only real CSVs
        found = next((p for p in sorted(data_dir.glob("*.csv")) if p.is_file()), None)
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
        bash_command=(
            "bash /opt/airflow/scripts/log_empty.sh "
            "{{ ti.xcom_pull(task_ids='wait_for_file') }}"
        ),
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
            df.to_csv(file_path, index=False)
            return file_path

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
            df.to_csv(file_path, index=False)
            return file_path

        
        @task(outlets=[cleaned_reviews])
        def remove_unnecessary_chars(file_path: str):
            df = pd.read_csv(file_path)
            df['content'] = df['content'].replace(
                f"[^\\w\\s{re.escape(string.punctuation)}]", "", regex=True
            )
            df.to_csv(file_path, index=False)
            yield Metadata(cleaned_reviews, {"path": file_path, "rows": len(df)})

        remove_unnecessary_chars(sort_data(replace_nulls(file_path)))


    branch = file_emptiness_check(file_path)
    branch >> [log_empty, process_data(file_path)]


file_processor()