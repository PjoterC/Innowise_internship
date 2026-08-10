from datetime import datetime

import pandas as pd
from airflow.sdk import PokeReturnValue, dag, task, task_group
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from pathlib import Path
import re
import string

@dag(
    dag_id="mongo_reader",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["pandas", "mongodb"],
)
def mongo_reader():

    @task.sensor(poke_interval=30, timeout=300, mode="reschedule")
    def wait_for_file() -> PokeReturnValue:
        data_dir = Path("/opt/airflow/data")
        # Only real CSVs: WSL leaves ":Zone.Identifier" sidecar files next to
        # anything downloaded through Windows, and they list first in iterdir().
        found = next((p for p in sorted(data_dir.glob("*.csv")) if p.is_file()), None)
        return PokeReturnValue(is_done=found is not None, xcom_value=str(found) if found else None)

    file_path = wait_for_file()

    @task.branch
    def file_emptiness_check(file_path: str) -> str:
        return "log_empty" if Path(file_path).stat().st_size == 0 else "data_processing.replace_nulls"

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
            df = df.fillna("-")
            
            df.to_csv(file_path, index=False)
            return file_path

        @task
        def sort_data(file_path: str) -> str:
            df = pd.read_csv(file_path)
            df["at"] = pd.to_datetime(df["at"])
            df = df.sort_values(by="at", ascending=True)
            df.to_csv(file_path, index=False)
            return file_path

        @task
        def remove_unnecessary_chars(file_path: str) -> str:
            df = pd.read_csv(file_path)
            df['content'] = df['content'].replace(
                f"[^\\w\\s{re.escape(string.punctuation)}]", "", regex=True
            )
            df.to_csv(file_path, index=False)
            return file_path

        remove_unnecessary_chars(sort_data(replace_nulls(file_path)))


    branch = file_emptiness_check(file_path)
    branch >> [log_empty, process_data(file_path)]


mongo_reader()