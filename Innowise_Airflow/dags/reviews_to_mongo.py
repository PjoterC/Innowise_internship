from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow.sdk import dag, task
from airflow.providers.mongo.hooks.mongo import MongoHook

from assets import cleaned_reviews

DB_NAME = "reviews_db"
COLLECTION_NAME = "reviews"
# Mongo caps a single command at 16 MB; batch rather than one insert_many.
BATCH_SIZE = 1000


def _resolve_source_path(context) -> str:
    """
    Path of the CSV that triggered this run.
    """
    events = context["triggering_asset_events"][cleaned_reviews]
    path = next(
        (event.extra["path"] for event in reversed(events) if event.extra.get("path")),
        None,
    )
    if path is not None:
        return path

    found = next((p for p in sorted(Path("/opt/airflow/data").glob("*.csv")) if p.is_file()), None)
    if found is None:
        raise FileNotFoundError("No triggering asset event and no CSV in /opt/airflow/data")
    return str(found)


@dag(
    dag_id="reviews_to_mongo",
    # Asset-aware scheduling: no cron at all. This DAG runs when `mongo_reader`
    # marks `cleaned_reviews` updated. For "on update *and* daily", swap this for
    # AssetOrTimeSchedule(timetable=CronTriggerTimetable("@daily", timezone="UTC"),
    #                     assets=[cleaned_reviews]).
    schedule=[cleaned_reviews],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["assets", "pandas", "mongodb"],
)
def reviews_to_mongo():

    
    @task(inlets=[cleaned_reviews])
    def load_to_mongo(**context) -> int:
        source_path = _resolve_source_path(context)
        df = pd.read_csv(source_path)
        print(f"Loading {len(df)} rows from {source_path}")

        hook = MongoHook(mongo_conn_id="mongo_default")
        collection = hook.get_conn()[DB_NAME][COLLECTION_NAME]
        # The producer rewrites the whole CSV in place, so the file is the full
        # truth each run: replace the collection instead of appending duplicates.
        collection.drop()

        records = df.to_dict(orient="records")
        for start in range(0, len(records), BATCH_SIZE):
            collection.insert_many(records[start:start + BATCH_SIZE])
        return len(records)

    @task
    def summarize(inserted: int):
        hook = MongoHook(mongo_conn_id="mongo_default")
        collection = hook.get_conn()[DB_NAME][COLLECTION_NAME]
        stored = collection.count_documents({})
        print(f"Inserted {inserted} rows, {DB_NAME}.{COLLECTION_NAME} now holds {stored}")
        print(pd.DataFrame(list(collection.find({}, {"_id": 0}).limit(5))))

    summarize(load_to_mongo())


reviews_to_mongo()
