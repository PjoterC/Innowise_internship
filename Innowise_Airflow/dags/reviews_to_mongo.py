from datetime import datetime

import pandas as pd
from airflow.sdk import dag, task
from airflow.providers.mongo.hooks.mongo import MongoHook

from assets import CLEANED_REVIEWS_PATH, cleaned_reviews

DB_NAME = "reviews_db"
COLLECTION_NAME = "reviews"
# Mongo caps a single command at 16 MB; batch rather than one insert_many.
BATCH_SIZE = 1000


@dag(
    dag_id="reviews_to_mongo",
    # This DAG runs when `file_processor` marks `cleaned_reviews` updated.
    schedule=[cleaned_reviews],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["assets", "pandas", "mongodb"],
)
def reviews_to_mongo():

    
    @task(inlets=[cleaned_reviews])
    def load_to_mongo() -> int:
        df = pd.read_csv(CLEANED_REVIEWS_PATH)
        print(f"Loading {len(df)} rows from {CLEANED_REVIEWS_PATH}")

        hook = MongoHook(mongo_conn_id="mongo_default")
        collection = hook.get_conn()[DB_NAME][COLLECTION_NAME]

        # Replace collection instead of appending (not specified in requirements)
        collection.drop()

        # A missing numeric value arrives as NaN, which pymongo would store as
        # a NaN double: $avg over it returns NaN and `{field: null}` never
        # matches. Store a real null instead.
        records = df.astype(object).where(pd.notna(df), None).to_dict(orient="records")
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
