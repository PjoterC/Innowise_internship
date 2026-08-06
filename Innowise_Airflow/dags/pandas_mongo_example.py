from datetime import datetime

import pandas as pd
from airflow.sdk import dag, task
from airflow.providers.mongo.hooks.mongo import MongoHook


@dag(
    dag_id="pandas_mongo_example",
    schedule=None,  # trigger manually for now
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["example", "pandas", "mongodb"],
)
def pandas_mongo_example():

    @task
    def build_dataframe() -> list[dict]:
        df = pd.DataFrame(
            {
                "name": ["Ada", "Grace", "Alan"],
                "score": [91, 88, 95],
            }
        )
        # XCom needs JSON-serializable data, so pass records (list of dicts)
        return df.to_dict(orient="records")

    @task
    def write_to_mongo(records: list[dict]):
        hook = MongoHook(mongo_conn_id="mongo_default")
        client = hook.get_conn()
        db = client["new_example_db"]
        collection = db["scores"]
        collection.insert_many(records)

    @task
    def read_from_mongo():
        hook = MongoHook(mongo_conn_id="mongo_default")
        client = hook.get_conn()
        db = client["new_example_db"]
        docs = list(db["scores"].find({}, {"_id": 0}))
        df = pd.DataFrame(docs)
        print(df)

    records = build_dataframe()
    write_to_mongo(records) >> read_from_mongo()


pandas_mongo_example()
