"""Assets shared between DAGs.

Kept out of the DAG files themselves so a consumer can import an asset without
importing (and therefore executing) the producer's module.
"""

from airflow.sdk import Asset

# The cleaned review CSV produced by `mongo_reader`. The URI is only an
# identifier — Airflow never opens it. The concrete file the run touched is
# passed along in the asset event's `extra`, since the producer discovers the
# path by globbing and it is not known statically.
cleaned_reviews = Asset(
    name="cleaned_reviews",
    uri="file:///opt/airflow/data/cleaned_reviews.csv",
)
