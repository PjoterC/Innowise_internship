"""Assets shared between DAGs.

Kept out of the DAG files themselves so a consumer can import an asset without
importing (and therefore executing) the producer's module.
"""

from pathlib import Path

from airflow.sdk import Asset

# Where `file_processor` looks for input.
RAW_DIR = Path("/opt/airflow/data")

# Where `file_processor` writes its result and `reviews_to_mongo` reads it from.
# A subdirectory rather than a sibling file, so the sensor
# can never pick the output back up as new input.
CLEANED_REVIEWS_PATH = RAW_DIR / "processed" / "cleaned_reviews.csv"

# The cleaned review CSV produced by `file_processor`.
cleaned_reviews = Asset(
    name="cleaned_reviews",
    uri=CLEANED_REVIEWS_PATH.as_uri(),
)
