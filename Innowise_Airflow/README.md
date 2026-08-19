# Airflow + Pandas + MongoDB environment

Apache Airflow **3.3.0** (Task SDK DAG authoring), Postgres as the metadata
database, MongoDB as the target the example DAG writes to.

## Prerequisites
- Docker Desktop (or Docker Engine + Docker Compose v2) installed
- At least 4GB RAM allocated to Docker


## Project layout
```
.
├── dags/                     # your Airflow DAGs go here
│   ├── assets.py             # the Asset shared by the two DAGs, + the paths it implies
│   ├── file_processor.py     # sensor -> emptiness branch -> transform TaskGroup
│   └── reviews_to_mongo.py   # asset-triggered loader
├── data/                     # drop input CSVs here; never written to
│   └── processed/            # cleaned_reviews.csv, regenerated on every run
├── scripts/                  # bash helpers called by the DAGs
├── logs/                     # Airflow logs (auto-created)
├── plugins/                  # custom Airflow plugins (optional)
├── config/                   # optional airflow.cfg overrides
├── Dockerfile                # extends apache/airflow:3.3.0 with pandas + the Mongo provider
├── requirements.txt          # extra Python packages (versions come from Airflow's constraints file)
├── docker-compose.yml        # api-server / scheduler / dag-processor / triggerer + Postgres + MongoDB
└── README.md
```

## Data flow
`file_processor` reads whatever CSV it finds in `data/` and writes its result to
`data/processed/cleaned_reviews.csv` — the input is only ever read, so a re-run
always starts from the same bytes. The output subdirectory is deliberately not a
sibling of the input: the sensor's `*.csv` glob is non-recursive, so it cannot
pick its own output back up as new input. Writing that file updates the
`cleaned_reviews` asset, which triggers `reviews_to_mongo`.

## First-time setup

1. On Linux, set the Airflow user ID so file permissions on the mounted
   `logs/` directory match your host user:
   ```bash
   echo -e "AIRFLOW_UID=$(id -u)" > .env
   ```
   (On macOS/Windows with Docker Desktop you can skip this — it's harmless either way.)

2. Build the custom image (installs pandas and the Airflow MongoDB provider):
   ```bash
   docker compose build
   ```

3. Initialize the Airflow metadata database:
   ```bash
   docker compose up airflow-init
   ```

4. Start everything:
   ```bash
   docker compose up -d
   ```

5. Open the Airflow UI: http://localhost:8080 — no login required.

6. MongoDB is reachable at `localhost:27017` (user `mongo`, password `mongo`) from your host machine, e.g. with `mongosh` or Compass:
   ```
   mongodb://mongo:mongo@localhost:27017/?authSource=admin
   ```

## How DAGs talk to MongoDB
The `docker-compose.yml` sets an environment variable:
```
AIRFLOW_CONN_MONGO_DEFAULT: {"conn_type": "mongo", "host": "mongodb", "port": 27017,
                             "login": "mongo", "password": "mongo",
                             "extra": {"authSource": "admin"}}
```
This registers an Airflow connection called `mongo_default` automatically, so any DAG can use:
```python
from airflow.providers.mongo.hooks.mongo import MongoHook
hook = MongoHook(mongo_conn_id="mongo_default")
client = hook.get_conn()
```


## Running the DAGs

To run the DAGs for the first time, enable them in the Airflow UI, running under *http://localhost:8080/dags*.
File processor's schedule is set to daily, but you can run it manually. The DAG handling the database update - `reviews_to_mongo`
runs after a successful read of a non-empty file by the first DAG. Put the processed data directly in the `data/`directory.



## Stopping / resetting
```bash
docker compose down          # stop containers, keep data
docker compose down -v       # stop containers AND wipe volumes (fresh start)
```
