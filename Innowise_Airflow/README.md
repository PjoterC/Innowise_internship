# Airflow + Pandas + MongoDB environment

Apache Airflow **3.3.0** (Task SDK DAG authoring), Postgres as the metadata
database, MongoDB as the target the example DAG writes to.

## Prerequisites
- Docker Desktop (or Docker Engine + Docker Compose v2) installed
- At least 4GB RAM allocated to Docker

> **Airflow does not run on native Windows.** It imports `os.register_at_fork`,
> which is POSIX-only, so `python dags/pandas_mongo_example.py` from a Windows
> venv fails with a confusing `ImportError: cannot import name
> 'ObjectStoragePath'` (a masked `AttributeError`). Run the stack in Docker —
> or WSL2 — and edit the DAG files on the host.

## Project layout
```
.
├── dags/                     # your Airflow DAGs go here
│   └── pandas_mongo_example.py
├── logs/                     # Airflow logs (auto-created)
├── plugins/                  # custom Airflow plugins (optional)
├── config/                   # optional airflow.cfg overrides
├── Dockerfile                # extends apache/airflow:3.3.0 with pandas + the Mongo provider
├── requirements.txt          # extra Python packages (versions come from Airflow's constraints file)
├── docker-compose.yml        # api-server / scheduler / dag-processor / triggerer + Postgres + MongoDB
└── README.md
```

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

5. Open the Airflow UI: http://localhost:8080 — no login required, see
   [Authentication](#authentication) below.

6. MongoDB is reachable at `localhost:27017` (user `mongo`, password `mongo`) from your host machine, e.g. with `mongosh` or Compass:
   ```
   mongodb://mongo:mongo@localhost:27017/?authSource=admin
   ```

## Verifying the stack works
1. In the Airflow UI, find the `pandas_mongo_example` DAG.
2. Un-pause it and trigger it manually (the play button).
3. Check task logs — `write_to_mongo` inserts a small Pandas DataFrame into MongoDB, and `read_from_mongo` reads it back out.

## Authentication

Airflow 3 replaced the FAB user table with pluggable *auth managers*. This stack
uses the default `SimpleAuthManager` with
`AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True`, which disables the login
screen and treats every visitor as an admin. That is fine for a local demo and
keeps the setup free of generated-password files — **do not use it for anything
network-reachable.**

To require a login instead, drop that variable and set:
```yaml
AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS: 'admin:admin'   # username:role
```
SimpleAuthManager then generates a random password into
`$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated` inside the
container; read it with
`docker compose exec airflow-apiserver cat simple_auth_manager_passwords.json.generated`.

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

**Why JSON and not a `mongodb://` URI?** `MongoHook._validate_connection` rejects
any `conn_type` other than `mongo`, and Airflow derives `conn_type` from a URI's
scheme — so the obvious `mongodb://mongo:mongo@mongodb:27017/` fails with
`conn_type 'mongodb' not allowed for MongoHook`. The JSON form states the
`conn_type` outright. The hook rebuilds a real `mongodb://` URI internally from
these fields, and everything in `extra` is passed to `MongoClient` as keyword
arguments (which is how `authSource` gets through — the URI query string is
discarded by `_create_uri`).

Note that Airflow's log masker redacts secret values everywhere, including in
exception text. Since the username and password here are both the literal string
`mongo`, error messages come out as `conn_type '***db' not allowed ...` — the
`***` is a redacted `mongo`, not a separate problem.

## Notes on the Airflow 3 service layout
- **`airflow-dag-processor` is not optional.** In Airflow 3 the scheduler no
  longer parses DAG files; without this service no DAG ever shows up in the UI.
- The webserver is now `airflow api-server`, not `airflow webserver`.
- `AIRFLOW__API_AUTH__JWT_SECRET` must be identical across every component. The
  scheduler signs a JWT per task instance and the API server verifies it, so a
  mismatch makes every task fail rather than producing an obvious auth error.

## Adding more Python packages
Add them to `requirements.txt`, then rebuild:
```bash
docker compose build
docker compose up -d
```
Leave them unpinned — the Dockerfile applies Airflow's official constraints file
for 3.3.0/Python 3.12, which is what keeps a new dependency from quietly
upgrading `apache-airflow` itself.

## Stopping / resetting
```bash
docker compose down          # stop containers, keep data
docker compose down -v       # stop containers AND wipe volumes (fresh start)
```
