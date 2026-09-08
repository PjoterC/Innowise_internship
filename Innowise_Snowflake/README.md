# Airline DWH on Snowflake (Task 1)

A small three-layer warehouse built from `data/Airline Dataset.csv` (98,619
bookings), loaded and orchestrated by Airflow. Runs on Snowflake **Enterprise
Edition**, which it uses for exactly two things: a native row access policy, and
a Time Travel window longer than a day.

## The structure of the data flow

```
data/Airline Dataset.csv
        │  PUT              (Airflow, client-side)
        ▼
@RAW.STG_AIRLINE_FILES     internal named stage
        │  COPY INTO        RAW.SP_LOAD_RAW_FROM_STAGE
        ▼
STAGE 1  RAW.AIRLINE_RAW           1:1 with the file, all VARCHAR, append-only
        │  3 streams ──> 3 MERGE procedures
        ▼
STAGE 2  CORE.DIM_PASSENGER  ─┐
         CORE.DIM_AIRPORT    ─┴──> CORE.FCT_FLIGHT_BOOKING
         CORE.DIM_DATE             static, seeded by the DDL
        │  1 stream ──> 1 MERGE procedure
        ▼
STAGE 3  MART.AGG_FLIGHT_STATUS_DAILY
         MART.V_FLIGHT_BOOKING_SECURE      secure view + row access policy

         META.ETL_AUDIT_LOG                every step logs insert/update counts
         META.RLS_CONTINENT_ACCESS         role -> continent grid
```

Eight tables. Two DAGs. Six stored procedures. Almost all the SQL lives in
Snowflake and Airflow only decides what runs when — the one deliberate exception
is `sql/airflow/load_raw_copy_into.sql`, the second of the two stage-1 load
paths described below.

## Two DAGs

| DAG | Does |
|---|---|
| `dwh_ingest_raw` | PUTs the CSV onto the internal stage, COPYs it into `RAW.AIRLINE_RAW` **one of two ways**, then triggers the pipeline below |
| `dwh_pipeline` | 2 dimensions in parallel → fact → aggregate → prints this run's audit rows |


### Two ways to load stage 1

`dwh_ingest_raw` has a `load_method` param and a branch. Exactly one of these
runs; the other is skipped.

| `load_method` | Task | How the COPY is issued |
|---|---|---|
| `procedure` (default) | `copy_into_raw_via_procedure` | `SnowflakeHook` calls `RAW.SP_LOAD_RAW_FROM_STAGE` |
| `operator` | `copy_into_raw_via_operator` | `SQLExecuteQueryOperator` runs `sql/airflow/load_raw_copy_into.sql` |

They land identical rows and both write their audit row through
`META.SP_WRITE_AUDIT`, so nothing downstream can tell which one ran. What
differs is where the SQL lives and how it behaves internally:

* the **procedure** is a database object. It validates its own arguments, wraps
  the COPY in an exception handler, and therefore audits a *failed* load as well
  as a successful one. Anything holding USAGE on it can call it — Airflow is
  merely one caller;
* the **.sql file** is an Airflow artifact, rendered by Jinja at task run time.
  Less machinery, and the exact statement appears in the task log — but a COPY
  that throws kills the task before the audit `CALL` at the end of the file is
  reached, so that failure exists only in the Airflow log.

Alternatives rather than a sequence, because Snowflake's load history is keyed
on (table, staged file): running both over one file would COPY the same rows
twice under two different batch ids.

Both DAGs are re-runnable, though not in the way the COPY's own load history
would suggest. `put_file_to_stage` PUTs with `OVERWRITE = TRUE`, so the staged
object is replaced on every run, and Snowflake identifies an already-loaded file
by name *and* ETag rather than by content — a re-upload is therefore a modified
file with a familiar name, which is precisely the case it reloads. Re-running
`dwh_ingest_raw` over an unchanged CSV appends a second full copy to
`RAW.AIRLINE_RAW`, with `FORCE` still `FALSE`.

That is a deliberate. `OVERWRITE = FALSE` would make PUT
skip the upload and the COPY skip the file even if it was
actually updated, only because the name was still the same.


Set `force_reload` to replay a file deliberately; it is the honest way to say
"load it again" and it shows up as such in the audit log. The same key design is
what makes a *failed* run restartable: if the fact load fails, retrying re-runs
all three CORE tasks and the two that already committed find nothing to do.

### Four streams

A stream is a cursor, and the first DML that reads it advances it. Three
consumers sharing one stream would mean two of them see an empty table.

`CORE.DIM_DATE` has no stream at all. Every column in it is a pure function of
the date, so it is generated once over a fixed range (2020–2034) by
`sql/ddl/03_core_layer.sql` instead of being loaded per run — which also lets a
report show days with **zero** bookings, something a calendar built only from
observed dates cannot express.
MART joins the calendar with an `INNER JOIN`, so a booking dated outside the
range would disappear silently. `sql/analysis/audit_report.sql` asserts that
none does.

## Running the app

```bash
# 1. credentials — see "Where credentials go" below
cp .env.example .env && $EDITOR .env
../.venv/bin/python check_connection.py

# 2. deploy the warehouse (DDL first, then procedures)
../.venv/bin/python scripts/run_sql.py --quiet sql/ddl/0*.sql sql/procedures/*.sql

# 3. start Airflow, then unpause and trigger `dwh_ingest_raw` at localhost:8080
docker compose up airflow-init
docker compose up -d
```

`scripts/run_sql.py` is the hand-driven entry point — for deploying DDL before
Airflow is up, and for the Time Travel and row-level-security scripts, which do
not belong on a schedule.

Redeploying after a DDL change needs a clean slate first: the DDL is written
with `CREATE ... IF NOT EXISTS`, which will not alter a table that already
exists. `sql/ddl/99_teardown.sql` drops everything.

## Repository structure

```
sql/ddl/         00 schemas · 01 META · 02 RAW · 03 CORE · 04 MART · 05 roles · 99 teardown
sql/procedures/  10 audit writer · 11 stage->RAW · 12-13, 15 RAW->CORE · 16 CORE->MART
sql/airflow/     load_raw_copy_into.sql — the operator-path COPY, templated by Airflow
sql/time_travel/ 01 two DDL statements · 02 two DML statements
sql/analysis/    rls_demo.sql · audit_report.sql
dags/            dwh_ingest_raw.py · dwh_pipeline.py · snowflake_connection_test.py
plugins/         dwh_common.py — the Snowflake hook the DAGs share
scripts/         run_sql.py — run any .sql file against the account in .env

task2/           both Snowflake and Postgres versions of queries
                 for task 2

streamlit/       DB_CLONE — Streamlit in Snowflake app: clone a 
database + grants (task 3)
```

## Audit logging

Every procedure wraps its DML the same way: run it, read the insert/update
counts out of `RESULT_SCAN` of its own query id, and call `META.SP_WRITE_AUDIT`
— on the failure path too, before re-raising so the Airflow task still goes red.

```sql
SELECT TARGET_OBJECT, OPERATION, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED, STATUS
FROM META.ETL_AUDIT_LOG ORDER BY AUDIT_ID DESC;
```

`ROWS_DELETED` is there for one step only. The mart aggregate has to *remove* a
cell whose last booking moved to another date or another status — recomputing
alone would leave the old count sitting there, since a cell with no fact rows
produces no group to overwrite it. Every other step logs 0.

The counts mean something because the MERGEs are hash-guarded: a row is only
rewritten when its content actually changed, so re-running over unchanged data
reports 0 updated rather than 98,619 no-op writes.

## Row-level security

Two mechanisms, two jobs.

`SECURE` on the view hides its definition and stops the optimiser pushing a
user-supplied predicate below the view's own filters.

The filtering itself is a **row access policy**, declared once in `META` and
attached to the view:

```sql
CREATE ROW ACCESS POLICY IF NOT EXISTS META.RAP_CONTINENT
    AS (continent_code VARCHAR) RETURNS BOOLEAN ->
        EXISTS (SELECT 1 FROM META.RLS_CONTINENT_ACCESS m
                 WHERE m.ROLE_NAME = CURRENT_ROLE() AND m.IS_ACTIVE
                   AND (m.CONTINENT_CODE = '*' OR m.CONTINENT_CODE = continent_code));

ALTER VIEW MART.V_FLIGHT_BOOKING_SECURE
    ADD ROW ACCESS POLICY META.RAP_CONTINENT ON (CONTINENT_CODE);
```

Who sees what is a row in `META.RLS_CONTINENT_ACCESS`, not a redeploy. The
policy body runs with the policy owner's rights, so the analyst roles hold no
grant on `META` at all — they cannot read the table that decides what they can
read. `CURRENT_ROLE()` resolves to the caller's role, which is what makes one
policy give three answers.

### The secondary-roles caveat

`CURRENT_ROLE()` returns the session's **primary** role, but Snowflake authorises
a statement against the primary role *union every active secondary role*, and
users default to `DEFAULT_SECONDARY_ROLES = ('ALL')`. So an account owner who
runs `USE ROLE DWH_ANALYST_EU` still carries ACCOUNTADMIN as a secondary: the
policy filters the view (it reads `CURRENT_ROLE()`), yet `SELECT` on the base
fact table still succeeds. The result looks like RLS working and grants failing,
when in fact neither is true — the session simply is not an analyst.

Testing this as an admin therefore requires `USE SECONDARY ROLES NONE;` after
each `USE ROLE`, which is what `sql/analysis/rls_demo.sql` does. A real analyst
user, granted only the analyst role, has nothing to union in and needs no such
guard. Check yours with `DESC USER <name>;`.

It is attached to the view rather than to `CORE.FCT_FLIGHT_BOOKING` on purpose:
a policy on the fact table would also apply to the pipeline's own reads — the
stream, and the aggregate procedure that drains it — so the loader's role would
need every continent granted just to do its job.

```bash
../.venv/bin/python scripts/run_sql.py sql/analysis/rls_demo.sql
```

...shows `DWH_ANALYST_EU` seeing only `EU`, `DWH_ANALYST_NAM` only `NAM`, and
`DWH_ADMIN` seeing everything — same query, three results — plus
`POLICY_REFERENCES` listing everywhere the policy is in force.

## Time Travel

`sql/time_travel/` has two DDL statements — a zero-copy `CLONE ... AT (OFFSET)`
and `DROP` + `UNDROP` — and two DML ones, each undoing an accident:

| | Accident | Repair |
|---|---|---|
| DML 1 | an `UPDATE` blanks a column | `UPDATE ... FROM <table> BEFORE (STATEMENT => ...)` joins the table to its own earlier version and puts the old value back |
| DML 2 | a `DELETE` removes rows | `INSERT ... SELECT ... BEFORE (STATEMENT => ...)` reads the rows back out of the snapshot |

Each is preceded by the statement that does the damage and followed by a count,
so `02_dml_time_travel.sql` runs top to bottom and every repair has something
real to repair.

The pairing is the point: a restore is a *join*, not a rewind. Nothing undoes a
statement — Time Travel simply exposes an earlier version of the table that
ordinary DML can read like any other source, so the shape of the repair follows
from the shape of the damage. Rows that are *wrong* still exist and can be
joined to; rows that are *gone* have to be selected out of the snapshot whole.

The wrong `UPDATE` is the more instructive of the two, because it is the one a
pipeline cannot notice. It leaves the row count unchanged and `RECORD_HASH`
still holding the hash of the old values, so the next run compares hashes, finds
them equal, and preserves the corruption. Nothing downstream ever reports it.

A third statement at the top of the file is context rather than a repair: it
pulls a `QUERY_ID` out of `META.ETL_AUDIT_LOG` and counts the table on both
sides of that load. That is what `QUERY_ID` is doing in the audit table — the id
of any logged load names the moment immediately before it ran, which turns "the
pipeline wrote something wrong at 03:00" into an addressable version of the
table. The two repairs use `LAST_QUERY_ID()` instead, because the statement each
one undoes was run seconds earlier in the same session.

Retention is set to **7 days** — enough to recover from a bad load discovered
the following Monday, without paying to keep three months of every intermediate
version of the landing table. Enterprise allows up to 90.

## What Enterprise is used for

| Feature | Used | Why |
|---|---|---|
| Row access policy | yes | the RLS rule on `MART.V_FLIGHT_BOOKING_SECURE` |
| Time Travel > 1 day | yes | 7-day retention |
| Materialized view | no | `MART.AGG_FLIGHT_STATUS_DAILY` is stream-driven, which the pipeline requires anyway and which a materialized view cannot express |
| Column masking policy | no | nothing here needs masking on top of row filtering |

## Notes on the source data

Profiled before modelling, and it changed the model:

* **`Pilot Name` equals the passenger's own name in 100% of rows.** Source junk.
  Dropped rather than made into a `DIM_PILOT` that would be a lie.
* **`Arrival Airport` is the IATA code of the airport in `Airport Name`** — the
  file has no separate destination. Neither column is unique alone (25 codes
  carry more than one name, 57 names more than one code), so the airport's
  natural key is code + name + country.
* **`Ticket Type` and `Passenger Status` are constant** across all 98,619 rows.
  Kept but useless for analysis.
* `Passenger ID` is unique per row, so the passenger dimension is 1:1 with the
  fact. Modelled as a dimension anyway — that is what it is, and a second file
  would break the coincidence.

# Connection setup

## Where credentials go

**One file: `.env`.** Both the local script and docker compose read it, so the
values are typed once. You can use the example template present in the repository.

### Variable description:

| Variable | Required | Notes |
|---|---|---|
| `SNOWFLAKE_ACCOUNT` | yes | The account **identifier** (`myorg-my_account`), not the URL |
| `SNOWFLAKE_USER` | yes | Login name |
| `SNOWFLAKE_PRIVATE_KEY_PATH` | one of the two | Path to a PKCS#8 key, e.g. `secrets/snowflake_key.p8` |
| `SNOWFLAKE_PASSWORD` | one of the two | Leave blank when using a key |
| `SNOWFLAKE_ROLE` / `_WAREHOUSE` / `_DATABASE` / `_SCHEMA` | no | Blank falls back to the user's defaults |

`.env` and `secrets/` are both gitignored, so nothing secret can be committed by
accident. `.env.example` is the only one that gets committed — keep it blank.



### Which authentication method?

| Method | `.env` fields | Local script | Airflow |
|---|---|---|---|
| Key pair | `SNOWFLAKE_PRIVATE_KEY_PATH` | yes | yes |
| **PAT** (access token) | `SNOWFLAKE_PAT`, authenticator stays `snowflake` | yes | yes |
| Password | `SNOWFLAKE_PASSWORD` | yes | yes, if the account still allows it |
| Static OAuth token | `SNOWFLAKE_OAUTH_TOKEN` + `SNOWFLAKE_AUTHENTICATOR=oauth` | yes | **no** — see below |
| OAuth refresh token | connection extras, not `.env` | no | yes |
| `externalbrowser` SSO | — | yes, interactively | no — nothing can click the browser prompt |

Whatever you pick, `SNOWFLAKE_AUTHENTICATOR` has to agree with it. Leaving it
**empty is not the same as leaving it default**: the connector reads an empty
authenticator as a request for an SSO handshake and stops sending your
credential, producing an error that mentions neither. `check_connection.py`
checks the two agree before it opens a socket.

*Recommended: Use the access token to connect, since it's been the one that was tested the most. Also, since Snowflake started the transition to forced MFA recently, other authentication methods like password auth can behave strangely sometimes. Remember to either create a network policy or bypass it in token settings.*





## Testing the connection locally

Remember to have all requirements from `requirements.txt` installed with pip.
No Docker involved. The repo-root `.venv` already has
`snowflake-connector-python` and `python-dotenv`:

```bash
source ../.venv/bin/activate
python check_connection.py
```

A working setup prints the session context:

```
Connected.

  Version    9.x.x
  Account    ABCD12345
  User       YOUR_USER
  Role       ACCOUNTADMIN
  Warehouse  COMPUTE_WH (or other default)
  Database   MYDB
  Schema     PUBLIC
```

On failure it prints Snowflake's error plus a hint about which `.env` field is
the likely cause. Get this green before moving on — it is a much shorter
feedback loop than a scheduler log.

## Testing the connection from Airflow

```bash
docker compose build      # installs the Snowflake provider into the image
docker compose up airflow-init   # one-time metadata database migration
docker compose up -d
```

Open http://localhost:8080 (no login), enable **`snowflake_connection_test`**,
and trigger it. Two green tasks means Airflow is talking to Snowflake:

- `report_session_context` — goes through `SnowflakeHook` directly
- `run_context_query` — goes through `SQLExecuteQueryOperator` and a templated
  `.sql` file, the path a real DAG uses

Both print the session context into the task log.



# SQL procedure loop (task 2)

Not much to discuss here, since the task focuses on one thing only.

The files that serve as the soloution are pretty clearly labeled and are present in the `task2/` directory.


# Streamlit DB clone (task 3)

Present in the `streamlit/` directory, a streamlit application that clones a database and grants appropriate permissions to specified roles.

The app is pretty intuitive and includes field descriptions (help parameters) to explain the flags and inputs, so the focus of this README section will be on the back-end.

File structure:

```
sp_clone_database.sql   F_IDENTIFIER + SP_CLONE_DATABASE — the plan and the grant matrix
streamlit_app.py        the form and the report; binds parameters, renders results
environment.yml         packages for the Snowflake-side runtime
deploy.sql              stage, PUT, CREATE STREAMLIT - deploys the streamlit application
```


## The two owner modes

The distinction is what the owner role is *for*, and it changes the grant set.

**Development — writable sandbox.** 

`USAGE, MONITOR, CREATE SCHEMA` on the
database, `ALL PRIVILEGES` on every schema and object in it, present and
future. `ALL PRIVILEGES` never includes `OWNERSHIP`, so this is purely
additive.

**Reasoning:**

On the database:

- `USAGE` - basic, needed to reach the database at all
- `MONITOR` - allows use of `DESCRIBE` and reading of metadata (can be useful for verification, debug etc.)
- `CREATE SCHEMA` - creating new schemas - necessary if we want to develop the database.

On schemas and objects:
- `ALL PRIVILEDGES` on every schema and object in it, present and future. On schemas, this is what allows `USAGE`, `MODIFY`, `MONITOR` and all `CREATE` priviledges - `CREATE TABLE`, `CREATE VIEW` etc. The core of what a database developer should be able to do. Creator owns the created objects.
On objects, `ALL PRIVILEDGES` differ per object type, but the developer should have access to all of them anyway.

**NOTE:** As mentioned before, the role does not grant OWNERSHIP of the cloned database by itself. Check **Transfer OWNERSHIP** note further below.


**Read-only — frozen snapshot.** 

`USAGE` down the container chain and `SELECT`
on tables, views, materialized views, external tables, dynamic tables, Iceberg
tables and streams, plus `USAGE` on functions and file formats so that a view
calling a UDF still works. The owner cannot change what it was handed.

**Reasoning:**

- `USAGE` down the container chain - database, all schemas and future schemas. Same as before, necessary to access anything.
- `SELECT` as described above - because the read-only role needs to be able to read the data obviously. Both present and future in case we add some read-only roles to a development database.

Since the role is read-only, it shouldn't have any way of modifying the database, that's why it doesn't have access to procedures or sequences, since they *can* modify the database.




### IMPORTANT NOTE:
**Transfer OWNERSHIP** is a separate flag, on by default for development and
off for read-only. By default, the owner of the database is the one that runs the streamlit application. The flag moves ownership of the database, its schemas and its
objects to the role selected in the *owner role* input field, so the role can drop and recreate things without the
app being involved again. It is off by default in read-only mode because
ownership overrides the read-only grant set entirely — a role can always write
to what it owns.



## Deploying

```bash
cd Innowise_Snowflake
../.venv/bin/python scripts/run_sql.py streamlit/deploy.sql
```

A Streamlit app in Snowflake runs with
its **owner's** rights, so for the app the caller is the role that ran
`deploy.sql` — whatever that role can do, the app can do for everyone allowed
to open it. Grant accordingly.

Run it from this repository rather than Snowsight worksheet.


## Some things worth knowing:

**Identifiers are whitelisted, not quoted.** - Every input in the form goes through the identifier check, which allows only specific symbols in the input to prevent SQL injection. One input value is escaped instead - the comment.


**Failures are per-statement.** - A grant on an object type the account does not
have (Iceberg tables, or materialized views on Standard Edition) fails
harmlessly and the run continues; the clone and the role creation are marked
critical and stop it regardless, because granting on a database that was never
created only produces a second, more confusing error. 


**Row access policy is not fully functional due to generalization.** - The policies are cloned and stay attached, but the generalized version of the clone allows to bypass it - for example read-only roles get SELECT on every table in every schema, so a policy guarding a view leaves the base tables under it fully readable. This can be checked when cloning database from task 1.