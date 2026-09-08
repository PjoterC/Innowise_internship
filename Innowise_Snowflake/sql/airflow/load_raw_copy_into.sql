-- The second way stage 1 gets loaded: a COPY INTO run straight from a .sql file
-- by SQLExecuteQueryOperator, with no stored procedure in between.
--
-- Same destination and same file format as RAW.SP_LOAD_RAW_FROM_STAGE, so the
-- two paths are interchangeable and dwh_ingest_raw picks one per run with the
-- `load_method` param. What differs is where the SQL lives and who templates
-- it: the procedure is a database object that validates its own arguments and
-- can be called by anything with USAGE on it, while this file is an Airflow
-- artifact rendered by Jinja and meaningful only to a task run.
--
-- Airflow splits this on semicolons (`split_statements=True`) and runs the
-- pieces down one connection, so the SET variables below survive from statement
-- to statement — which is what lets the audit CALL at the bottom report the
-- COPY's own query id, row count and elapsed time.
--
-- On templating:
--   * `run_id` is doubled for quotes, because it lands inside a SQL string
--     literal. The procedure escapes the same value for the same reason;
--   * the batch id is spelled out as a literal inside the COPY rather than read
--     from $v_batch_id, because a COPY transformation's SELECT list is a
--     restricted expression context and `$name` there collides with the `$1`
--     stage-column syntax. It is rendered twice, so it has to render the *same*
--     twice: hence the try number rather than a clock reading, which would tick
--     between the two substitutions and leave the COUNT below matching nothing;
--   * the pattern comes from put_file_to_stage, which builds it out of
--     [A-Za-z0-9_.-] only, so it cannot close the literal it sits in;


SET v_started  = CURRENT_TIMESTAMP();
SET v_batch_id = '{{ run_id | replace("'", "''") }}::try{{ ti.try_number }}';

COPY INTO RAW.AIRLINE_RAW (
    SOURCE_ROW_ID, PASSENGER_ID, FIRST_NAME, LAST_NAME, GENDER, AGE,
    NATIONALITY, AIRPORT_NAME, AIRPORT_COUNTRY_CODE, COUNTRY_NAME,
    AIRPORT_CONTINENT, CONTINENTS, DEPARTURE_DATE, ARRIVAL_AIRPORT,
    PILOT_NAME, FLIGHT_STATUS, TICKET_TYPE, PASSENGER_STATUS,
    SRC_FILE_NAME, SRC_FILE_ROW_NUMBER, BATCH_ID)
FROM (
    SELECT t.$1,  t.$2,  t.$3,  t.$4,  t.$5,  t.$6,
           t.$7,  t.$8,  t.$9,  t.$10, t.$11, t.$12,
           t.$13, t.$14, t.$15, t.$16, t.$17, t.$18,
           METADATA$FILENAME, METADATA$FILE_ROW_NUMBER,
           '{{ run_id | replace("'", "''") }}::try{{ ti.try_number }}'
    FROM @RAW.STG_AIRLINE_FILES t)
PATTERN = '{{ ti.xcom_pull(task_ids="put_file_to_stage") }}'
FILE_FORMAT = (FORMAT_NAME = RAW.FF_AIRLINE_CSV)
ON_ERROR = ABORT_STATEMENT
FORCE = FALSE;

-- Immediately after the COPY, before any other statement moves it on.
SET v_query_id = LAST_QUERY_ID();

-- Counted from the landing table by batch rather than from the COPY's own
-- result, for the same reason the procedure does it: a COPY that loaded nothing
-- returns a result with no "rows_loaded" column at all, so reading it would
-- fail in exactly the case most worth logging.
SET v_rows = (SELECT COUNT(*) FROM RAW.AIRLINE_RAW WHERE BATCH_ID = $v_batch_id);

-- The same audit writer the procedures use, so both load paths produce rows of
-- one shape in META.ETL_AUDIT_LOG and audit_report.sql needs to know nothing
-- about which one ran.
CALL META.SP_WRITE_AUDIT(
    '{{ run_id | replace("'", "''") }}', 'dwh_ingest_raw', 'RAW.AIRLINE_RAW', 'COPY',
    $v_rows, 0, 0, $v_started, 'SUCCESS', NULL, $v_query_id);
