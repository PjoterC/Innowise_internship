-- Stage file -> stage 1 table, 1:1. Airflow does the PUT (a client-side command
-- that cannot run inside a procedure) and then calls this; everything
-- server-side lives here, so the load is a database object that can be
-- reviewed, granted and called from anywhere.
--
-- The COPY is dynamic SQL for one reason: PATTERN and FORCE cannot be bind
-- variables in a COPY statement.
--
--   P_FORCE_RELOAD  FALSE — Snowflake skips files it has already loaded (it
--                           remembers them for 64 days), which is what makes
--                           re-running the DAG safe.
--                   TRUE  — reload regardless; for a deliberate replay only.

USE DATABASE AIRLINE_DWH;

CREATE OR REPLACE PROCEDURE RAW.SP_LOAD_RAW_FROM_STAGE(
    P_RUN_ID       STRING,
    P_FILE_PATTERN STRING,
    P_FORCE_RELOAD BOOLEAN
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    V_STARTED  TIMESTAMP_LTZ := CURRENT_TIMESTAMP();
    V_BATCH_ID STRING;
    V_SQL      STRING;
    V_QUERY_ID STRING;
    V_ROWS     NUMBER := 0;
    V_FILES    NUMBER := 0;
BEGIN
    -- Milliseconds are in there because one DAG run may call this twice (two
    -- source files, two tasks) and the two batches must not merge.
    V_BATCH_ID := P_RUN_ID || '::' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISSFF3');

    V_SQL := '
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
                   ''' || V_BATCH_ID || '''
            FROM @RAW.STG_AIRLINE_FILES t)
        PATTERN = ''' || P_FILE_PATTERN || '''
        FILE_FORMAT = (FORMAT_NAME = RAW.FF_AIRLINE_CSV)
        ON_ERROR = ABORT_STATEMENT
        FORCE = ' || IFF(COALESCE(P_FORCE_RELOAD, FALSE), 'TRUE', 'FALSE');

    EXECUTE IMMEDIATE :V_SQL;
    V_QUERY_ID := SQLID;

    -- Counted from the landing table by batch, not from RESULT_SCAN of the
    -- COPY: a COPY that loaded nothing returns a result with no "rows_loaded"
    -- column at all, so reading it would fail in exactly the case most worth
    -- logging — "the DAG ran and there was nothing new".
    SELECT COUNT(*), COUNT(DISTINCT SRC_FILE_NAME)
      INTO :V_ROWS, :V_FILES
      FROM RAW.AIRLINE_RAW WHERE BATCH_ID = :V_BATCH_ID;

    CALL META.SP_WRITE_AUDIT(:P_RUN_ID, 'dwh_ingest_raw', 'RAW.AIRLINE_RAW', 'COPY',
                             :V_ROWS, 0, :V_STARTED, 'SUCCESS', NULL, :V_QUERY_ID);

    RETURN 'batch_id=' || V_BATCH_ID || ' files=' || V_FILES || ' inserted=' || V_ROWS;

EXCEPTION
    WHEN OTHER THEN
        -- Log, then re-raise: the task must still turn red, but the audit table
        -- is where the failure outlives the Airflow log retention.
        CALL META.SP_WRITE_AUDIT(:P_RUN_ID, 'dwh_ingest_raw', 'RAW.AIRLINE_RAW', 'COPY',
                                 0, 0, :V_STARTED, 'FAILED', SQLERRM, NULL);
        RAISE;
END;
$$;
