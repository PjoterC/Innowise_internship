-- Stage file -> stage 1 table, 1:1. Airflow does the PUT (a client-side command
-- that cannot run inside a procedure) and then calls this; everything
-- server-side lives here, so the load is a database object that can be
-- reviewed, granted and called from anywhere.
--
-- The COPY is dynamic SQL for one reason: PATTERN and FORCE cannot be bind
-- variables in a COPY statement. That makes two of the arguments part of the
-- statement *text* rather than data, so each is dealt with before it gets there:
--
--   P_FILE_PATTERN  REJECTED if it holds a quote or a backslash. Those two are
--                   the complete set of ways out of the string literal it is
--                   embedded in — a literal ends at the first unescaped quote,
--                   and a backslash is what escapes one; every other character
--                   is inert regex text that at worst matches no file. Neither
--                   has a legitimate use in a filename regex here, which is why
--                   the ingest DAG already spells "escaped dot" as [.]. A
--                   pattern carrying a quote is a bug or an attack, never a file
--                   name, so failing beats quietly rewriting it.
--
--   P_RUN_ID        ESCAPED, not rejected. It reaches the statement through
--                   BATCH_ID, where it is data rather than code — whatever
--                   Airflow put in a run id should be stored verbatim — so both
--                   characters are doubled and the parser collapses each pair
--                   back to one.
--
-- dwh_ingest_raw.py sanitises the pattern on its side too. This
-- is the guard for every *other* caller, since the procedure can be granted and
-- called from anywhere. Patterns the DAG generates pass it unchanged.
--
--   P_FORCE_RELOAD  FALSE — leave Snowflake's own load history to decide. Note
--                           that it identifies a loaded file by name and ETag,
--                           not by content, so a file re-staged by a PUT with
--                           OVERWRITE = TRUE is reloaded regardless: the caller
--                           gets a second copy in RAW under a new BATCH_ID.
--                   TRUE  — reload regardless; for a deliberate replay only.
--                   Boolean, so it reaches the text through IFF and cannot
--                   carry anything but the word TRUE or FALSE.

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
    V_STARTED   TIMESTAMP_LTZ := CURRENT_TIMESTAMP();
    V_BATCH_ID  STRING;
    V_BATCH_LIT STRING;
    V_SQL       STRING;
    V_QUERY_ID  STRING;
    V_ROWS      NUMBER := 0;
    V_FILES     NUMBER := 0;
    -- Both are raised before anything executes, and both travel through the
    -- handler at the bottom — so a rejected call is audited as FAILED with its
    -- reason rather than disappearing.
    E_MISSING_ARG    EXCEPTION (-20001,
        'P_RUN_ID and P_FILE_PATTERN are both required and P_FILE_PATTERN must be non-empty.');
    E_UNSAFE_PATTERN EXCEPTION (-20002,
        'P_FILE_PATTERN must not contain a quote or a backslash.');
BEGIN
    -- A NULL argument would propagate through the concatenation below and make
    -- the entire statement NULL, which EXECUTE IMMEDIATE then reports as a
    -- parse error naming nothing useful.
    IF (P_RUN_ID IS NULL OR P_FILE_PATTERN IS NULL OR TRIM(P_FILE_PATTERN) = '') THEN
        RAISE E_MISSING_ARG;
    END IF;

    -- The injection guard. See the header for why these two characters are the
    -- whole set, and why this rejects rather than escapes.
    IF (CONTAINS(P_FILE_PATTERN, '''') OR CONTAINS(P_FILE_PATTERN, '\\')) THEN
        RAISE E_UNSAFE_PATTERN;
    END IF;

    -- Milliseconds are in there because one DAG run may call this twice (two
    -- source files, two tasks) and the two batches must not merge.
    V_BATCH_ID := P_RUN_ID || '::' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISSFF3');

    -- Quote and backslash doubled for the one place the batch id is spliced
    -- into statement text. The parser collapses each pair, so the value that
    -- lands in BATCH_ID is V_BATCH_ID unchanged — which is what the COUNT below
    -- and the returned summary both go on matching.
    V_BATCH_LIT := REPLACE(REPLACE(V_BATCH_ID, '\\', '\\\\'), '''', '''''');

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
                   ''' || V_BATCH_LIT || '''
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
                             :V_ROWS, 0, 0, :V_STARTED, 'SUCCESS', NULL, :V_QUERY_ID);

    RETURN 'batch_id=' || V_BATCH_ID || ' files=' || V_FILES || ' inserted=' || V_ROWS;

EXCEPTION
    WHEN OTHER THEN
        -- Log, then re-raise: the task must still turn red, but the audit table
        -- is where the failure outlives the Airflow log retention. A rejected
        -- argument arrives here too, so the reason is on the row.
        CALL META.SP_WRITE_AUDIT(:P_RUN_ID, 'dwh_ingest_raw', 'RAW.AIRLINE_RAW', 'COPY',
                                 0, 0, 0, :V_STARTED, 'FAILED', SQLERRM, NULL);
        RAISE;
END;
$$;
