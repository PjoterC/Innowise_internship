-- RAW — stage 1. A byte-for-byte copy of the source file: every column
-- VARCHAR, in file order, nothing cleaned or dropped. Anything a downstream
-- transform gets wrong can be replayed from here without touching the source.

USE DATABASE AIRLINE_DWH;

-- ESCAPE_UNENCLOSED_FIELD defaults to backslash, which silently eats one in any
-- unquoted field. UTF-8 is pinned because the data has names like "Grenoble-Isère".
CREATE FILE FORMAT IF NOT EXISTS RAW.FF_AIRLINE_CSV
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    EMPTY_FIELD_AS_NULL = TRUE
    ESCAPE_UNENCLOSED_FIELD = NONE
    ENCODING = 'UTF8';

-- Internal, not external: an external stage would need a bucket and a storage
-- integration this project does not have. Airflow PUTs here, a procedure COPYs
-- out. (An external stage is also what Snowpipe auto-ingest would require —
-- see the README for what would change.)
CREATE STAGE IF NOT EXISTS RAW.STG_AIRLINE_FILES
    FILE_FORMAT = RAW.FF_AIRLINE_CSV
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Airflow PUTs the source CSV here before COPY INTO.';

-- Column order matches the file exactly, including its unnamed leading index
-- column, because the COPY is positional ($1..$18). Renaming is fine; reordering
-- is not.
CREATE TABLE IF NOT EXISTS RAW.AIRLINE_RAW (
    SOURCE_ROW_ID        VARCHAR,   -- $1, the file's unnamed index column
    PASSENGER_ID         VARCHAR,
    FIRST_NAME           VARCHAR,
    LAST_NAME            VARCHAR,
    GENDER               VARCHAR,
    AGE                  VARCHAR,
    NATIONALITY          VARCHAR,
    AIRPORT_NAME         VARCHAR,
    AIRPORT_COUNTRY_CODE VARCHAR,
    COUNTRY_NAME         VARCHAR,
    AIRPORT_CONTINENT    VARCHAR,
    CONTINENTS           VARCHAR,
    DEPARTURE_DATE       VARCHAR,
    ARRIVAL_AIRPORT      VARCHAR,
    PILOT_NAME           VARCHAR,
    FLIGHT_STATUS        VARCHAR,
    TICKET_TYPE          VARCHAR,
    PASSENGER_STATUS     VARCHAR,   -- $18
    -- Load metadata, never present in the file.
    SRC_FILE_NAME        VARCHAR,
    SRC_FILE_ROW_NUMBER  NUMBER(38,0),
    LOAD_TS              TIMESTAMP_LTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    BATCH_ID             VARCHAR       NOT NULL
);

-- One stream per consumer, since stream is a cursor.
-- APPEND_ONLY because RAW is insert-only.
-- The streams are always created before any load.
CREATE STREAM IF NOT EXISTS RAW.STRM_AIRLINE_RAW_DIM_PASSENGER
    ON TABLE RAW.AIRLINE_RAW APPEND_ONLY = TRUE;
CREATE STREAM IF NOT EXISTS RAW.STRM_AIRLINE_RAW_DIM_AIRPORT
    ON TABLE RAW.AIRLINE_RAW APPEND_ONLY = TRUE;
CREATE STREAM IF NOT EXISTS RAW.STRM_AIRLINE_RAW_DIM_DATE
    ON TABLE RAW.AIRLINE_RAW APPEND_ONLY = TRUE;
CREATE STREAM IF NOT EXISTS RAW.STRM_AIRLINE_RAW_FCT_BOOKING
    ON TABLE RAW.AIRLINE_RAW APPEND_ONLY = TRUE;
